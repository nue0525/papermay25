"""
Prefect Task Orchestrator - Replaces the original task.py
This handles the overall workflow orchestration and task coordination
"""

import json
import asyncio
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional, Tuple
from collections import defaultdict, deque

from prefect import flow, task, get_run_logger
from prefect.task_runners import ConcurrentTaskRunner
from prefect.artifacts import create_markdown_artifact
from prefect.context import get_run_context

from Connection.pg import create_connection
from Utility.utils import getCurrentUTCtime
from APIs.Common.logger import getTaskLogger

# Import optimized tasks
from CoreEngine.modularize.postgres_optimized import (
    postgres_extract_optimized,
    postgres_load_optimized
)
from CoreEngine.Transformations.transformations_optimized import (
    expression_transform_optimized
)

logger = getTaskLogger


@flow(
    name="start-process-nodes",
    description="Main flow that replaces the original task.py processNodes functionality",
    task_runner=ConcurrentTaskRunner(max_workers=10),
    retries=1,
    retry_delay_seconds=60
)
async def start_process_nodes_flow(
    jobRunDict: Dict[str, Any],
    workflowRunId: str,
    workflow_id: str,
    workflow_name: str,
    tokenDict: Dict[str, Any],
    workflowLogPath: str,
    module: str,
    fromJob: bool
) -> Dict[str, Any]:
    """
    Main orchestration flow that replaces the original start_processNodes function
    Dynamically creates and executes tasks based on workflow configuration
    """
    run_logger = get_run_logger()
    run_context = get_run_context()
    
    run_logger.info(f"Starting workflow orchestration: {workflow_name}")
    
    try:
        # Extract workflow configuration
        workflows = jobRunDict.get("workflows", [])
        if not workflows:
            raise ValueError("No workflows found in jobRunDict")
        
        workflow_config = workflows[0]  # Assuming single workflow for now
        
        # Parse input parameters and dependencies
        inputParams = json.loads(jobRunDict.get("inputParams", "{}"))
        dependencies = workflow_config.get("dependencies", {})
        nodes = {node["nodeId"]: node for node in workflow_config.get("nodes", [])}
        
        run_logger.info(f"Processing {len(nodes)} nodes with dependencies: {dependencies}")
        
        # Record workflow start
        await _record_workflow_start_prefect(
            workflowRunId, workflow_id, str(run_context.flow_run.id), tokenDict
        )
        
        # Build execution graph
        execution_graph = _build_node_execution_graph(inputParams, dependencies, nodes)
        
        # Execute nodes using topological sort
        node_results = await _execute_nodes_with_dependencies(
            execution_graph,
            workflowRunId,
            workflow_id,
            workflow_name,
            jobRunDict,
            tokenDict,
            workflowLogPath,
            module,
            run_logger
        )
        
        # Update final workflow status
        await _record_workflow_completion_prefect(
            workflowRunId, workflow_id, "Completed", node_results
        )
        
        # Create execution summary
        await _create_workflow_execution_summary(
            workflow_name, workflowRunId, node_results
        )
        
        run_logger.info(f"Workflow {workflow_name} completed successfully")
        
        return {
            "status": "completed",
            "workflow_run_id": workflowRunId,
            "total_nodes": len(node_results),
            "successful_nodes": len([r for r in node_results if r.get("status") == "completed"]),
            "failed_nodes": len([r for r in node_results if r.get("status") == "failed"]),
            "total_rows_processed": sum(r.get("row_count", 0) for r in node_results),
            "execution_time_seconds": sum(r.get("execution_time", 0) for r in node_results),
            "node_results": node_results
        }
        
    except Exception as ex:
        run_logger.error(f"Workflow orchestration failed: {str(ex)}")
        
        # Record workflow failure
        await _record_workflow_completion_prefect(
            workflowRunId, workflow_id, "Failed", [], str(ex)
        )
        
        raise


def _build_node_execution_graph(
    inputParams: Dict[str, Any],
    dependencies: Dict[str, List[str]],
    nodes: Dict[str, Any]
) -> Dict[str, Dict[str, Any]]:
    """Build execution graph combining input params, dependencies, and node info"""
    
    execution_graph = {}
    
    for node_id, node_info in nodes.items():
        input_config = inputParams.get(node_id, {})
        
        execution_graph[node_id] = {
            "node_info": node_info,
            "input_config": input_config,
            "dependencies": dependencies.get(node_id, []),
            "dependents": [],
            "sequence": input_config.get("sequence", 0),
            "connector_type": input_config.get("connector_type", ""),
            "node_name": input_config.get("node_name", node_info.get("nodeName", f"Node_{node_id}"))
        }
    
    # Build reverse dependencies (dependents)
    for node_id, parent_ids in dependencies.items():
        for parent_id in parent_ids:
            if parent_id in execution_graph:
                execution_graph[parent_id]["dependents"].append(node_id)
    
    return execution_graph


async def _execute_nodes_with_dependencies(
    execution_graph: Dict[str, Dict[str, Any]],
    workflowRunId: str,
    workflow_id: str,
    workflow_name: str,
    jobRunDict: Dict[str, Any],
    tokenDict: Dict[str, Any],
    workflowLogPath: str,
    module: str,
    run_logger
) -> List[Dict[str, Any]]:
    """Execute nodes respecting dependencies using topological sort"""
    
    # Perform topological sort
    execution_order = _topological_sort_nodes(execution_graph)
    run_logger.info(f"Execution order: {[execution_graph[nid]['node_name'] for nid in execution_order]}")
    
    node_results = []
    node_storage_paths = {}  # Track storage paths for data flow
    
    for node_id in execution_order:
        node_config = execution_graph[node_id]
        connector_type = node_config["connector_type"]
        node_name = node_config["node_name"]
        input_config = node_config["input_config"]
        
        run_logger.info(f"Executing node: {node_name} (Type: {connector_type})")
        
        try:
            # Determine storage path from parent nodes
            storage_path = None
            parent_nodes = node_config["dependencies"]
            if parent_nodes:
                # For nodes with dependencies, get storage path from first parent
                parent_id = parent_nodes[0]
                storage_path = node_storage_paths.get(parent_id)
            
            # Execute node based on type
            if connector_type == "postgresql":
                if node_config["sequence"] == 1:  # Source node
                    result = await _execute_postgres_extract(
                        node_id, node_name, input_config, 
                        workflowRunId, workflow_id, jobRunDict, 
                        tokenDict, workflowLogPath
                    )
                else:  # Target node
                    result = await _execute_postgres_load(
                        node_id, node_name, input_config, storage_path,
                        workflowRunId, workflow_id, jobRunDict,
                        tokenDict, workflowLogPath
                    )
            
            elif connector_type == "expression":
                result = await _execute_expression_transform(
                    node_id, node_name, input_config, storage_path,
                    workflowRunId, workflow_id, jobRunDict,
                    tokenDict, workflowLogPath
                )
            
            else:
                raise ValueError(f"Unsupported connector type: {connector_type}")
            
            # Store result and update storage paths
            node_results.append(result)
            if result.get("storage_path"):
                node_storage_paths[node_id] = result["storage_path"]
            
            run_logger.info(f"Node {node_name} completed: {result.get('status', 'unknown')}")
            
        except Exception as ex:
            run_logger.error(f"Node {node_name} failed: {str(ex)}")
            
            # Record node failure
            failed_result = {
                "status": "failed",
                "node_id": node_id,
                "node_name": node_name,
                "error": str(ex),
                "execution_time": 0,
                "row_count": 0
            }
            node_results.append(failed_result)
            
            # Decide whether to continue or fail the entire workflow
            # For now, fail the entire workflow on any node failure
            raise
    
    return node_results


def _topological_sort_nodes(execution_graph: Dict[str, Dict[str, Any]]) -> List[str]:
    """Perform topological sort to determine execution order"""
    
    # Calculate in-degrees
    in_degree = defaultdict(int)
    for node_id in execution_graph:
        in_degree[node_id] = len(execution_graph[node_id]["dependencies"])
    
    # Initialize queue with nodes that have no dependencies
    queue = deque([node_id for node_id in execution_graph if in_degree[node_id] == 0])
    execution_order = []
    
    while queue:
        current_node = queue.popleft()
        execution_order.append(current_node)
        
        # Reduce in-degree for dependent nodes
        for dependent_node in execution_graph[current_node]["dependents"]:
            in_degree[dependent_node] -= 1
            if in_degree[dependent_node] == 0:
                queue.append(dependent_node)
    
    # Check for circular dependencies
    if len(execution_order) != len(execution_graph):
        raise ValueError("Circular dependency detected in workflow graph")
    
    return execution_order


@task(name="execute-postgres-extract", retries=3, retry_delay_seconds=30)
async def _execute_postgres_extract(
    node_id: str,
    node_name: str,
    config: Dict[str, Any],
    workflow_run_id: str,
    workflow_id: str,
    job_run_dict: Dict[str, Any],
    token_dict: Dict[str, Any],
    workflow_log_path: str
) -> Dict[str, Any]:
    """Execute PostgreSQL extraction with optimized performance"""
    
    return await postgres_extract_optimized(
        config.get("dbDetails", {}),
        node_id,
        node_name,
        workflowRunId=workflow_run_id,
        workflow_id=workflow_id,
        workflow_name=job_run_dict.get("workflows", [{}])[0].get("workflow_name", ""),
        jobRunDict=job_run_dict,
        tokenDict=token_dict,
        workflowLogPath=workflow_log_path,
        module="workflows"
    )


@task(name="execute-postgres-load", retries=3, retry_delay_seconds=30)
async def _execute_postgres_load(
    node_id: str,
    node_name: str,
    config: Dict[str, Any],
    storage_path: Optional[str],
    workflow_run_id: str,
    workflow_id: str,
    job_run_dict: Dict[str, Any],
    token_dict: Dict[str, Any],
    workflow_log_path: str
) -> Dict[str, Any]:
    """Execute PostgreSQL load with optimized performance"""
    
    return await postgres_load_optimized(
        config.get("dbDetails", {}),
        node_id,
        node_name,
        storage_path or "",
        workflowRunId=workflow_run_id,
        workflow_id=workflow_id,
        workflow_name=job_run_dict.get("workflows", [{}])[0].get("workflow_name", ""),
        jobRunDict=job_run_dict,
        tokenDict=token_dict,
        workflowLogPath=workflow_log_path,
        module="workflows",
        field_mappings=config.get("field_mapping", [])
    )


@task(name="execute-expression-transform", retries=3, retry_delay_seconds=30)
async def _execute_expression_transform(
    node_id: str,
    node_name: str,
    config: Dict[str, Any],
    storage_path: Optional[str],
    workflow_run_id: str,
    workflow_id: str,
    job_run_dict: Dict[str, Any],
    token_dict: Dict[str, Any],
    workflow_log_path: str
) -> Dict[str, Any]:
    """Execute expression transformation with optimized performance"""
    
    return await expression_transform_optimized(
        config,
        node_id,
        node_name,
        storage_path or "",
        workflowRunId=workflow_run_id,
        workflow_id=workflow_id,
        workflow_name=job_run_dict.get("workflows", [{}])[0].get("workflow_name", ""),
        jobRunDict=job_run_dict,
        tokenDict=token_dict,
        workflowLogPath=workflow_log_path,
        module="workflows"
    )


async def _record_workflow_start_prefect(
    workflow_run_id: str,
    workflow_id: str,
    prefect_flow_run_id: str,
    token_dict: Dict[str, Any]
):
    """Record workflow start with Prefect integration"""
    try:
        connection = create_connection()
        cursor = connection.cursor()
        
        # Insert or update workflow run stats
        query = """
            INSERT INTO workflow_run_stats 
            (workflow_run_id, workflow_id, run_status, start_timestamp, 
             run_by_id, run_by, prefect_flow_run_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (workflow_run_id) 
            DO UPDATE SET 
                run_status = 'Running',
                start_timestamp = EXCLUDED.start_timestamp,
                prefect_flow_run_id = EXCLUDED.prefect_flow_run_id
        """
        
        cursor.execute(query, (
            workflow_run_id,
            workflow_id,
            "Running",
            getCurrentUTCtime(),
            token_dict.get("UserId", ""),
            token_dict.get("fullName", ""),
            prefect_flow_run_id
        ))
        
        connection.commit()
        cursor.close()
        connection.close()
        
    except Exception as ex:
        logger.error(f"Error recording workflow start: {str(ex)}")


async def _record_workflow_completion_prefect(
    workflow_run_id: str,
    workflow_id: str,
    status: str,
    node_results: List[Dict[str, Any]],
    error_message: Optional[str] = None
):
    """Record workflow completion with metrics"""
    try:
        connection = create_connection()
        cursor = connection.cursor()
        
        # Update workflow run stats
        end_time = getCurrentUTCtime()
        
        query = """
            UPDATE workflow_run_stats 
            SET run_status = %s, 
                end_timestamp = %s,
                error_message = %s
            WHERE workflow_run_id = %s
        """
        
        cursor.execute(query, (
            status,
            end_time,
            error_message[:1000] if error_message else None,  # Truncate error message
            workflow_run_id
        ))
        
        # Insert execution metrics
        if node_results:
            metrics_data = []
            total_rows = sum(r.get("row_count", 0) for r in node_results)
            total_time = sum(r.get("execution_time", 0) for r in node_results)
            
            metrics_data.extend([
                (workflow_run_id, workflow_id, "total_rows_processed", total_rows, "rows", "performance", None, end_time),
                (workflow_run_id, workflow_id, "total_execution_time", total_time, "seconds", "performance", None, end_time),
                (workflow_run_id, workflow_id, "nodes_executed", len(node_results), "count", "performance", None, end_time)
            ])
            
            metrics_query = """
                INSERT INTO workflow_execution_metrics 
                (workflow_run_id, workflow_id, metric_name, metric_value, metric_unit, metric_type, node_id, recorded_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            cursor.executemany(metrics_query, metrics_data)
        
        connection.commit()
        cursor.close()
        connection.close()
        
    except Exception as ex:
        logger.error(f"Error recording workflow completion: {str(ex)}")


async def _create_workflow_execution_summary(
    workflow_name: str,
    workflow_run_id: str,
    node_results: List[Dict[str, Any]]
):
    """Create Prefect artifact with workflow execution summary"""
    try:
        successful_nodes = [r for r in node_results if r.get("status") == "completed"]
        failed_nodes = [r for r in node_results if r.get("status") == "failed"]
        
        total_rows = sum(r.get("row_count", 0) for r in successful_nodes)
        total_time = sum(r.get("execution_time", 0) for r in node_results)
        
        markdown_content = f"""
# Workflow Execution Summary: {workflow_name}

## Overview
- **Workflow Run ID**: `{workflow_run_id}`
- **Status**: {'✅ Success' if not failed_nodes else '❌ Failed'}
- **Total Nodes**: {len(node_results)}
- **Successful Nodes**: {len(successful_nodes)}
- **Failed Nodes**: {len(failed_nodes)}
- **Total Rows Processed**: {total_rows:,}
- **Total Execution Time**: {total_time:.2f} seconds
- **Average Rows/Second**: {total_rows/total_time if total_time > 0 else 0:,.0f}

## Node Execution Details
"""
        
        for result in node_results:
            status_icon = "✅" if result.get("status") == "completed" else "❌"
            node_name = result.get("node_name", result.get("node_id", "Unknown"))
            
            markdown_content += f"""
### {status_icon} {node_name}
- **Status**: {result.get("status", "Unknown")}
- **Rows Processed**: {result.get("row_count", 0):,}
- **Execution Time**: {result.get("execution_time", 0):.2f}s
- **Throughput**: {result.get("row_count", 0)/result.get("execution_time", 1):,.0f} rows/sec
"""
            
            if result.get("error"):
                markdown_content += f"- **Error**: {result['error']}\n"
        
        await create_markdown_artifact(
            key=f"workflow-summary-{workflow_run_id}",
            markdown=markdown_content,
            description=f"Execution summary for {workflow_name}"
        )
        
    except Exception as ex:
        logger.warning(f"Could not create execution summary: {str(ex)}")


# Entry point function that can be called from existing code
def start_long_running_task(
    jobRunDict: Dict[str, Any],
    workflowRunId: str,
    workflow_id: str,
    workflow_name: str,
    tokenDict: Dict[str, Any],
    redisQueue: Any,  # Not used in Prefect version
    module: str,
    fromJob: bool,
    workflowLogPath: Optional[str] = None
) -> Any:
    """
    Entry point that replaces the original start_long_running_task
    Now uses Prefect instead of Redis RQ
    """
    logger.info(f"Starting Prefect workflow: {workflow_name}")
    
    try:
        # Create workflow log path if not provided
        if not workflowLogPath:
            from CoreEngine.modularize.utils import createLogPath
            workflowLogPath = createLogPath(workflow_name)
        
        # Submit the flow for execution
        flow_run = start_process_nodes_flow.with_options(
            name=f"workflow-{workflowRunId}"
        ).submit(
            jobRunDict=jobRunDict,
            workflowRunId=workflowRunId,
            workflow_id=workflow_id,
            workflow_name=workflow_name,
            tokenDict=tokenDict,
            workflowLogPath=workflowLogPath,
            module=module,
            fromJob=fromJob
        )
        
        logger.info(f"Prefect flow submitted: {flow_run}")
        return flow_run
        
    except Exception as ex:
        logger.error(f"Error starting Prefect workflow: {str(ex)}")
        raise

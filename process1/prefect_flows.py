"""
Prefect Flows for Dynamic Workflow Orchestration
Replaces the Redis RQ-based task orchestration with Prefect flows
"""

import json
import asyncio
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional, Tuple, Set
from collections import defaultdict, deque

from prefect import flow, get_run_logger
from prefect.task_runners import ConcurrentTaskRunner
from prefect.artifacts import create_markdown_artifact
from prefect.context import get_run_context

from Connection.pg import create_connection
from psycopg2 import sql
from Utility.utils import getCurrentUTCtime

# Import our Prefect tasks
from .prefect_tasks import (
    store_workflow_metadata_task,
    postgres_extract_task,
    postgres_load_task,
    expression_transform_task,
    update_metrics_task
)


@flow(
    name="workflow-execution",
    description="Main dynamic flow for executing data workflows",
    task_runner=ConcurrentTaskRunner(),
    retries=1,
    retry_delay_seconds=60
)
async def workflow_execution_flow(
    workflow_run_id: str,
    workflow_id: str,
    workflow_name: str,
    job_run_id: str,
    workspace_info: Dict[str, str],
    connection_info_v1: Dict[str, Any],
    dependencies: Dict[str, List[str]],
    user_info: Dict[str, str],
    storage_config: Dict[str, Any],
    notification_config: Optional[Dict[str, str]] = None
) -> Dict[str, Any]:
    """
    Dynamic workflow execution flow that processes nodes based on their dependencies
    
    This replaces the Redis RQ-based orchestration with Prefect's native dependency management
    """
    run_logger = get_run_logger()
    run_context = get_run_context()
    
    run_logger.info(f"Starting workflow execution: {workflow_name}")
    
    try:
        # Record workflow start
        await _record_workflow_start(
            workflow_run_id, workflow_id, run_context.flow_run.id, user_info
        )
        
        # Store workflow metadata in PostgreSQL
        await store_workflow_metadata_task(
            workspace_info['workspace_id'],
            workspace_info['workspace_name'],
            workspace_info['folder_id'],
            workspace_info['folder_name'],
            workspace_info['job_id'],
            workspace_info['job_name'],
            workflow_id,
            workflow_name,
            connection_info_v1,
            dependencies,
            user_info['user_id']
        )
        
        # Build execution graph
        execution_graph = _build_execution_graph(connection_info_v1, dependencies)
        run_logger.info(f"Built execution graph with {len(execution_graph)} nodes")
        
        # Execute workflow using topological sort for dependency resolution
        node_results = await _execute_workflow_nodes(
            execution_graph,
            connection_info_v1,
            workflow_run_id,
            workflow_id,
            workflow_name,
            job_run_id,
            workspace_info,
            storage_config,
            run_logger
        )
        
        # Update metrics and data lineage
        await update_metrics_task(workflow_run_id, workflow_id, node_results)
        
        # Record workflow completion
        await _record_workflow_completion(workflow_run_id, workflow_id, "Completed")
        
        # Send notifications if configured
        if notification_config:
            await _send_workflow_notifications(
                workflow_name, "Completed", notification_config, node_results
            )
        
        # Create execution summary artifact
        await _create_execution_summary_artifact(
            workflow_name, workflow_run_id, node_results
        )
        
        run_logger.info(f"Workflow {workflow_name} completed successfully")
        
        return {
            "status": "completed",
            "workflow_run_id": workflow_run_id,
            "total_nodes": len(node_results),
            "total_rows_processed": sum(r.get('row_count', 0) for r in node_results),
            "execution_time": sum(r.get('execution_time', 0) for r in node_results),
            "node_results": node_results
        }
        
    except Exception as ex:
        run_logger.error(f"Workflow execution failed: {str(ex)}")
        
        # Record workflow failure
        await _record_workflow_completion(workflow_run_id, workflow_id, "Failed")
        
        # Send failure notifications
        if notification_config:
            await _send_workflow_notifications(
                workflow_name, "Failed", notification_config, [], str(ex)
            )
        
        raise


@flow(
    name="metadata-storage",
    description="Store workflow metadata on creation",
    task_runner=ConcurrentTaskRunner()
)
async def metadata_storage_flow(
    workspace_info: Dict[str, str],
    workflow_id: str,
    workflow_name: str,
    connection_info_v1: Dict[str, Any],
    dependencies: Dict[str, List[str]],
    created_by: str
) -> str:
    """
    Flow to store workflow metadata when a workflow is created or updated
    """
    run_logger = get_run_logger()
    run_logger.info(f"Storing metadata for workflow: {workflow_name}")
    
    result = await store_workflow_metadata_task(
        workspace_info['workspace_id'],
        workspace_info['workspace_name'],
        workspace_info['folder_id'],
        workspace_info['folder_name'],
        workspace_info['job_id'],
        workspace_info['job_name'],
        workflow_id,
        workflow_name,
        connection_info_v1,
        dependencies,
        created_by
    )
    
    run_logger.info(f"Metadata storage completed for {workflow_name}")
    return result


def _build_execution_graph(
    connection_info_v1: Dict[str, Any], 
    dependencies: Dict[str, List[str]]
) -> Dict[str, Dict[str, Any]]:
    """
    Build execution graph with node information and dependency relationships
    """
    execution_graph = {}
    
    for node_id, node_config in connection_info_v1.items():
        execution_graph[node_id] = {
            'config': node_config,
            'dependencies': dependencies.get(node_id, []),
            'dependents': [],
            'sequence': node_config.get('sequence', 0),
            'connector_type': node_config.get('connector_type'),
            'node_name': node_config.get('node_name', f'Node_{node_id}')
        }
    
    # Build dependent relationships
    for node_id, parent_ids in dependencies.items():
        for parent_id in parent_ids:
            if parent_id in execution_graph:
                execution_graph[parent_id]['dependents'].append(node_id)
    
    return execution_graph


async def _execute_workflow_nodes(
    execution_graph: Dict[str, Dict[str, Any]],
    connection_info_v1: Dict[str, Any],
    workflow_run_id: str,
    workflow_id: str,
    workflow_name: str,
    job_run_id: str,
    workspace_info: Dict[str, str],
    storage_config: Dict[str, Any],
    run_logger
) -> List[Dict[str, Any]]:
    """
    Execute workflow nodes using topological sorting to respect dependencies
    """
    # Perform topological sort to get execution order
    execution_order = _topological_sort(execution_graph)
    run_logger.info(f"Execution order: {[execution_graph[nid]['node_name'] for nid in execution_order]}")
    
    node_results = []
    node_storage_paths = {}  # Track storage paths for dependent nodes
    
    for node_id in execution_order:
        node_info = execution_graph[node_id]
        node_config = node_info['config']
        connector_type = node_info['connector_type']
        node_name = node_info['node_name']
        
        run_logger.info(f"Executing node: {node_name} ({connector_type})")
        
        try:
            # Determine storage path for dependent nodes
            storage_path = None
            if node_info['dependencies']:
                # Use storage path from parent node
                parent_id = node_info['dependencies'][0]  # Use first parent
                storage_path = node_storage_paths.get(parent_id)
            
            # Execute node based on connector type
            if connector_type == 'postgresql':
                if node_info['sequence'] == 1:  # Source node
                    result = await postgres_extract_task(
                        node_id=node_id,
                        node_name=node_name,
                        config=node_config.get('dbDetails', {}),
                        workflow_run_id=workflow_run_id,
                        workflow_id=workflow_id,
                        workflow_name=workflow_name,
                        job_run_id=job_run_id,
                        workspace_info=workspace_info,
                        storage_config=storage_config
                    )
                else:  # Target node
                    result = await postgres_load_task(
                        node_id=node_id,
                        node_name=node_name,
                        config=node_config.get('dbDetails', {}),
                        storage_path=storage_path or "",
                        workflow_run_id=workflow_run_id,
                        workflow_id=workflow_id,
                        workflow_name=workflow_name,
                        job_run_id=job_run_id,
                        workspace_info=workspace_info,
                        storage_config=storage_config,
                        field_mappings=node_config.get('field_mapping', [])
                    )
            
            elif connector_type == 'expression':
                result = await expression_transform_task(
                    node_id=node_id,
                    node_name=node_name,
                    config=node_config,
                    storage_path=storage_path or "",
                    transformation_config=node_config,
                    workflow_run_id=workflow_run_id,
                    workflow_id=workflow_id,
                    workflow_name=workflow_name,
                    job_run_id=job_run_id,
                    workspace_info=workspace_info,
                    storage_config=storage_config
                )
            
            else:
                raise ValueError(f"Unsupported connector type: {connector_type}")
            
            # Store result and update storage paths
            node_results.append(result)
            if result.get('storage_path'):
                node_storage_paths[node_id] = result['storage_path']
            
            run_logger.info(f"Node {node_name} completed successfully")
            
        except Exception as ex:
            run_logger.error(f"Node {node_name} failed: {str(ex)}")
            # Record node failure
            result = {
                "status": "failed",
                "node_id": node_id,
                "error": str(ex),
                "execution_time": 0,
                "row_count": 0
            }
            node_results.append(result)
            raise  # Re-raise to fail the entire workflow
    
    return node_results


def _topological_sort(execution_graph: Dict[str, Dict[str, Any]]) -> List[str]:
    """
    Perform topological sort to determine execution order based on dependencies
    """
    # Calculate in-degrees (number of dependencies)
    in_degree = defaultdict(int)
    for node_id in execution_graph:
        in_degree[node_id] = len(execution_graph[node_id]['dependencies'])
    
    # Find nodes with no dependencies (in-degree = 0)
    queue = deque([node_id for node_id in execution_graph if in_degree[node_id] == 0])
    execution_order = []
    
    while queue:
        current = queue.popleft()
        execution_order.append(current)
        
        # Reduce in-degree for dependent nodes
        for dependent in execution_graph[current]['dependents']:
            in_degree[dependent] -= 1
            if in_degree[dependent] == 0:
                queue.append(dependent)
    
    # Check for circular dependencies
    if len(execution_order) != len(execution_graph):
        raise ValueError("Circular dependency detected in workflow")
    
    return execution_order


async def _record_workflow_start(
    workflow_run_id: str, workflow_id: str, prefect_flow_run_id: str, user_info: Dict[str, str]
):
    """Record workflow start in database with Prefect integration"""
    try:
        connection = create_connection()
        cursor = connection.cursor()
        
        query = sql.SQL("""
            INSERT INTO workflow_run_stats 
            (workspace_id, workspace_name, folder_id, folder_name, job_id, job_run_id, 
             job_name, workflow_id, workflow_run_id, workflow_name, run_status, 
             run_by_id, run_by, start_timestamp, prefect_flow_run_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (workflow_run_id) 
            DO UPDATE SET 
                run_status = 'Running',
                start_timestamp = EXCLUDED.start_timestamp,
                prefect_flow_run_id = EXCLUDED.prefect_flow_run_id
        """)
        
        # Note: You'll need to pass more complete workspace info in practice
        cursor.execute(query, (
            user_info.get('workspace_id', ''),
            user_info.get('workspace_name', ''),
            user_info.get('folder_id', ''),
            user_info.get('folder_name', ''),
            user_info.get('job_id', ''),
            user_info.get('job_run_id', ''),
            user_info.get('job_name', ''),
            workflow_id,
            workflow_run_id,
            user_info.get('workflow_name', ''),
            'Running',
            user_info['user_id'],
            user_info.get('user_name', ''),
            getCurrentUTCtime(),
            str(prefect_flow_run_id)
        ))
        
        connection.commit()
        cursor.close()
        connection.close()
        
    except Exception as ex:
        print(f"Error recording workflow start: {str(ex)}")


async def _record_workflow_completion(
    workflow_run_id: str, workflow_id: str, status: str
):
    """Record workflow completion in database"""
    try:
        connection = create_connection()
        cursor = connection.cursor()
        
        query = sql.SQL("""
            UPDATE workflow_run_stats 
            SET run_status = %s, end_timestamp = %s
            WHERE workflow_run_id = %s
        """)
        
        cursor.execute(query, (status, getCurrentUTCtime(), workflow_run_id))
        
        connection.commit()
        cursor.close()
        connection.close()
        
    except Exception as ex:
        print(f"Error recording workflow completion: {str(ex)}")


async def _send_workflow_notifications(
    workflow_name: str,
    status: str,
    notification_config: Dict[str, str],
    node_results: List[Dict[str, Any]],
    error_message: Optional[str] = None
):
    """Send workflow completion/failure notifications"""
    try:
        # Import email service (assuming it exists)
        from Email.service import email_service
        
        if status == "Completed" and notification_config.get('on_success_notify'):
            await email_service.send_workflow_status_notification(
                workflow_name=workflow_name,
                workflow_status="Completed",
                email_to=[notification_config.get('notify_email')],
                workflow_details=f"Successfully processed {sum(r.get('row_count', 0) for r in node_results)} records"
            )
        elif status == "Failed" and notification_config.get('on_failure_notify'):
            await email_service.send_workflow_status_notification(
                workflow_name=workflow_name,
                workflow_status="Failed",
                email_to=[notification_config.get('notify_email')],
                workflow_details=error_message or "Workflow execution failed"
            )
    except Exception as ex:
        print(f"Error sending notifications: {str(ex)}")


async def _create_execution_summary_artifact(
    workflow_name: str,
    workflow_run_id: str,
    node_results: List[Dict[str, Any]]
):
    """Create Prefect artifact with execution summary"""
    try:
        successful_nodes = [r for r in node_results if r.get('status') == 'completed']
        failed_nodes = [r for r in node_results if r.get('status') == 'failed']
        
        total_rows = sum(r.get('row_count', 0) for r in successful_nodes)
        total_time = sum(r.get('execution_time', 0) for r in successful_nodes)
        
        markdown_content = f"""
# Workflow Execution Summary: {workflow_name}

## Overview
- **Workflow Run ID**: {workflow_run_id}
- **Total Nodes**: {len(node_results)}
- **Successful Nodes**: {len(successful_nodes)}
- **Failed Nodes**: {len(failed_nodes)}
- **Total Rows Processed**: {total_rows:,}
- **Total Execution Time**: {total_time:.2f} seconds

## Node Results
"""
        
        for result in node_results:
            status_icon = "✅" if result.get('status') == 'completed' else "❌"
            markdown_content += f"""
### {status_icon} Node: {result.get('node_id', 'Unknown')}
- **Status**: {result.get('status', 'Unknown')}
- **Rows Processed**: {result.get('row_count', 0):,}
- **Execution Time**: {result.get('execution_time', 0):.2f} seconds
"""
            if result.get('error'):
                markdown_content += f"- **Error**: {result['error']}\n"
        
        await create_markdown_artifact(
            key=f"execution-summary-{workflow_run_id}",
            markdown=markdown_content,
            description=f"Execution summary for workflow {workflow_name}"
        )
        
    except Exception as ex:
        print(f"Error creating execution summary artifact: {str(ex)}")


# Utility function to get workflow metadata from database
async def get_workflow_metadata(workflow_id: str) -> Optional[Dict[str, Any]]:
    """
    Retrieve workflow metadata from PostgreSQL tables
    This replaces querying MongoDB collections
    """
    try:
        connection = create_connection()
        cursor = connection.cursor()
        
        # Get workflow hierarchy
        hierarchy_query = sql.SQL("""
            SELECT workspace_id, workspace_name, folder_id, folder_name,
                   job_id, job_name, workflow_id, workflow_name
            FROM workflow_hierarchy 
            WHERE workflow_id = %s AND is_active = TRUE
        """)
        
        cursor.execute(hierarchy_query, (workflow_id,))
        hierarchy_result = cursor.fetchone()
        
        if not hierarchy_result:
            return None
        
        # Get workflow nodes
        nodes_query = sql.SQL("""
            SELECT node_id, node_name, connector_type, sequence_number,
                   connection_info, field_mappings, transformations
            FROM workflow_nodes 
            WHERE workflow_id = %s AND is_enabled = TRUE
            ORDER BY sequence_number
        """)
        
        cursor.execute(nodes_query, (workflow_id,))
        nodes_results = cursor.fetchall()
        
        # Get dependencies
        deps_query = sql.SQL("""
            SELECT child_node_id, parent_node_id
            FROM workflow_dependencies 
            WHERE workflow_id = %s AND is_active = TRUE
        """)
        
        cursor.execute(deps_query, (workflow_id,))
        deps_results = cursor.fetchall()
        
        cursor.close()
        connection.close()
        
        # Build response structure
        workspace_info = {
            'workspace_id': hierarchy_result[0],
            'workspace_name': hierarchy_result[1],
            'folder_id': hierarchy_result[2],
            'folder_name': hierarchy_result[3],
            'job_id': hierarchy_result[4],
            'job_name': hierarchy_result[5],
            'workflow_id': hierarchy_result[6],
            'workflow_name': hierarchy_result[7]
        }
        
        # Build connection_info_v1 structure
        connection_info_v1 = {}
        for node_result in nodes_results:
            node_id = node_result[0]
            connection_info_v1[node_id] = {
                'node_name': node_result[1],
                'connector_type': node_result[2],
                'sequence': node_result[3],
                'dbDetails': node_result[4] if node_result[4] else {},
                'field_mapping': node_result[5] if node_result[5] else [],
                'transformations': node_result[6] if node_result[6] else {}
            }
        
        # Build dependencies structure
        dependencies = defaultdict(list)
        for dep_result in deps_results:
            child_id, parent_id = dep_result
            dependencies[child_id].append(parent_id)
        
        return {
            'workspace_info': workspace_info,
            'connection_info_v1': connection_info_v1,
            'dependencies': dict(dependencies)
        }
        
    except Exception as ex:
        print(f"Error getting workflow metadata: {str(ex)}")
        return None


# Function to create Prefect deployment for a workflow
def create_workflow_deployment(
    workflow_id: str,
    workflow_name: str,
    schedule_config: Optional[Dict[str, Any]] = None
):
    """
    Create a Prefect deployment for the workflow
    This enables scheduled execution and better management
    """
    from prefect.deployments import Deployment
    from prefect.server.schemas.schedules import CronSchedule
    
    # Create deployment
    deployment = Deployment.build_from_flow(
        flow=workflow_execution_flow,
        name=f"workflow-{workflow_id}",
        tags=[
            f"workflow_id:{workflow_id}",
            f"workflow_name:{workflow_name}",
            "data_pipeline"
        ],
        description=f"Data pipeline workflow: {workflow_name}",
        version="1.0.0"
    )
    
    # Add schedule if provided
    if schedule_config:
        cron_expression = schedule_config.get('cron_schedule')
        if cron_expression:
            deployment.schedule = CronSchedule(cron=cron_expression)
    
    return deployment

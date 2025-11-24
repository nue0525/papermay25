"""Prefect Workflow Executor.

Core workflow execution engine that orchestrates node execution using Prefect.
Fetches workflow graphs from database and executes nodes in dependency order.

Architecture:
    - Prefect @flow for workflow orchestration
    - Prefect @task for individual node execution
    - Async execution with dependency resolution
    - Real-time status updates to database
    - Error handling and retry logic

Execution Flow:
    1. Load workflow graph (nodes + edges) from database
    2. Build dependency graph (topological sort)
    3. Execute nodes in parallel when dependencies met
    4. Update node_run_details after each node completes
    5. Update workflow_runs when all nodes complete
    6. Handle errors and retries per node configuration

Node Executors:
    - extract: Read data from source (DB, file, API)
    - transform: Apply Polars transformations
    - load: Write data to destination
    - llm_prompt: AI inference
    - router: Conditional branching
    - And all other node types

Usage:
    from backend.worker.engine.executor import execute_workflow
    
    # Execute workflow (async, returns when complete)
    await execute_workflow(
        workflow_run_id="run_123",
        workflow_id="wf_456",
        tenant_id="tenant_abc"
    )
"""
from prefect import flow, task
from prefect.task_runners import ConcurrentTaskRunner
from typing import List, Dict, Any, Optional
import asyncio
from datetime import datetime
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text
import polars as pl

from backend.core.config import get_settings
from backend.core.logging import get_logger
from backend.graph.types import NodeType, WorkflowNode
from backend.services.transformspec import apply_transformspec

settings = get_settings()
logger = get_logger(__name__)

# Create async engine for worker database access
engine = create_async_engine(settings.POSTGRES_DSN, echo=False, pool_pre_ping=True)
AsyncSessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
)


async def get_workflow_graph(workflow_id: str, tenant_id: str) -> Dict[str, Any]:
    """Fetch workflow graph from database.
    
    Args:
        workflow_id: Workflow to fetch
        tenant_id: Tenant ID for access control
    
    Returns:
        Dict with 'nodes' and 'edges' lists
    """
    async with AsyncSessionLocal() as session:
        # Fetch nodes
        nodes_stmt = text("""
            SELECT node_id, workflow_id, node_type, connector_id, connector_name,
                   node_config, sequence, tenant_id
            FROM maple.workflow_nodes
            WHERE workflow_id = :workflow_id AND tenant_id = :tenant_id
            ORDER BY sequence ASC
        """)
        nodes_result = await session.execute(nodes_stmt, {
            "workflow_id": workflow_id,
            "tenant_id": tenant_id
        })
        nodes = nodes_result.mappings().all()
        
        # Fetch edges
        edges_stmt = text("""
            SELECT workflow_edge_id, from_node_id, to_node_id, edge_type, "condition"
            FROM maple.workflow_edges
            WHERE workflow_id = :workflow_id AND tenant_id = :tenant_id
        """)
        edges_result = await session.execute(edges_stmt, {
            "workflow_id": workflow_id,
            "tenant_id": tenant_id
        })
        edges = edges_result.mappings().all()
        
        return {
            "nodes": [dict(node) for node in nodes],
            "edges": [dict(edge) for edge in edges]
        }


async def update_run_status(
    workflow_run_id: str,
    status: str,
    error_message: Optional[str] = None,
    rows_processed: Optional[int] = None
):
    """Update workflow run status in database.
    
    Args:
        workflow_run_id: Run to update
        status: New status (pending, running, success, failed)
        error_message: Error details if failed
        rows_processed: Total rows processed
    """
    async with AsyncSessionLocal() as session:
        update_stmt = text("""
            UPDATE maple.workflow_runs
            SET status = :status,
                error_message = :error_message,
                rows_processed = :rows_processed,
                completed_at = CASE WHEN :status IN ('success', 'failed', 'cancelled')
                                    THEN NOW() ELSE completed_at END,
                updated_at = NOW()
            WHERE workflow_run_id = :workflow_run_id
        """)
        await session.execute(update_stmt, {
            "workflow_run_id": workflow_run_id,
            "status": status,
            "error_message": error_message,
            "rows_processed": rows_processed
        })
        await session.commit()


async def update_node_run_status(
    node_run_detail_id: str,
    status: str,
    error_message: Optional[str] = None,
    rows_processed: Optional[int] = None,
    output_preview: Optional[Dict] = None
):
    """Update node run status in database.
    
    Args:
        node_run_detail_id: Node run to update
        status: New status (running, success, failed)
        error_message: Error details if failed
        rows_processed: Rows processed by this node
        output_preview: Sample output data
    """
    async with AsyncSessionLocal() as session:
        update_stmt = text("""
            UPDATE maple.node_run_details
            SET status = :status,
                error_message = :error_message,
                rows_processed = :rows_processed,
                output_preview = CAST(:output_preview AS JSONB),
                completed_at = CASE WHEN :status IN ('success', 'failed')
                                    THEN NOW() ELSE completed_at END,
                updated_at = NOW()
            WHERE node_run_detail_id = :node_run_detail_id
        """)
        await session.execute(update_stmt, {
            "node_run_detail_id": node_run_detail_id,
            "status": status,
            "error_message": error_message,
            "rows_processed": rows_processed,
            "output_preview": output_preview
        })
        await session.commit()


@task(name="execute_node", retries=2, retry_delay_seconds=10)
async def execute_node(
    node: Dict[str, Any],
    workflow_run_id: str,
    tenant_id: str,
    input_data: Optional[pl.LazyFrame] = None
) -> pl.LazyFrame:
    """Execute a single workflow node.
    
    Dispatches to appropriate executor based on node_type.
    
    Args:
        node: Node configuration from database
        workflow_run_id: Parent workflow run
        tenant_id: Tenant ID
        input_data: Input LazyFrame from upstream nodes
    
    Returns:
        Output LazyFrame to pass to downstream nodes
    
    Raises:
        Exception: If node execution fails
    """
    node_id = node["node_id"]
    node_type = node["node_type"]
    node_config = node.get("node_config", {})
    
    logger.info(f"Executing node {node_id} (type: {node_type})")
    
    # Create node run detail record
    node_run_detail_id = f"noderun_{node_id}_{workflow_run_id}"
    async with AsyncSessionLocal() as session:
        create_stmt = text("""
            INSERT INTO maple.node_run_details (
                node_run_detail_id, workflow_run_id, node_id, tenant_id,
                status, sequence, started_at, created_at
            ) VALUES (
                :node_run_detail_id, :workflow_run_id, :node_id, :tenant_id,
                'running', :sequence, NOW(), NOW()
            )
        """)
        await session.execute(create_stmt, {
            "node_run_detail_id": node_run_detail_id,
            "workflow_run_id": workflow_run_id,
            "node_id": node_id,
            "tenant_id": tenant_id,
            "sequence": node.get("sequence", 0)
        })
        await session.commit()
    
    try:
        # Dispatch to node type handler
        if node_type == NodeType.extract:
            from backend.worker.engine.nodes.extract import execute_extract
            output = await execute_extract(node_config, tenant_id)
        
        elif node_type == NodeType.transform:
            from backend.worker.engine.nodes.transform import execute_transform
            output = await execute_transform(node_config, input_data)
        
        elif node_type == NodeType.load:
            from backend.worker.engine.nodes.load import execute_load
            output = await execute_load(node_config, input_data, tenant_id)
        
        elif node_type == NodeType.llm_prompt:
            from backend.worker.engine.nodes.ai import execute_llm_prompt
            output = await execute_llm_prompt(node_config, input_data)
        
        else:
            raise NotImplementedError(f"Node type {node_type} not implemented")
        
        # Update node run as success
        rows_processed = output.select(pl.len()).collect().item() if output else 0
        await update_node_run_status(
            node_run_detail_id,
            status="success",
            rows_processed=rows_processed
        )
        
        logger.info(f"Node {node_id} completed successfully ({rows_processed} rows)")
        return output
    
    except Exception as e:
        logger.error(f"Node {node_id} failed: {e}", exc_info=True)
        await update_node_run_status(
            node_run_detail_id,
            status="failed",
            error_message=str(e)
        )
        raise


@flow(name="execute_workflow", task_runner=ConcurrentTaskRunner())
async def execute_workflow(
    workflow_run_id: str,
    workflow_id: str,
    tenant_id: str
):
    """Execute complete workflow with all nodes.
    
    Main Prefect flow that orchestrates node execution.
    
    Args:
        workflow_run_id: Run identifier
        workflow_id: Workflow to execute
        tenant_id: Tenant ID
    
    Raises:
        Exception: If workflow execution fails
    """
    logger.info(f"Starting workflow execution: {workflow_id} (run: {workflow_run_id})")
    
    try:
        # Update run status to running
        await update_run_status(workflow_run_id, "running")
        
        # Fetch workflow graph
        graph = await get_workflow_graph(workflow_id, tenant_id)
        nodes = graph["nodes"]
        edges = graph["edges"]
        
        if not nodes:
            raise ValueError(f"Workflow {workflow_id} has no nodes")
        
        logger.info(f"Loaded workflow graph: {len(nodes)} nodes, {len(edges)} edges")
        
        # TODO: Build dependency graph and execute in topological order
        # For now, execute sequentially by sequence number
        node_outputs = {}
        total_rows = 0
        
        for node in sorted(nodes, key=lambda n: n.get("sequence", 0)):
            node_id = node["node_id"]
            
            # Get input from upstream nodes (simplified - just use previous node output)
            input_data = list(node_outputs.values())[-1] if node_outputs else None
            
            # Execute node
            output = await execute_node(node, workflow_run_id, tenant_id, input_data)
            node_outputs[node_id] = output
            
            if output:
                rows = output.select(pl.len()).collect().item()
                total_rows += rows
        
        # Update run status to success
        await update_run_status(
            workflow_run_id,
            status="success",
            rows_processed=total_rows
        )
        
        logger.info(
            f"Workflow {workflow_id} completed successfully "
            f"({total_rows} total rows processed)"
        )
    
    except Exception as e:
        logger.error(f"Workflow {workflow_id} failed: {e}", exc_info=True)
        await update_run_status(
            workflow_run_id,
            status="failed",
            error_message=str(e)
        )
        raise


async def submit_workflow_execution(
    workflow_run_id: str,
    workflow_id: str,
    tenant_id: str
):
    """Submit workflow for async execution.
    
    Called by API endpoint to trigger workflow execution.
    
    Args:
        workflow_run_id: Run identifier
        workflow_id: Workflow to execute
        tenant_id: Tenant ID
    """
    # Submit to Prefect for async execution
    # In production, this would submit to Prefect Cloud/Server
    # For now, execute directly (blocks until complete)
    await execute_workflow(workflow_run_id, workflow_id, tenant_id)

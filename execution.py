"""Workflow Execution Endpoint.

Triggers workflow execution by creating a run record and submitting to Prefect.
Integrates the API layer with the worker engine for actual execution.

Execution Flow:
    1. Client calls POST /workflows/{id}/execute
    2. Validate workflow exists and user has permission
    3. Fetch workflow graph (nodes + edges) from database
    4. Create workflow_runs record with status='pending'
    5. Submit workflow to Prefect for async execution
    6. Return run_id immediately (don't wait for completion)
    7. Prefect worker executes nodes in topological order
    8. Worker updates workflow_runs and node_run_details as it progresses

Monitoring:
    - GET /workflow_runs/{run_id} - Check overall status
    - GET /workflow_runs/{run_id}/nodes - See individual node progress
    - WebSocket (future) - Real-time updates

Example:
    POST /workflows/wf_123/execute?tenant_id=tenant_abc
    {
        "trigger_type": "manual",
        "config": {"env": "prod", "batch_size": 5000}
    }
    
    Response:
    {
        "workflow_run_id": "run_456",
        "status": "pending",
        "message": "Workflow submitted for execution"
    }
"""
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from backend.db.session import get_db
from backend.schemas.runs import WorkflowRunCreate, WorkflowRunOut
from backend.core.logging import get_logger
from backend.core.auth import get_current_user, get_optional_user
from typing import Optional
from pydantic import BaseModel
import uuid

logger = get_logger(__name__)

router = APIRouter()


class ExecuteWorkflowRequest(BaseModel):
    """Request payload for workflow execution.
    
    Attributes:
        trigger_type (str): How execution was initiated (manual, scheduled, api)
        triggered_by (Optional[str]): User or system initiating execution
        config (Optional[dict]): Runtime configuration overrides
            - env: Environment (dev, staging, prod)
            - batch_size: Records per batch
            - timeout_seconds: Max execution time
            - Any workflow-specific parameters
    
    Example:
        {
            "trigger_type": "manual",
            "triggered_by": "user_123",
            "config": {
                "env": "prod",
                "batch_size": 5000,
                "timeout_seconds": 3600
            }
        }
    """
    trigger_type: str = "manual"
    triggered_by: Optional[str] = None
    config: Optional[dict] = None


class ExecuteWorkflowResponse(BaseModel):
    """Response for workflow execution request.
    
    Attributes:
        workflow_run_id (str): Unique run identifier for tracking
        workflow_id (str): Workflow being executed
        status (str): Initial status (usually 'pending')
        message (str): Human-readable status message
    """
    workflow_run_id: str
    workflow_id: str
    status: str
    message: str


@router.post("/{workflow_id}/execute", response_model=ExecuteWorkflowResponse, status_code=202)
async def execute_workflow(
    workflow_id: str,
    payload: ExecuteWorkflowRequest,
    user: dict = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    # Extract tenant_id from authenticated user
    tenant_id = user["tenant_id"]
    """Trigger workflow execution.
    
    Creates a workflow run record and submits to Prefect for async execution.
    Returns immediately with run_id for tracking (non-blocking).
    
    Args:
        workflow_id: Workflow to execute
        tenant_id: Tenant ID for access control
        payload: Execution parameters
        db: Database session
    
    Returns:
        ExecuteWorkflowResponse with run_id and initial status
    
    Raises:
        HTTPException: 404 if workflow not found
        HTTPException: 500 if execution submission fails
    
    HTTP Status:
        202 Accepted - Execution request accepted, processing async
    
    Example:
        POST /api/v1/workflows/wf_123/execute?tenant_id=tenant_abc
        {
            "trigger_type": "manual",
            "triggered_by": "user_456",
            "config": {"batch_size": 1000}
        }
        
        Response:
        {
            "workflow_run_id": "run_789",
            "workflow_id": "wf_123",
            "status": "pending",
            "message": "Workflow submitted for execution"
        }
    
    Notes:
        - Execution is asynchronous (returns immediately)
        - Use GET /workflow_runs/{run_id} to monitor progress
        - Workflow must have at least one node to execute
        - Invalid graph structure will be caught during execution
    """
    # 1. Verify workflow exists
    workflow_stmt = text("""
        SELECT workflow_id, tenant_id, workspace_id, folder_id, job_id, workflow_name
        FROM maple.workflows
        WHERE workflow_id = :workflow_id AND tenant_id = :tenant_id AND deleted = false
    """)
    workflow_result = await db.execute(workflow_stmt, {
        "workflow_id": workflow_id,
        "tenant_id": tenant_id
    })
    workflow = workflow_result.mappings().first()
    
    if not workflow:
        raise HTTPException(
            status_code=404,
            detail=f"Workflow {workflow_id} not found"
        )
    
    logger.info(
        f"Executing workflow {workflow_id} ({workflow['workflow_name']}) "
        f"for tenant {tenant_id}"
    )
    
    # 2. Create workflow run record
    workflow_run_id = f"run_{uuid.uuid4().hex[:16]}"
    
    run_create = WorkflowRunCreate(
        workflow_run_id=workflow_run_id,
        tenant_id=tenant_id,
        workspace_id=workflow.get("workspace_id"),
        folder_id=workflow.get("folder_id"),
        job_id=workflow.get("job_id"),
        workflow_id=workflow_id,
        trigger_type=payload.trigger_type,
        triggered_by=payload.triggered_by,
        config=payload.config
    )
    
    run_stmt = text("""
        INSERT INTO maple.workflow_runs (
            workflow_run_id, tenant_id, workspace_id, folder_id, job_id, workflow_id,
            status, trigger_type, triggered_by, config, started_at, created_at
        ) VALUES (
            :workflow_run_id, :tenant_id, :workspace_id, :folder_id, :job_id, :workflow_id,
            'pending', :trigger_type, :triggered_by, CAST(:config AS JSONB), NOW(), NOW()
        )
        RETURNING workflow_run_id, status
    """)
    
    run_result = await db.execute(run_stmt, run_create.model_dump())
    run_row = run_result.mappings().first()
    await db.commit()
    
    logger.info(f"Created workflow run {workflow_run_id} with status 'pending'")
    
    # 3. Submit to Prefect for async execution
    from backend.worker.engine.executor import submit_workflow_execution
    import asyncio
    
    # Submit to Prefect asynchronously (fire and forget)
    try:
        # Create background task to execute workflow
        asyncio.create_task(
            submit_workflow_execution(workflow_run_id, workflow_id, tenant_id)
        )
        logger.info(
            f"Workflow {workflow_id} submitted for execution. "
            f"Run ID: {workflow_run_id}"
        )
    except Exception as e:
        logger.error(f"Failed to submit workflow {workflow_id} to Prefect: {e}")
        # Update run status to failed
        await db.execute(text("""
            UPDATE maple.workflow_runs
            SET status = 'failed', error_message = :error, updated_at = NOW()
            WHERE workflow_run_id = :run_id
        """), {"run_id": workflow_run_id, "error": str(e)})
        await db.commit()
        raise HTTPException(status_code=500, detail=f"Failed to submit workflow: {e}")
    
    return ExecuteWorkflowResponse(
        workflow_run_id=workflow_run_id,
        workflow_id=workflow_id,
        status="pending",
        message="Workflow submitted for execution. Use GET /workflow_runs/{run_id} to monitor progress."
    )


@router.get("/{workflow_id}/runs", response_model=list[WorkflowRunOut])
async def list_workflow_runs_for_workflow(
    workflow_id: str,
    tenant_id: str,
    limit: int = 50,
    offset: int = 0,
    db: AsyncSession = Depends(get_db)
):
    """List all runs for a specific workflow.
    
    Shows execution history for the workflow.
    
    Args:
        workflow_id: Workflow to get runs for
        tenant_id: Tenant ID for access control
        limit: Maximum results (default: 50)
        offset: Pagination offset
        db: Database session
    
    Returns:
        List of workflow runs ordered by start time (newest first)
    
    Example:
        GET /api/v1/workflows/wf_123/runs?tenant_id=tenant_abc&limit=10
    """
    stmt = text("""
        SELECT workflow_run_id, tenant_id, workflow_id, status, trigger_type, triggered_by,
               started_at, completed_at, duration_ms, rows_processed, error_message, config
        FROM maple.workflow_runs
        WHERE workflow_id = :workflow_id AND tenant_id = :tenant_id
        ORDER BY started_at DESC
        LIMIT :limit OFFSET :offset
    """)
    
    result = await db.execute(stmt, {
        "workflow_id": workflow_id,
        "tenant_id": tenant_id,
        "limit": limit,
        "offset": offset
    })
    
    rows = result.mappings().all()
    return [WorkflowRunOut(**row) for row in rows]

"""Workflow Run Tracking API Endpoints.

Provides endpoints for monitoring workflow execution runs specifically.
This is distinct from job runs and jobpackage runs - focuses only on workflow-level execution.
Tracks run status, metrics, and errors for debugging and observability.

Database Schema:
    Table: maple.workflow_runs
    Key Fields:
        - workflow_run_id (UUID, PK)
        - workflow_id (UUID, FK)
        - tenant_id (UUID)
        - status (VARCHAR): pending, running, success, failed, cancelled
        - started_at, completed_at (TIMESTAMP)
        - rows_processed (INTEGER)
        - duration_ms (INTEGER)
        - error_message (TEXT)
        - config (JSONB): Runtime configuration
    
    Table: maple.node_run_details
    Key Fields:
        - node_run_detail_id (UUID, PK)
        - workflow_run_id (UUID, FK)
        - node_id (UUID, FK)
        - status (VARCHAR): pending, running, success, failed
        - started_at, completed_at (TIMESTAMP)
        - rows_processed (INTEGER)
        - duration_ms (INTEGER)
        - error_message (TEXT)
        - output_preview (JSONB): Sample output data

Use Cases:
    - Monitor running workflows in real-time
    - Debug failed workflow executions
    - Track performance metrics over time
    - Audit trail for compliance
    - Cost/resource tracking

Endpoints:
    POST   /workflow_runs                           - Create workflow run record
    GET    /workflow_runs/{run_id}                  - Get run details
    GET    /workflow_runs                           - List runs (with filters)
    PATCH  /workflow_runs/{run_id}                  - Update run status/metrics
    GET    /workflow_runs/{run_id}/nodes            - Get node executions for run
    POST   /workflow_runs/{run_id}/nodes            - Create node run record
    PATCH  /workflow_runs/{run_id}/nodes/{node_id} - Update node status/metrics
    
Note:
    This module is specifically for workflow-level execution tracking.
    For job-level runs, see backend.api.v1.job_runs (to be implemented)
    For jobpackage-level runs, see backend.api.v1.jobpackage_runs (to be implemented)
"""
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from backend.db.session import get_db
from backend.core.auth import get_current_user, get_optional_user
from backend.schemas.runs import (
    WorkflowRunCreate, WorkflowRunUpdate, WorkflowRunOut,
    NodeRunDetailCreate, NodeRunDetailUpdate, NodeRunDetailOut
)

router = APIRouter()


@router.post("/", response_model=WorkflowRunOut, status_code=201)
async def create_workflow_run(
    payload: WorkflowRunCreate,
    user: dict = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    # Verify tenant_id matches authenticated user
    if payload.tenant_id != user["tenant_id"]:
        raise HTTPException(status_code=403, detail="Tenant ID mismatch")
    """Create a new workflow run record when execution starts.
    
    Args:
        payload: Workflow run data
        db: Database session
    
    Returns:
        Created workflow run record
    
    Example:
        POST /api/v1/workflow_runs
        {
            "workflow_run_id": "run_123",
            "tenant_id": "tenant_abc",
            "workflow_id": "wf_456",
            "trigger_type": "manual",
            "triggered_by": "user_789"
        }
    """
    stmt = text("""
        INSERT INTO maple.workflow_runs (
            workflow_run_id, tenant_id, workspace_id, folder_id, job_id, workflow_id,
            status, trigger_type, triggered_by, config, started_at, created_at
        ) VALUES (
            :workflow_run_id, :tenant_id, :workspace_id, :folder_id, :job_id, :workflow_id,
            'pending', :trigger_type, :triggered_by, CAST(:config AS JSONB), NOW(), NOW()
        )
        RETURNING workflow_run_id, tenant_id, workflow_id, status, trigger_type, triggered_by,
                  started_at, completed_at, duration_ms, rows_processed, error_message, config
    """)
    result = await db.execute(stmt, payload.model_dump())
    row = result.mappings().first()
    await db.commit()
    return WorkflowRunOut(**row)


@router.get("/{workflow_run_id}", response_model=WorkflowRunOut)
async def get_workflow_run(
    workflow_run_id: str,
    tenant_id: str,
    user: dict = Depends(get_optional_user),
    db: AsyncSession = Depends(get_db)
):
    """Retrieve workflow run details by ID.
    
    Args:
        workflow_run_id: Run identifier
        tenant_id: Tenant ID for access control
        db: Database session
    
    Returns:
        Workflow run details with current status and metrics
    
    Raises:
        HTTPException: 404 if run not found
    """
    stmt = text("""
        SELECT workflow_run_id, tenant_id, workflow_id, status, trigger_type, triggered_by,
               started_at, completed_at, duration_ms, rows_processed, error_message, config
        FROM maple.workflow_runs
        WHERE workflow_run_id = :workflow_run_id AND tenant_id = :tenant_id
    """)
    result = await db.execute(stmt, {
        "workflow_run_id": workflow_run_id,
        "tenant_id": tenant_id
    })
    row = result.mappings().first()
    if not row:
        raise HTTPException(status_code=404, detail="Workflow run not found")
    return WorkflowRunOut(**row)


@router.get("/", response_model=list[WorkflowRunOut])
async def list_workflow_runs(
    tenant_id: str,
    workflow_id: Optional[str] = None,
    status: Optional[str] = None,
    limit: int = 50,
    offset: int = 0,
    db: AsyncSession = Depends(get_db)
):
    """List workflow runs with optional filters.
    
    Args:
        tenant_id: Tenant ID filter
        workflow_id: Optional workflow filter
        status: Optional status filter (pending, running, success, failed)
        limit: Maximum results (default: 50)
        offset: Pagination offset (default: 0)
        db: Database session
    
    Returns:
        List of workflow runs ordered by start time (newest first)
    
    Example:
        GET /api/v1/workflow_runs?tenant_id=tenant_abc&workflow_id=wf_123&status=running
    """
    # Build dynamic query based on filters
    where_clauses = ["tenant_id = :tenant_id"]
    params = {"tenant_id": tenant_id, "limit": limit, "offset": offset}
    
    if workflow_id:
        where_clauses.append("workflow_id = :workflow_id")
        params["workflow_id"] = workflow_id
    
    if status:
        where_clauses.append("status = :status")
        params["status"] = status
    
    where_sql = " AND ".join(where_clauses)
    
    stmt = text(f"""
        SELECT workflow_run_id, tenant_id, workflow_id, status, trigger_type, triggered_by,
               started_at, completed_at, duration_ms, rows_processed, error_message, config
        FROM maple.workflow_runs
        WHERE {where_sql}
        ORDER BY started_at DESC
        LIMIT :limit OFFSET :offset
    """)
    
    result = await db.execute(stmt, params)
    rows = result.mappings().all()
    return [WorkflowRunOut(**row) for row in rows]


@router.patch("/{workflow_run_id}", response_model=WorkflowRunOut)
async def update_workflow_run(
    workflow_run_id: str,
    tenant_id: str,
    payload: WorkflowRunUpdate,
    db: AsyncSession = Depends(get_db)
):
    """Update workflow run status and metrics.
    
    Called during and after workflow execution to update progress.
    
    Args:
        workflow_run_id: Run identifier
        tenant_id: Tenant ID for access control
        payload: Fields to update
        db: Database session
    
    Returns:
        Updated workflow run record
    
    Example:
        PATCH /api/v1/runs/run_123?tenant_id=tenant_abc
        {
            "status": "success",
            "rows_processed": 10000,
            "duration_ms": 45230
        }
    """
    fields = payload.model_dump(exclude_unset=True)
    if not fields:
        return await get_workflow_run(workflow_run_id, tenant_id, db)
    
    # Add completed_at if status is terminal
    if "status" in fields and fields["status"] in ("success", "failed", "cancelled"):
        fields["completed_at"] = "NOW()"
    
    set_parts = []
    params = {"workflow_run_id": workflow_run_id, "tenant_id": tenant_id}
    
    for key, value in fields.items():
        if key == "completed_at":
            set_parts.append(f"{key} = NOW()")
        else:
            set_parts.append(f"{key} = :{key}")
            params[key] = value
    
    set_clause = ", ".join(set_parts) + ", updated_at = NOW()"
    
    stmt = text(f"""
        UPDATE maple.workflow_runs SET {set_clause}
        WHERE workflow_run_id = :workflow_run_id AND tenant_id = :tenant_id
        RETURNING workflow_run_id, tenant_id, workflow_id, status, trigger_type, triggered_by,
                  started_at, completed_at, duration_ms, rows_processed, error_message, config
    """)
    
    result = await db.execute(stmt, params)
    row = result.mappings().first()
    if not row:
        raise HTTPException(status_code=404, detail="Workflow run not found")
    await db.commit()
    return WorkflowRunOut(**row)


@router.get("/{workflow_run_id}/nodes", response_model=list[NodeRunDetailOut])
async def list_node_runs(
    workflow_run_id: str,
    tenant_id: str,
    db: AsyncSession = Depends(get_db)
):
    """Get all node executions for a workflow run.
    
    Shows execution details for each node in the workflow.
    
    Args:
        workflow_run_id: Parent workflow run
        tenant_id: Tenant ID for access control
        db: Database session
    
    Returns:
        List of node executions ordered by sequence
    """
    stmt = text("""
        SELECT node_run_detail_id, workflow_run_id, node_id, tenant_id, status, sequence,
               started_at, completed_at, duration_ms, rows_processed, error_message, output_preview
        FROM maple.node_run_details
        WHERE workflow_run_id = :workflow_run_id AND tenant_id = :tenant_id
        ORDER BY sequence ASC
    """)
    result = await db.execute(stmt, {
        "workflow_run_id": workflow_run_id,
        "tenant_id": tenant_id
    })
    rows = result.mappings().all()
    return [NodeRunDetailOut(**row) for row in rows]


@router.post("/{workflow_run_id}/nodes", response_model=NodeRunDetailOut, status_code=201)
async def create_node_run(
    workflow_run_id: str,
    payload: NodeRunDetailCreate,
    db: AsyncSession = Depends(get_db)
):
    """Create node run record when node execution starts.
    
    Args:
        workflow_run_id: Parent workflow run
        payload: Node run data
        db: Database session
    
    Returns:
        Created node run record
    """
    stmt = text("""
        INSERT INTO maple.node_run_details (
            node_run_detail_id, workflow_run_id, node_id, tenant_id,
            status, sequence, started_at, created_at
        ) VALUES (
            :node_run_detail_id, :workflow_run_id, :node_id, :tenant_id,
            'pending', :sequence, NOW(), NOW()
        )
        RETURNING node_run_detail_id, workflow_run_id, node_id, tenant_id, status, sequence,
                  started_at, completed_at, duration_ms, rows_processed, error_message, output_preview
    """)
    result = await db.execute(stmt, payload.model_dump())
    row = result.mappings().first()
    await db.commit()
    return NodeRunDetailOut(**row)


@router.patch("/{workflow_run_id}/nodes/{node_run_detail_id}", response_model=NodeRunDetailOut)
async def update_node_run(
    workflow_run_id: str,
    node_run_detail_id: str,
    tenant_id: str,
    payload: NodeRunDetailUpdate,
    db: AsyncSession = Depends(get_db)
):
    """Update node run status and metrics.
    
    Called during and after node execution.
    
    Args:
        workflow_run_id: Parent workflow run
        node_run_detail_id: Node run identifier
        tenant_id: Tenant ID for access control
        payload: Fields to update
        db: Database session
    
    Returns:
        Updated node run record
    """
    fields = payload.model_dump(exclude_unset=True)
    if not fields:
        # Return current state
        stmt = text("""
            SELECT node_run_detail_id, workflow_run_id, node_id, tenant_id, status, sequence,
                   started_at, completed_at, duration_ms, rows_processed, error_message, output_preview
            FROM maple.node_run_details
            WHERE node_run_detail_id = :node_run_detail_id AND tenant_id = :tenant_id
        """)
        result = await db.execute(stmt, {
            "node_run_detail_id": node_run_detail_id,
            "tenant_id": tenant_id
        })
        row = result.mappings().first()
        if not row:
            raise HTTPException(status_code=404, detail="Node run not found")
        return NodeRunDetailOut(**row)
    
    # Add completed_at if status is terminal
    if "status" in fields and fields["status"] in ("success", "failed"):
        fields["completed_at"] = "NOW()"
    
    set_parts = []
    params = {
        "node_run_detail_id": node_run_detail_id,
        "workflow_run_id": workflow_run_id,
        "tenant_id": tenant_id
    }
    
    for key, value in fields.items():
        if key == "completed_at":
            set_parts.append(f"{key} = NOW()")
        elif key == "output_preview":
            set_parts.append(f"{key} = CAST(:{key} AS JSONB)")
            params[key] = value
        else:
            set_parts.append(f"{key} = :{key}")
            params[key] = value
    
    set_clause = ", ".join(set_parts) + ", updated_at = NOW()"
    
    stmt = text(f"""
        UPDATE maple.node_run_details SET {set_clause}
        WHERE node_run_detail_id = :node_run_detail_id
          AND workflow_run_id = :workflow_run_id
          AND tenant_id = :tenant_id
        RETURNING node_run_detail_id, workflow_run_id, node_id, tenant_id, status, sequence,
                  started_at, completed_at, duration_ms, rows_processed, error_message, output_preview
    """)
    
    result = await db.execute(stmt, params)
    row = result.mappings().first()
    if not row:
        raise HTTPException(status_code=404, detail="Node run not found")
    await db.commit()
    return NodeRunDetailOut(**row)

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from backend.db.session import get_db
from backend.schemas.workflows import WorkflowCreate, WorkflowUpdate, WorkflowOut
from backend.core.auth import get_current_user, get_optional_user

router = APIRouter()

@router.post("/", response_model=WorkflowOut, status_code=201)
async def create_workflow(
    payload: WorkflowCreate,
    user: dict = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    # Insert into maple.workflows with ON CONFLICT upsert on workflow_id
    stmt = text(
        """
        INSERT INTO maple.workflows (
            workflow_id, tenant_id, workspace_id, folder_id, job_id,
            workflow_name, description, status, created_at
        ) VALUES (
            :workflow_id, :tenant_id, :workspace_id, :folder_id, :job_id,
            :workflow_name, :description, 'Active', NOW()
        )
        ON CONFLICT (workflow_id) DO UPDATE SET
            workflow_name = EXCLUDED.workflow_name,
            description = EXCLUDED.description,
            updated_at = NOW()
        RETURNING workflow_id, tenant_id, workflow_name, status, workspace_id, folder_id, job_id
        """
    )
    res = await db.execute(stmt, payload.model_dump())
    row = res.mappings().first()
    await db.commit()
    return WorkflowOut(**row)

@router.get("/{workflow_id}", response_model=WorkflowOut)
async def get_workflow(
    workflow_id: str,
    tenant_id: str,
    user: dict = Depends(get_optional_user),
    db: AsyncSession = Depends(get_db)
):
    stmt = text(
        """
        SELECT workflow_id, tenant_id, workflow_name, status, workspace_id, folder_id, job_id
        FROM maple.workflows
        WHERE workflow_id = :workflow_id AND tenant_id = :tenant_id AND deleted = false
        """
    )
    res = await db.execute(stmt, {"workflow_id": workflow_id, "tenant_id": tenant_id})
    row = res.mappings().first()
    if not row:
        raise HTTPException(status_code=404, detail="Workflow not found")
    return WorkflowOut(**row)

@router.get("/", response_model=list[WorkflowOut])
async def list_workflows(tenant_id: str, limit: int = 50, offset: int = 0, db: AsyncSession = Depends(get_db)):
    stmt = text(
        """
        SELECT workflow_id, tenant_id, workflow_name, status, workspace_id, folder_id, job_id
        FROM maple.workflows
        WHERE tenant_id = :tenant_id AND deleted = false
        ORDER BY created_at DESC
        LIMIT :limit OFFSET :offset
        """
    )
    res = await db.execute(stmt, {"tenant_id": tenant_id, "limit": limit, "offset": offset})
    rows = res.mappings().all()
    return [WorkflowOut(**r) for r in rows]

@router.patch("/{workflow_id}", response_model=WorkflowOut)
async def update_workflow(
    workflow_id: str,
    payload: WorkflowUpdate,
    user: dict = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    # Extract tenant_id from authenticated user
    tenant_id = user["tenant_id"]
    fields = payload.model_dump(exclude_unset=True)
    if not fields:
        # no-op fetch current
        return await get_workflow(workflow_id, tenant_id, db)

    set_parts = []
    params = {"workflow_id": workflow_id, "tenant_id": tenant_id}
    for k, v in fields.items():
        set_parts.append(f"{k} = :{k}")
        params[k] = v
    set_clause = ", ".join(set_parts) + ", updated_at = NOW()"

    stmt = text(
        f"""
        UPDATE maple.workflows SET {set_clause}
        WHERE workflow_id = :workflow_id AND tenant_id = :tenant_id AND deleted = false
        RETURNING workflow_id, tenant_id, workflow_name, status, workspace_id, folder_id, job_id
        """
    )
    res = await db.execute(stmt, params)
    row = res.mappings().first()
    if not row:
        raise HTTPException(status_code=404, detail="Workflow not found or not updated")
    await db.commit()
    return WorkflowOut(**row)

@router.delete("/{workflow_id}", status_code=204)
async def delete_workflow(
    workflow_id: str,
    user: dict = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    # Extract tenant_id from authenticated user
    tenant_id = user["tenant_id"]
    stmt = text(
        """
        UPDATE maple.workflows SET deleted = true, updated_at = NOW()
        WHERE workflow_id = :workflow_id AND tenant_id = :tenant_id AND deleted = false
        """
    )
    await db.execute(stmt, {"workflow_id": workflow_id, "tenant_id": tenant_id})
    await db.commit()
    return None

@router.get("/{workflow_id}/graph")
async def get_workflow_graph(workflow_id: str, tenant_id: str, db: AsyncSession = Depends(get_db)):
    nodes_stmt = text(
        """
        SELECT node_id, tenant_id, workflow_id, node_type, connector_id, connector_name, node_config, position,
               sequence, workspace_id, folder_id, job_id
        FROM maple.workflow_nodes
        WHERE workflow_id = :workflow_id AND tenant_id = :tenant_id
        ORDER BY sequence ASC
        """
    )
    edges_stmt = text(
        """
        SELECT workflow_edge_id, tenant_id, workspace_id, folder_id, job_id, workflow_id,
               from_node_id, to_node_id, edge_type, "condition"
        FROM maple.workflow_edges
        WHERE workflow_id = :workflow_id AND tenant_id = :tenant_id
        """
    )
    nres = await db.execute(nodes_stmt, {"workflow_id": workflow_id, "tenant_id": tenant_id})
    eres = await db.execute(edges_stmt, {"workflow_id": workflow_id, "tenant_id": tenant_id})
    nodes = nres.mappings().all()
    edges = eres.mappings().all()
    return {"nodes": nodes, "edges": edges}

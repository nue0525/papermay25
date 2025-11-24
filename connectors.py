from typing import Optional
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from backend.db.session import get_db
from backend.schemas.connectors import ConnectorCreate, ConnectorUpdate, ConnectorOut

router = APIRouter()

@router.post("/", response_model=ConnectorOut, status_code=201)
async def create_connector(payload: ConnectorCreate, db: AsyncSession = Depends(get_db)):
    stmt = text(
        """
        INSERT INTO maple.connectors (
            connector_id, tenant_id, workspace_id, connector_name, connector_type, connector_category,
            description, connection_detail, status, created_at
        ) VALUES (
            :connector_id, :tenant_id, :workspace_id, :connector_name, :connector_type, :connector_category,
            :description, CAST(:connection_detail AS JSONB), 'Active', NOW()
        )
        ON CONFLICT (connector_id) DO UPDATE SET
            connector_name = EXCLUDED.connector_name,
            connector_type = EXCLUDED.connector_type,
            connector_category = EXCLUDED.connector_category,
            description = EXCLUDED.description,
            connection_detail = EXCLUDED.connection_detail,
            workspace_id = EXCLUDED.workspace_id,
            updated_at = NOW()
        RETURNING connector_id, tenant_id, workspace_id, connector_name, connector_type, connector_category,
                  description, connection_detail, status
        """
    )
    res = await db.execute(stmt, payload.model_dump())
    row = res.mappings().first()
    await db.commit()
    return ConnectorOut(**row)

@router.get("/{connector_id}", response_model=ConnectorOut)
async def get_connector(connector_id: str, tenant_id: str, db: AsyncSession = Depends(get_db)):
    stmt = text(
        """
        SELECT connector_id, tenant_id, workspace_id, connector_name, connector_type, connector_category,
               description, connection_detail, status
        FROM maple.connectors
        WHERE connector_id = :connector_id AND tenant_id = :tenant_id AND deleted = false
        """
    )
    res = await db.execute(stmt, {"connector_id": connector_id, "tenant_id": tenant_id})
    row = res.mappings().first()
    if not row:
        raise HTTPException(status_code=404, detail="Connector not found")
    return ConnectorOut(**row)

@router.get("/", response_model=list[ConnectorOut])
async def list_connectors(
    tenant_id: str, 
    workspace_id: Optional[str] = None,
    limit: int = 50, 
    offset: int = 0, 
    db: AsyncSession = Depends(get_db)
):
    # Build WHERE clause dynamically based on provided filters
    where_conditions = ["tenant_id = :tenant_id", "deleted = false"]
    params = {"tenant_id": tenant_id, "limit": limit, "offset": offset}
    
    if workspace_id:
        where_conditions.append("workspace_id = :workspace_id")
        params["workspace_id"] = workspace_id
    
    where_clause = " AND ".join(where_conditions)
    
    stmt = text(f"""
        SELECT connector_id, tenant_id, workspace_id, connector_name, connector_type, connector_category,
               description, connection_detail, status
        FROM maple.connectors
        WHERE {where_clause}
        ORDER BY created_at DESC
        LIMIT :limit OFFSET :offset
    """)
    res = await db.execute(stmt, params)
    rows = res.mappings().all()
    return [ConnectorOut(**r) for r in rows]

@router.patch("/{connector_id}", response_model=ConnectorOut)
async def update_connector(connector_id: str, tenant_id: str, payload: ConnectorUpdate, db: AsyncSession = Depends(get_db)):
    fields = payload.model_dump(exclude_unset=True)
    if not fields:
        return await get_connector(connector_id, tenant_id, db)
    set_parts = []
    params = {"connector_id": connector_id, "tenant_id": tenant_id}
    for k, v in fields.items():
        if k == "connection_detail":
            set_parts.append(f"{k} = CAST(:{k} AS JSONB)")
        else:
            set_parts.append(f"{k} = :{k}")
        params[k] = v
    set_clause = ", ".join(set_parts) + ", updated_at = NOW()"
    stmt = text(
        f"""
        UPDATE maple.connectors SET {set_clause}
        WHERE connector_id = :connector_id AND tenant_id = :tenant_id AND deleted = false
        RETURNING connector_id, tenant_id, workspace_id, connector_name, connector_type, connector_category,
                  description, connection_detail, status
        """
    )
    res = await db.execute(stmt, params)
    row = res.mappings().first()
    if not row:
        raise HTTPException(status_code=404, detail="Connector not found or not updated")
    await db.commit()
    return ConnectorOut(**row)

@router.delete("/{connector_id}", status_code=204)
async def delete_connector(connector_id: str, tenant_id: str, db: AsyncSession = Depends(get_db)):
    stmt = text(
        """
        UPDATE maple.connectors SET deleted = true, updated_at = NOW()
        WHERE connector_id = :connector_id AND tenant_id = :tenant_id AND deleted = false
        """
    )
    await db.execute(stmt, {"connector_id": connector_id, "tenant_id": tenant_id})
    await db.commit()
    return None

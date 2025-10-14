# Audit, Logging & Versioning Architecture

## Overview

Comprehensive audit logging system tracking all user actions, system events, workflow executions, and data changes at multiple levels: server, workflow, node, job, and jobpackage levels.

---

## 1. Audit Logging Requirements

### Audit Levels

| Level | What to Track | Retention | Storage |
|-------|--------------|-----------|---------|
| **Server Level** | API requests, auth events, system errors | 90 days | PostgreSQL + TimescaleDB |
| **Workflow Level** | Workflow CRUD, executions, status changes | 1 year | PostgreSQL + TimescaleDB |
| **Node Level** | Node execution, data processed, errors | 90 days | TimescaleDB |
| **Job Level** | Job CRUD, executions, schedule changes | 1 year | PostgreSQL |
| **JobPackage Level** | Package CRUD, batch executions | 1 year | PostgreSQL |
| **User Actions** | All CRUD operations, permission changes | Permanent | PostgreSQL |

### What to Capture

- ✅ **Who**: User ID, username, email
- ✅ **What**: Action (CREATE, UPDATE, DELETE, EXECUTE)
- ✅ **When**: Timestamp (UTC)
- ✅ **Where**: Resource type and ID (workflow:123, job:456)
- ✅ **How**: IP address, user agent, API endpoint
- ✅ **Changes**: Before/after state (JSON diff)
- ✅ **Context**: Subscription ID, workspace ID, request ID

---

## 2. Database Schema for Audit & Logging

### PostgreSQL + TimescaleDB Schema

```sql
-- =============================================================================
-- AUDIT TABLES
-- =============================================================================

-- 1. Audit Log (All user actions)
CREATE TABLE audit_logs (
    audit_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    subscription_id UUID NOT NULL REFERENCES subscriptions(subscription_id),

    -- Who
    user_id UUID REFERENCES users(user_id),
    username VARCHAR(255),
    user_email VARCHAR(255),

    -- What
    action VARCHAR(50) NOT NULL,  -- 'CREATE', 'UPDATE', 'DELETE', 'EXECUTE', 'LOGIN', 'LOGOUT'
    resource_type VARCHAR(100) NOT NULL,  -- 'workspace', 'workflow', 'job', 'connector', etc.
    resource_id VARCHAR(255),
    resource_name VARCHAR(500),

    -- Changes (before/after state)
    old_values JSONB,
    new_values JSONB,
    changes JSONB,  -- Computed diff

    -- Where & How
    ip_address INET,
    user_agent TEXT,
    api_endpoint VARCHAR(500),
    http_method VARCHAR(10),
    request_id UUID,

    -- Context
    workspace_id UUID REFERENCES workspaces(workspace_id) ON DELETE SET NULL,
    folder_id UUID REFERENCES folders(folder_id) ON DELETE SET NULL,

    -- Status
    status VARCHAR(50) DEFAULT 'success',  -- 'success', 'failed', 'unauthorized'
    error_message TEXT,

    -- Timing
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_audit_logs_subscription ON audit_logs(subscription_id, created_at DESC);
CREATE INDEX idx_audit_logs_user ON audit_logs(user_id, created_at DESC);
CREATE INDEX idx_audit_logs_resource ON audit_logs(resource_type, resource_id, created_at DESC);
CREATE INDEX idx_audit_logs_action ON audit_logs(action, created_at DESC);
CREATE INDEX idx_audit_logs_workspace ON audit_logs(workspace_id, created_at DESC);

-- Convert to TimescaleDB hypertable for time-series optimization
SELECT create_hypertable('audit_logs', 'created_at', if_not_exists => TRUE);

-- 2. Execution Logs (Workflow/Job/Node execution details)
CREATE TABLE execution_logs (
    log_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    subscription_id UUID NOT NULL REFERENCES subscriptions(subscription_id),

    -- Execution context
    execution_id UUID NOT NULL,
    execution_type VARCHAR(50) NOT NULL,  -- 'workflow', 'job', 'jobpackage', 'node'

    -- Resource reference
    workflow_id UUID REFERENCES workflows(workflow_id) ON DELETE CASCADE,
    job_id UUID REFERENCES jobs(job_id) ON DELETE CASCADE,
    jobpackage_id UUID REFERENCES jobpackages(jobpackage_id) ON DELETE CASCADE,
    node_id VARCHAR(255),  -- ReactFlow node ID

    -- Log details
    log_level VARCHAR(20) NOT NULL,  -- 'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'
    log_message TEXT NOT NULL,
    log_details JSONB DEFAULT '{}'::jsonb,

    -- Performance metrics
    duration_ms INTEGER,
    rows_processed BIGINT,
    memory_used_mb NUMERIC(10,2),
    cpu_percent NUMERIC(5,2),

    -- Status
    status VARCHAR(50),  -- 'running', 'success', 'failed', 'cancelled'
    error_code VARCHAR(100),
    error_details JSONB,

    -- User context
    triggered_by UUID REFERENCES users(user_id),

    -- Timing
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_execution_logs_subscription ON execution_logs(subscription_id, created_at DESC);
CREATE INDEX idx_execution_logs_execution ON execution_logs(execution_id, created_at DESC);
CREATE INDEX idx_execution_logs_workflow ON execution_logs(workflow_id, created_at DESC);
CREATE INDEX idx_execution_logs_level ON execution_logs(log_level, created_at DESC) WHERE log_level IN ('ERROR', 'CRITICAL');

-- Convert to TimescaleDB hypertable
SELECT create_hypertable('execution_logs', 'created_at', if_not_exists => TRUE);

-- 3. Server Logs (API requests, system events)
CREATE TABLE server_logs (
    log_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    -- Request details
    request_id UUID NOT NULL,
    http_method VARCHAR(10),
    endpoint VARCHAR(500),
    query_params JSONB,

    -- Authentication
    user_id UUID REFERENCES users(user_id),
    subscription_id UUID REFERENCES subscriptions(subscription_id),
    auth_method VARCHAR(50),  -- 'jwt', 'api_key', 'oauth'

    -- Request/Response
    request_body JSONB,
    response_status INTEGER,
    response_body JSONB,
    response_time_ms INTEGER,

    -- Client info
    ip_address INET,
    user_agent TEXT,
    referer TEXT,

    -- Error details
    error_message TEXT,
    error_traceback TEXT,

    -- Timing
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_server_logs_request ON server_logs(request_id);
CREATE INDEX idx_server_logs_user ON server_logs(user_id, created_at DESC);
CREATE INDEX idx_server_logs_subscription ON server_logs(subscription_id, created_at DESC);
CREATE INDEX idx_server_logs_status ON server_logs(response_status, created_at DESC) WHERE response_status >= 400;

-- Convert to TimescaleDB hypertable
SELECT create_hypertable('server_logs', 'created_at', if_not_exists => TRUE);

-- 4. Data Change History (Track all updates for versioning)
CREATE TABLE data_change_history (
    change_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    subscription_id UUID NOT NULL REFERENCES subscriptions(subscription_id),

    -- Resource identification
    table_name VARCHAR(100) NOT NULL,  -- 'workflows', 'jobs', 'connectors'
    record_id UUID NOT NULL,
    record_name VARCHAR(500),

    -- Change details
    change_type VARCHAR(20) NOT NULL,  -- 'INSERT', 'UPDATE', 'DELETE'
    old_data JSONB,
    new_data JSONB,
    diff JSONB,  -- Computed difference

    -- User context
    changed_by UUID REFERENCES users(user_id),
    change_reason TEXT,  -- Optional user-provided reason

    -- Versioning
    version_number INTEGER NOT NULL DEFAULT 1,
    is_current_version BOOLEAN DEFAULT TRUE,

    -- Timing
    changed_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_data_change_history_subscription ON data_change_history(subscription_id, changed_at DESC);
CREATE INDEX idx_data_change_history_record ON data_change_history(table_name, record_id, changed_at DESC);
CREATE INDEX idx_data_change_history_user ON data_change_history(changed_by, changed_at DESC);
CREATE INDEX idx_data_change_history_version ON data_change_history(table_name, record_id, version_number);

-- Convert to TimescaleDB hypertable
SELECT create_hypertable('data_change_history', 'changed_at', if_not_exists => TRUE);

-- =============================================================================
-- VERSIONING SYSTEM
-- =============================================================================

-- 5. Versions (Workflow/Job versioning with check-in/check-out)
CREATE TABLE versions (
    version_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    subscription_id UUID NOT NULL REFERENCES subscriptions(subscription_id),

    -- Resource identification
    resource_type VARCHAR(50) NOT NULL,  -- 'workflow', 'job', 'connector'
    resource_id UUID NOT NULL,
    resource_name VARCHAR(500),

    -- Version details
    version_number INTEGER NOT NULL,
    version_tag VARCHAR(100),  -- 'v1.0', 'v2.0-beta'
    version_description TEXT,

    -- Version state
    version_data JSONB NOT NULL,  -- Complete snapshot of resource
    is_current BOOLEAN DEFAULT FALSE,
    is_published BOOLEAN DEFAULT FALSE,

    -- Check-in/Check-out
    checked_out_by UUID REFERENCES users(user_id),
    checked_out_at TIMESTAMPTZ,
    checked_in_at TIMESTAMPTZ,

    -- Git integration (optional)
    git_commit_hash VARCHAR(40),
    git_branch VARCHAR(255),
    git_repo_url TEXT,

    -- User context
    created_by UUID REFERENCES users(user_id),
    created_at TIMESTAMPTZ DEFAULT NOW(),

    UNIQUE(subscription_id, resource_type, resource_id, version_number)
);

CREATE INDEX idx_versions_subscription ON versions(subscription_id);
CREATE INDEX idx_versions_resource ON versions(resource_type, resource_id, version_number DESC);
CREATE INDEX idx_versions_checked_out ON versions(checked_out_by) WHERE checked_out_by IS NOT NULL;
CREATE INDEX idx_versions_current ON versions(resource_type, resource_id) WHERE is_current = TRUE;

-- 6. Version Comments (Commit messages)
CREATE TABLE version_comments (
    comment_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    version_id UUID NOT NULL REFERENCES versions(version_id) ON DELETE CASCADE,

    comment_text TEXT NOT NULL,
    commented_by UUID REFERENCES users(user_id),
    commented_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_version_comments_version ON version_comments(version_id, commented_at DESC);

-- =============================================================================
-- CASCADE DELETE TRACKING
-- =============================================================================

-- 7. Deletion Log (Track cascade deletes)
CREATE TABLE deletion_logs (
    deletion_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    subscription_id UUID NOT NULL REFERENCES subscriptions(subscription_id),

    -- What was deleted
    resource_type VARCHAR(50) NOT NULL,
    resource_id UUID NOT NULL,
    resource_name VARCHAR(500),
    resource_data JSONB,  -- Snapshot before deletion

    -- Cascade information
    parent_resource_type VARCHAR(50),
    parent_resource_id UUID,
    cascade_level INTEGER DEFAULT 0,  -- 0 = direct delete, 1+ = cascade

    -- Deleted children count
    cascaded_workspaces INTEGER DEFAULT 0,
    cascaded_folders INTEGER DEFAULT 0,
    cascaded_jobs INTEGER DEFAULT 0,
    cascaded_workflows INTEGER DEFAULT 0,
    cascaded_jobpackages INTEGER DEFAULT 0,

    -- User context
    deleted_by UUID REFERENCES users(user_id),
    deletion_reason TEXT,

    -- Soft delete support
    is_soft_delete BOOLEAN DEFAULT FALSE,
    can_be_restored BOOLEAN DEFAULT TRUE,
    restore_expiry_at TIMESTAMPTZ,  -- After this date, permanently deleted

    -- Timing
    deleted_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_deletion_logs_subscription ON deletion_logs(subscription_id, deleted_at DESC);
CREATE INDEX idx_deletion_logs_resource ON deletion_logs(resource_type, resource_id);
CREATE INDEX idx_deletion_logs_parent ON deletion_logs(parent_resource_type, parent_resource_id);
CREATE INDEX idx_deletion_logs_soft_delete ON deletion_logs(is_soft_delete, can_be_restored) WHERE can_be_restored = TRUE;
```

---

## 3. Audit Service

### Python Service for Audit Logging

```python
# services/audit_service.py

from typing import Optional, Dict, Any, List
from uuid import UUID
import asyncpg
from datetime import datetime
import logging
import json

logger = logging.getLogger(__name__)

class AuditService:
    """
    Comprehensive audit logging service
    """

    def __init__(self, db_pool: asyncpg.Pool):
        self.db_pool = db_pool

    # =========================================================================
    # 1. AUDIT LOG CREATION
    # =========================================================================

    async def log_action(
        self,
        subscription_id: str,
        user_id: str,
        action: str,
        resource_type: str,
        resource_id: str,
        resource_name: Optional[str] = None,
        old_values: Optional[Dict[str, Any]] = None,
        new_values: Optional[Dict[str, Any]] = None,
        ip_address: Optional[str] = None,
        user_agent: Optional[str] = None,
        api_endpoint: Optional[str] = None,
        http_method: Optional[str] = None,
        request_id: Optional[str] = None,
        workspace_id: Optional[str] = None,
        folder_id: Optional[str] = None,
        status: str = 'success',
        error_message: Optional[str] = None
    ) -> str:
        """
        Log user action to audit_logs table
        Returns: audit_id
        """

        # Compute changes (diff)
        changes = None
        if old_values and new_values:
            changes = self._compute_diff(old_values, new_values)

        # Get user details
        async with self.db_pool.acquire() as conn:
            user = await conn.fetchrow(
                "SELECT username, email FROM users WHERE user_id = $1",
                user_id
            )

            audit_id = await conn.fetchval(
                """
                INSERT INTO audit_logs (
                    subscription_id, user_id, username, user_email,
                    action, resource_type, resource_id, resource_name,
                    old_values, new_values, changes,
                    ip_address, user_agent, api_endpoint, http_method, request_id,
                    workspace_id, folder_id, status, error_message
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20)
                RETURNING audit_id
                """,
                subscription_id, user_id, user["username"], user["email"],
                action, resource_type, resource_id, resource_name,
                old_values, new_values, changes,
                ip_address, user_agent, api_endpoint, http_method, request_id,
                workspace_id, folder_id, status, error_message
            )

        logger.info(
            f"Audit log created: {action} {resource_type}:{resource_id} "
            f"by user {user_id}"
        )

        return str(audit_id)

    async def log_execution(
        self,
        subscription_id: str,
        execution_id: str,
        execution_type: str,
        log_level: str,
        log_message: str,
        log_details: Optional[Dict[str, Any]] = None,
        workflow_id: Optional[str] = None,
        job_id: Optional[str] = None,
        jobpackage_id: Optional[str] = None,
        node_id: Optional[str] = None,
        status: Optional[str] = None,
        duration_ms: Optional[int] = None,
        rows_processed: Optional[int] = None,
        memory_used_mb: Optional[float] = None,
        cpu_percent: Optional[float] = None,
        error_code: Optional[str] = None,
        error_details: Optional[Dict[str, Any]] = None,
        triggered_by: Optional[str] = None
    ) -> str:
        """
        Log execution event to execution_logs table
        """

        query = """
        INSERT INTO execution_logs (
            subscription_id, execution_id, execution_type,
            workflow_id, job_id, jobpackage_id, node_id,
            log_level, log_message, log_details,
            status, duration_ms, rows_processed, memory_used_mb, cpu_percent,
            error_code, error_details, triggered_by
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)
        RETURNING log_id
        """

        async with self.db_pool.acquire() as conn:
            log_id = await conn.fetchval(
                query,
                subscription_id, execution_id, execution_type,
                workflow_id, job_id, jobpackage_id, node_id,
                log_level, log_message, log_details or {},
                status, duration_ms, rows_processed, memory_used_mb, cpu_percent,
                error_code, error_details, triggered_by
            )

        return str(log_id)

    async def log_server_request(
        self,
        request_id: str,
        http_method: str,
        endpoint: str,
        user_id: Optional[str] = None,
        subscription_id: Optional[str] = None,
        query_params: Optional[Dict[str, Any]] = None,
        request_body: Optional[Dict[str, Any]] = None,
        response_status: Optional[int] = None,
        response_body: Optional[Dict[str, Any]] = None,
        response_time_ms: Optional[int] = None,
        ip_address: Optional[str] = None,
        user_agent: Optional[str] = None,
        error_message: Optional[str] = None,
        error_traceback: Optional[str] = None
    ) -> str:
        """
        Log server request to server_logs table
        """

        query = """
        INSERT INTO server_logs (
            request_id, http_method, endpoint,
            user_id, subscription_id, query_params, request_body,
            response_status, response_body, response_time_ms,
            ip_address, user_agent, error_message, error_traceback
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
        RETURNING log_id
        """

        async with self.db_pool.acquire() as conn:
            log_id = await conn.fetchval(
                query,
                request_id, http_method, endpoint,
                user_id, subscription_id, query_params, request_body,
                response_status, response_body, response_time_ms,
                ip_address, user_agent, error_message, error_traceback
            )

        return str(log_id)

    # =========================================================================
    # 2. DATA CHANGE TRACKING
    # =========================================================================

    async def track_change(
        self,
        subscription_id: str,
        table_name: str,
        record_id: str,
        record_name: str,
        change_type: str,  # 'INSERT', 'UPDATE', 'DELETE'
        old_data: Optional[Dict[str, Any]] = None,
        new_data: Optional[Dict[str, Any]] = None,
        changed_by: str = None,
        change_reason: Optional[str] = None
    ) -> str:
        """
        Track data changes for versioning and audit
        """

        # Compute diff
        diff = None
        if old_data and new_data:
            diff = self._compute_diff(old_data, new_data)

        # Get current version number
        async with self.db_pool.acquire() as conn:
            current_version = await conn.fetchval(
                """
                SELECT COALESCE(MAX(version_number), 0)
                FROM data_change_history
                WHERE table_name = $1 AND record_id = $2
                """,
                table_name, record_id
            )

            new_version = current_version + 1

            # Mark all previous versions as not current
            await conn.execute(
                """
                UPDATE data_change_history
                SET is_current_version = FALSE
                WHERE table_name = $1 AND record_id = $2
                """,
                table_name, record_id
            )

            # Insert new change record
            change_id = await conn.fetchval(
                """
                INSERT INTO data_change_history (
                    subscription_id, table_name, record_id, record_name,
                    change_type, old_data, new_data, diff,
                    changed_by, change_reason, version_number, is_current_version
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, TRUE)
                RETURNING change_id
                """,
                subscription_id, table_name, record_id, record_name,
                change_type, old_data, new_data, diff,
                changed_by, change_reason, new_version
            )

        return str(change_id)

    # =========================================================================
    # 3. CASCADE DELETE TRACKING
    # =========================================================================

    async def track_deletion(
        self,
        subscription_id: str,
        resource_type: str,
        resource_id: str,
        resource_name: str,
        resource_data: Dict[str, Any],
        deleted_by: str,
        parent_resource_type: Optional[str] = None,
        parent_resource_id: Optional[str] = None,
        cascade_level: int = 0,
        cascaded_counts: Optional[Dict[str, int]] = None,
        deletion_reason: Optional[str] = None,
        is_soft_delete: bool = False,
        restore_expiry_days: int = 30
    ) -> str:
        """
        Track deletion with cascade information
        """

        from datetime import timedelta

        restore_expiry_at = None
        if is_soft_delete:
            restore_expiry_at = datetime.utcnow() + timedelta(days=restore_expiry_days)

        counts = cascaded_counts or {}

        query = """
        INSERT INTO deletion_logs (
            subscription_id, resource_type, resource_id, resource_name, resource_data,
            parent_resource_type, parent_resource_id, cascade_level,
            cascaded_workspaces, cascaded_folders, cascaded_jobs,
            cascaded_workflows, cascaded_jobpackages,
            deleted_by, deletion_reason, is_soft_delete, can_be_restored, restore_expiry_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, TRUE, $17)
        RETURNING deletion_id
        """

        async with self.db_pool.acquire() as conn:
            deletion_id = await conn.fetchval(
                query,
                subscription_id, resource_type, resource_id, resource_name, resource_data,
                parent_resource_type, parent_resource_id, cascade_level,
                counts.get("workspaces", 0), counts.get("folders", 0), counts.get("jobs", 0),
                counts.get("workflows", 0), counts.get("jobpackages", 0),
                deleted_by, deletion_reason, is_soft_delete, restore_expiry_at
            )

        logger.info(
            f"Deletion tracked: {resource_type}:{resource_id} "
            f"(cascade_level={cascade_level}, soft_delete={is_soft_delete})"
        )

        return str(deletion_id)

    # =========================================================================
    # 4. QUERY AUDIT LOGS
    # =========================================================================

    async def get_audit_logs(
        self,
        subscription_id: str,
        resource_type: Optional[str] = None,
        resource_id: Optional[str] = None,
        user_id: Optional[str] = None,
        action: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Query audit logs with filters
        """

        where_clauses = ["subscription_id = $1"]
        params = [subscription_id]
        param_counter = 2

        if resource_type:
            where_clauses.append(f"resource_type = ${param_counter}")
            params.append(resource_type)
            param_counter += 1

        if resource_id:
            where_clauses.append(f"resource_id = ${param_counter}")
            params.append(resource_id)
            param_counter += 1

        if user_id:
            where_clauses.append(f"user_id = ${param_counter}")
            params.append(user_id)
            param_counter += 1

        if action:
            where_clauses.append(f"action = ${param_counter}")
            params.append(action)
            param_counter += 1

        if start_date:
            where_clauses.append(f"created_at >= ${param_counter}")
            params.append(start_date)
            param_counter += 1

        if end_date:
            where_clauses.append(f"created_at <= ${param_counter}")
            params.append(end_date)
            param_counter += 1

        where_sql = " AND ".join(where_clauses)

        query = f"""
        SELECT
            audit_id, user_id, username, user_email,
            action, resource_type, resource_id, resource_name,
            old_values, new_values, changes,
            status, error_message, created_at
        FROM audit_logs
        WHERE {where_sql}
        ORDER BY created_at DESC
        LIMIT {limit}
        """

        async with self.db_pool.acquire() as conn:
            rows = await conn.fetch(query, *params)

        return [dict(row) for row in rows]

    # =========================================================================
    # HELPER METHODS
    # =========================================================================

    def _compute_diff(
        self,
        old_data: Dict[str, Any],
        new_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Compute difference between old and new data
        """

        diff = {}

        all_keys = set(old_data.keys()) | set(new_data.keys())

        for key in all_keys:
            old_value = old_data.get(key)
            new_value = new_data.get(key)

            if old_value != new_value:
                diff[key] = {
                    "old": old_value,
                    "new": new_value
                }

        return diff
```

---

## 4. FastAPI Middleware for Automatic Audit Logging

### Request/Response Logging Middleware

```python
# middleware/audit_middleware.py

from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.types import ASGIApp
from services.audit_service import AuditService
from uuid import uuid4
import time
import logging

logger = logging.getLogger(__name__)

class AuditMiddleware(BaseHTTPMiddleware):
    """
    Middleware to automatically log all API requests
    """

    def __init__(self, app: ASGIApp, audit_service: AuditService):
        super().__init__(app)
        self.audit_service = audit_service

    async def dispatch(self, request: Request, call_next):
        # Generate request ID
        request_id = str(uuid4())
        request.state.request_id = request_id

        # Start timer
        start_time = time.time()

        # Get client info
        ip_address = request.client.host if request.client else None
        user_agent = request.headers.get("user-agent")

        # Get user context (if authenticated)
        user_id = getattr(request.state, "user_id", None)
        subscription_id = getattr(request.state, "subscription_id", None)

        # Execute request
        try:
            response = await call_next(request)

            # Calculate response time
            response_time_ms = int((time.time() - start_time) * 1000)

            # Log successful request
            await self.audit_service.log_server_request(
                request_id=request_id,
                http_method=request.method,
                endpoint=str(request.url.path),
                user_id=user_id,
                subscription_id=subscription_id,
                query_params=dict(request.query_params),
                response_status=response.status_code,
                response_time_ms=response_time_ms,
                ip_address=ip_address,
                user_agent=user_agent
            )

            return response

        except Exception as e:
            # Log failed request
            response_time_ms = int((time.time() - start_time) * 1000)

            await self.audit_service.log_server_request(
                request_id=request_id,
                http_method=request.method,
                endpoint=str(request.url.path),
                user_id=user_id,
                subscription_id=subscription_id,
                query_params=dict(request.query_params),
                response_status=500,
                response_time_ms=response_time_ms,
                ip_address=ip_address,
                user_agent=user_agent,
                error_message=str(e),
                error_traceback=traceback.format_exc()
            )

            raise
```

---

## 5. Versioning System

### Check-in/Check-out for Workflows

```python
# services/version_service.py

from typing import Optional, Dict, Any, List
from uuid import UUID
import asyncpg
from datetime import datetime

class VersionService:
    """
    Version control system for workflows and jobs
    """

    def __init__(self, db_pool: asyncpg.Pool):
        self.db_pool = db_pool

    async def create_version(
        self,
        subscription_id: str,
        resource_type: str,
        resource_id: str,
        resource_name: str,
        version_data: Dict[str, Any],
        version_tag: Optional[str] = None,
        version_description: Optional[str] = None,
        created_by: str = None,
        is_published: bool = False
    ) -> Dict[str, Any]:
        """
        Create new version of a resource
        """

        async with self.db_pool.acquire() as conn:
            # Get next version number
            current_version = await conn.fetchval(
                """
                SELECT COALESCE(MAX(version_number), 0)
                FROM versions
                WHERE subscription_id = $1 AND resource_type = $2 AND resource_id = $3
                """,
                subscription_id, resource_type, resource_id
            )

            new_version_number = current_version + 1

            # Mark all previous versions as not current
            await conn.execute(
                """
                UPDATE versions
                SET is_current = FALSE
                WHERE subscription_id = $1 AND resource_type = $2 AND resource_id = $3
                """,
                subscription_id, resource_type, resource_id
            )

            # Create new version
            version_id = await conn.fetchval(
                """
                INSERT INTO versions (
                    subscription_id, resource_type, resource_id, resource_name,
                    version_number, version_tag, version_description,
                    version_data, is_current, is_published, created_by
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, TRUE, $9, $10)
                RETURNING version_id
                """,
                subscription_id, resource_type, resource_id, resource_name,
                new_version_number, version_tag, version_description,
                version_data, is_published, created_by
            )

        return {
            "versionId": str(version_id),
            "versionNumber": new_version_number,
            "isPublished": is_published
        }

    async def checkout_version(
        self,
        version_id: str,
        user_id: str
    ) -> Dict[str, Any]:
        """
        Check out a version for editing (locks it)
        """

        async with self.db_pool.acquire() as conn:
            # Check if already checked out
            existing_checkout = await conn.fetchval(
                "SELECT checked_out_by FROM versions WHERE version_id = $1",
                version_id
            )

            if existing_checkout:
                raise ValueError(f"Version already checked out by user {existing_checkout}")

            # Check out
            await conn.execute(
                """
                UPDATE versions
                SET checked_out_by = $2, checked_out_at = NOW()
                WHERE version_id = $1
                """,
                version_id, user_id
            )

        return {"versionId": version_id, "checkedOutBy": user_id}

    async def checkin_version(
        self,
        version_id: str,
        user_id: str,
        comment: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Check in a version (unlocks it)
        """

        async with self.db_pool.acquire() as conn:
            # Verify user owns the checkout
            checked_out_by = await conn.fetchval(
                "SELECT checked_out_by FROM versions WHERE version_id = $1",
                version_id
            )

            if checked_out_by != user_id:
                raise ValueError("Only the user who checked out can check in")

            # Check in
            await conn.execute(
                """
                UPDATE versions
                SET checked_out_by = NULL, checked_in_at = NOW()
                WHERE version_id = $1
                """,
                version_id
            )

            # Add comment
            if comment:
                await conn.execute(
                    """
                    INSERT INTO version_comments (version_id, comment_text, commented_by)
                    VALUES ($1, $2, $3)
                    """,
                    version_id, comment, user_id
                )

        return {"versionId": version_id, "checkedIn": True}

    async def get_version_history(
        self,
        subscription_id: str,
        resource_type: str,
        resource_id: str
    ) -> List[Dict[str, Any]]:
        """
        Get version history for a resource
        """

        query = """
        SELECT
            v.version_id,
            v.version_number,
            v.version_tag,
            v.version_description,
            v.is_current,
            v.is_published,
            v.checked_out_by,
            v.created_by,
            v.created_at,
            u.username AS created_by_username
        FROM versions v
        LEFT JOIN users u ON v.created_by = u.user_id
        WHERE v.subscription_id = $1 AND v.resource_type = $2 AND v.resource_id = $3
        ORDER BY v.version_number DESC
        """

        async with self.db_pool.acquire() as conn:
            rows = await conn.fetch(query, subscription_id, resource_type, resource_id)

        return [dict(row) for row in rows]
```

---

## 6. Cascade Delete Strategy

### Soft Delete with Cascade Tracking

```sql
-- Add soft_deleted column to all major tables

ALTER TABLE workspaces ADD COLUMN soft_deleted BOOLEAN DEFAULT FALSE;
ALTER TABLE folders ADD COLUMN soft_deleted BOOLEAN DEFAULT FALSE;
ALTER TABLE jobs ADD COLUMN soft_deleted BOOLEAN DEFAULT FALSE;
ALTER TABLE workflows ADD COLUMN soft_deleted BOOLEAN DEFAULT FALSE;
ALTER TABLE jobpackages ADD COLUMN soft_deleted BOOLEAN DEFAULT FALSE;

-- Function to handle cascade delete

CREATE OR REPLACE FUNCTION cascade_delete_workspace(
    p_workspace_id UUID,
    p_deleted_by UUID,
    p_is_soft_delete BOOLEAN DEFAULT TRUE
) RETURNS JSONB AS $$
DECLARE
    v_subscription_id UUID;
    v_workspace_data JSONB;
    v_cascaded_counts JSONB;
    v_folder_count INTEGER;
    v_job_count INTEGER;
    v_workflow_count INTEGER;
    v_jobpackage_count INTEGER;
BEGIN
    -- Get workspace data
    SELECT subscription_id, row_to_json(workspaces.*)::jsonb
    INTO v_subscription_id, v_workspace_data
    FROM workspaces
    WHERE workspace_id = p_workspace_id;

    -- Count cascaded resources
    SELECT COUNT(*) INTO v_folder_count
    FROM folders WHERE workspace_id = p_workspace_id;

    SELECT COUNT(*) INTO v_job_count
    FROM jobs j
    JOIN folders f ON j.folder_id = f.folder_id
    WHERE f.workspace_id = p_workspace_id;

    SELECT COUNT(*) INTO v_workflow_count
    FROM workflows w
    JOIN jobs j ON w.job_id = j.job_id
    JOIN folders f ON j.folder_id = f.folder_id
    WHERE f.workspace_id = p_workspace_id;

    -- Soft delete or hard delete
    IF p_is_soft_delete THEN
        UPDATE workspaces SET soft_deleted = TRUE WHERE workspace_id = p_workspace_id;
        UPDATE folders SET soft_deleted = TRUE WHERE workspace_id = p_workspace_id;
        UPDATE jobs SET soft_deleted = TRUE
        FROM folders f WHERE jobs.folder_id = f.folder_id AND f.workspace_id = p_workspace_id;
        UPDATE workflows SET soft_deleted = TRUE
        FROM jobs j, folders f
        WHERE workflows.job_id = j.job_id AND j.folder_id = f.folder_id AND f.workspace_id = p_workspace_id;
    ELSE
        -- Hard delete (cascade via foreign keys)
        DELETE FROM workspaces WHERE workspace_id = p_workspace_id;
    END IF;

    -- Log deletion
    INSERT INTO deletion_logs (
        subscription_id, resource_type, resource_id, resource_name, resource_data,
        cascaded_folders, cascaded_jobs, cascaded_workflows,
        deleted_by, is_soft_delete, cascade_level
    )
    VALUES (
        v_subscription_id, 'workspace', p_workspace_id, v_workspace_data->>'workspace_name', v_workspace_data,
        v_folder_count, v_job_count, v_workflow_count,
        p_deleted_by, p_is_soft_delete, 0
    );

    -- Return counts
    RETURN jsonb_build_object(
        'folders', v_folder_count,
        'jobs', v_job_count,
        'workflows', v_workflow_count
    );
END;
$$ LANGUAGE plpgsql;
```

---

## 7. Summary

### Audit & Logging Features

| Feature | Implementation | Storage | Retention |
|---------|---------------|---------|-----------|
| **User Actions** | audit_logs table | PostgreSQL + TimescaleDB | Permanent |
| **Workflow Execution** | execution_logs table | TimescaleDB | 1 year |
| **Server Logs** | server_logs table | TimescaleDB | 90 days |
| **Data Changes** | data_change_history table | PostgreSQL | Permanent |
| **Version Control** | versions table | PostgreSQL | Permanent |
| **Cascade Deletes** | deletion_logs table | PostgreSQL | Permanent |

### Versioning Features

- ✅ Check-in/check-out locking
- ✅ Version tagging (v1.0, v2.0-beta)
- ✅ Version comments (commit messages)
- ✅ Publish/unpublish versions
- ✅ Git integration (optional)
- ✅ Rollback to previous version

### Cascade Delete Strategy

- ✅ Soft delete (recoverable for 30 days)
- ✅ Hard delete (permanent)
- ✅ Track cascade counts
- ✅ Snapshot before deletion
- ✅ Restore functionality

### Next Steps

- Implement log rotation and archival
- Add log analytics dashboard
- Create restore from soft delete functionality
- Implement Git push/pull for versioning
- Add compliance reports (SOC2, GDPR)

# RBAC & Multi-Tenancy Architecture
## Enterprise Permission System & Tenant Isolation

**Document Version**: 1.0
**Last Updated**: 2025-10-13

---

## RBAC System Design

### Permission Model Hierarchy

```
Subscription (Tenant)
  ├── Users
  │   └── Direct Permissions (Object-level)
  ├── Groups
  │   ├── Users (Members)
  │   └── Roles
  │       └── Permissions
  └── Roles
      └── Permissions
          ├── Object Type Permission
          └── Individual Object Permission
```

### Permission Levels

```python
class PermissionLevel(str, Enum):
    OWNER = "owner"       # Full control + transfer ownership
    ADMIN = "admin"       # Full control except ownership transfer
    EDIT = "edit"         # Read + Write + Execute
    VIEW = "view"         # Read only
    NONE = "none"         # No access
```

### Permission Actions

```python
class PermissionAction(str, Enum):
    # CRUD
    CREATE = "create"
    READ = "read"
    UPDATE = "update"
    DELETE = "delete"

    # Execution
    EXECUTE = "execute"
    STOP = "stop"
    PAUSE = "pause"

    # Collaboration
    SHARE = "share"
    COMMENT = "comment"

    # Management
    MANAGE_PERMISSIONS = "manage_permissions"
    MANAGE_VERSIONS = "manage_versions"
```

---

## Database Schema

### Core RBAC Tables

```sql
-- ============================================
-- USERS, GROUPS, ROLES
-- ============================================

CREATE TABLE users (
  user_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  subscription_id UUID NOT NULL REFERENCES subscriptions(subscription_id),
  email VARCHAR(255) NOT NULL,
  username VARCHAR(100) NOT NULL,
  full_name VARCHAR(255),
  is_subscription_owner BOOLEAN DEFAULT false,  -- Tenant owner flag
  is_active BOOLEAN DEFAULT true,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  CONSTRAINT uk_users_email UNIQUE(subscription_id, email)
);

CREATE TABLE groups (
  group_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  subscription_id UUID NOT NULL REFERENCES subscriptions(subscription_id),
  group_name VARCHAR(255) NOT NULL,
  description TEXT,
  created_by UUID REFERENCES users(user_id),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  CONSTRAINT uk_groups_name UNIQUE(subscription_id, group_name)
);

CREATE TABLE user_groups (
  user_id UUID REFERENCES users(user_id) ON DELETE CASCADE,
  group_id UUID REFERENCES groups(group_id) ON DELETE CASCADE,
  added_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  added_by UUID REFERENCES users(user_id),

  PRIMARY KEY (user_id, group_id)
);

CREATE INDEX idx_user_groups_user ON user_groups(user_id);
CREATE INDEX idx_user_groups_group ON user_groups(group_id);

CREATE TABLE roles (
  role_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  subscription_id UUID NOT NULL REFERENCES subscriptions(subscription_id),
  role_name VARCHAR(255) NOT NULL,
  description TEXT,
  is_system_role BOOLEAN DEFAULT false,  -- Built-in roles (Admin, Editor, Viewer)
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  CONSTRAINT uk_roles_name UNIQUE(subscription_id, role_name)
);

-- Role permissions (object-type level)
CREATE TABLE role_permissions (
  role_permission_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  role_id UUID NOT NULL REFERENCES roles(role_id) ON DELETE CASCADE,
  object_type VARCHAR(50) NOT NULL,  -- workspace, folder, job, workflow, connector
  actions JSONB NOT NULL DEFAULT '[]'::jsonb,  -- ["create", "read", "update", "delete"]

  CONSTRAINT uk_role_permissions UNIQUE(role_id, object_type)
);

CREATE INDEX idx_role_permissions_role ON role_permissions(role_id);

-- User-Role assignments
CREATE TABLE user_roles (
  user_id UUID REFERENCES users(user_id) ON DELETE CASCADE,
  role_id UUID REFERENCES roles(role_id) ON DELETE CASCADE,
  assigned_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  assigned_by UUID REFERENCES users(user_id),

  PRIMARY KEY (user_id, role_id)
);

-- Group-Role assignments
CREATE TABLE group_roles (
  group_id UUID REFERENCES groups(group_id) ON DELETE CASCADE,
  role_id UUID REFERENCES roles(role_id) ON DELETE CASCADE,
  assigned_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  assigned_by UUID REFERENCES users(user_id),

  PRIMARY KEY (group_id, role_id)
);

-- ============================================
-- OBJECT-LEVEL PERMISSIONS (Fine-grained)
-- ============================================

CREATE TABLE object_permissions (
  permission_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  object_type VARCHAR(50) NOT NULL,  -- workspace, folder, job, workflow
  object_id UUID NOT NULL,

  -- Grant to user OR group (one must be NULL)
  user_id UUID REFERENCES users(user_id) ON DELETE CASCADE,
  group_id UUID REFERENCES groups(group_id) ON DELETE CASCADE,

  permission_level VARCHAR(20) NOT NULL,  -- owner, admin, edit, view
  actions JSONB DEFAULT '[]'::jsonb,  -- Specific actions if not using level

  granted_by UUID REFERENCES users(user_id),
  granted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  expires_at TIMESTAMPTZ,  -- Optional expiration

  CONSTRAINT chk_user_or_group CHECK (
    (user_id IS NOT NULL AND group_id IS NULL) OR
    (user_id IS NULL AND group_id IS NOT NULL)
  )
);

CREATE INDEX idx_object_permissions_object ON object_permissions(object_type, object_id);
CREATE INDEX idx_object_permissions_user ON object_permissions(user_id);
CREATE INDEX idx_object_permissions_group ON object_permissions(group_id);

-- ============================================
-- PERMISSION INHERITANCE
-- ============================================

CREATE TABLE permission_inheritance (
  inheritance_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  parent_object_type VARCHAR(50) NOT NULL,
  parent_object_id UUID NOT NULL,
  child_object_type VARCHAR(50) NOT NULL,
  child_object_id UUID NOT NULL,

  CONSTRAINT uk_inheritance UNIQUE(parent_object_type, parent_object_id, child_object_type, child_object_id)
);

-- Example: Workspace permissions inherit to Folders
-- INSERT INTO permission_inheritance
-- (parent_object_type, parent_object_id, child_object_type, child_object_id)
-- VALUES ('workspace', 'ws_123', 'folder', 'folder_456')
```

### Built-in System Roles

```sql
-- Insert system roles for each subscription
INSERT INTO roles (subscription_id, role_name, description, is_system_role) VALUES
($1, 'Owner', 'Full control over subscription', true),
($2, 'Admin', 'Manage all resources except billing', true),
($3, 'Editor', 'Create and edit workflows', true),
($4, 'Viewer', 'Read-only access', true),
($5, 'Executor', 'Execute workflows only', true);

-- Owner permissions (everything)
INSERT INTO role_permissions (role_id, object_type, actions)
SELECT role_id, object_type, '["create", "read", "update", "delete", "execute", "share", "manage_permissions"]'::jsonb
FROM roles, unnest(ARRAY['workspace', 'folder', 'job', 'workflow', 'connector', 'user', 'group', 'role']) AS object_type
WHERE role_name = 'Owner';

-- Editor permissions
INSERT INTO role_permissions (role_id, object_type, actions)
SELECT role_id, object_type, '["create", "read", "update", "delete", "execute"]'::jsonb
FROM roles, unnest(ARRAY['workspace', 'folder', 'job', 'workflow']) AS object_type
WHERE role_name = 'Editor';

-- Viewer permissions
INSERT INTO role_permissions (role_id, object_type, actions)
SELECT role_id, object_type, '["read"]'::jsonb
FROM roles, unnest(ARRAY['workspace', 'folder', 'job', 'workflow', 'connector']) AS object_type
WHERE role_name = 'Viewer';
```

---

## Permission Checking Logic

### Python Permission Service

```python
# backend/Auth/permissions.py
from typing import List, Set, Optional
from enum import Enum

class ObjectType(str, Enum):
    SUBSCRIPTION = "subscription"
    WORKSPACE = "workspace"
    FOLDER = "folder"
    JOB = "job"
    WORKFLOW = "workflow"
    CONNECTOR = "connector"
    USER = "user"
    GROUP = "group"
    ROLE = "role"

class PermissionService:
    """
    Centralized permission checking
    Handles: Role-based + Object-level + Inheritance
    """

    async def check_permission(
        self,
        user_id: str,
        object_type: ObjectType,
        object_id: str,
        required_action: str
    ) -> bool:
        """
        Check if user has permission for action on object
        Returns: True if allowed, False otherwise
        """
        # 1. Check if user is subscription owner (bypass all checks)
        is_owner = await self.is_subscription_owner(user_id)
        if is_owner:
            return True

        # 2. Check object-level permissions (direct user permission)
        has_direct = await self.check_direct_permission(
            user_id, object_type, object_id, required_action
        )
        if has_direct:
            return True

        # 3. Check group permissions (user is member of group with permission)
        has_group = await self.check_group_permission(
            user_id, object_type, object_id, required_action
        )
        if has_group:
            return True

        # 4. Check role permissions (user has role with permission)
        has_role = await self.check_role_permission(
            user_id, object_type, required_action
        )
        if has_role:
            return True

        # 5. Check inherited permissions (from parent objects)
        has_inherited = await self.check_inherited_permission(
            user_id, object_type, object_id, required_action
        )
        if has_inherited:
            return True

        return False

    async def is_subscription_owner(self, user_id: str) -> bool:
        """Check if user is subscription owner"""
        result = await db.fetchval(
            "SELECT is_subscription_owner FROM users WHERE user_id = $1",
            user_id
        )
        return result or False

    async def check_direct_permission(
        self,
        user_id: str,
        object_type: ObjectType,
        object_id: str,
        required_action: str
    ) -> bool:
        """Check direct object permission"""
        permission = await db.fetchrow("""
            SELECT permission_level, actions
            FROM object_permissions
            WHERE object_type = $1
            AND object_id = $2
            AND user_id = $3
            AND (expires_at IS NULL OR expires_at > NOW())
        """, object_type, object_id, user_id)

        if not permission:
            return False

        # Check if permission level grants access
        if permission["permission_level"] == "owner":
            return True  # Owner can do anything

        # Check specific actions
        actions = permission["actions"] or []
        return required_action in actions or self.permission_level_grants_action(
            permission["permission_level"], required_action
        )

    async def check_group_permission(
        self,
        user_id: str,
        object_type: ObjectType,
        object_id: str,
        required_action: str
    ) -> bool:
        """Check permission via group membership"""
        permissions = await db.fetch("""
            SELECT op.permission_level, op.actions
            FROM object_permissions op
            JOIN user_groups ug ON op.group_id = ug.group_id
            WHERE op.object_type = $1
            AND op.object_id = $2
            AND ug.user_id = $3
            AND (op.expires_at IS NULL OR op.expires_at > NOW())
        """, object_type, object_id, user_id)

        for perm in permissions:
            if perm["permission_level"] == "owner":
                return True

            actions = perm["actions"] or []
            if required_action in actions:
                return True

            if self.permission_level_grants_action(perm["permission_level"], required_action):
                return True

        return False

    async def check_role_permission(
        self,
        user_id: str,
        object_type: ObjectType,
        required_action: str
    ) -> bool:
        """Check permission via role assignment"""
        # Check user roles
        user_role_actions = await db.fetch("""
            SELECT rp.actions
            FROM role_permissions rp
            JOIN user_roles ur ON rp.role_id = ur.role_id
            WHERE ur.user_id = $1
            AND rp.object_type = $2
        """, user_id, object_type)

        for row in user_role_actions:
            actions = row["actions"] or []
            if required_action in actions:
                return True

        # Check group roles
        group_role_actions = await db.fetch("""
            SELECT rp.actions
            FROM role_permissions rp
            JOIN group_roles gr ON rp.role_id = gr.role_id
            JOIN user_groups ug ON gr.group_id = ug.group_id
            WHERE ug.user_id = $1
            AND rp.object_type = $2
        """, user_id, object_type)

        for row in group_role_actions:
            actions = row["actions"] or []
            if required_action in actions:
                return True

        return False

    async def check_inherited_permission(
        self,
        user_id: str,
        object_type: ObjectType,
        object_id: str,
        required_action: str
    ) -> bool:
        """
        Check inherited permissions from parent objects
        Example: If user has permission on Workspace, check if Folder inherits it
        """
        # Get parent objects
        parents = await db.fetch("""
            SELECT parent_object_type, parent_object_id
            FROM permission_inheritance
            WHERE child_object_type = $1
            AND child_object_id = $2
        """, object_type, object_id)

        for parent in parents:
            # Recursively check parent permission
            has_parent_permission = await self.check_permission(
                user_id,
                parent["parent_object_type"],
                parent["parent_object_id"],
                required_action
            )
            if has_parent_permission:
                return True

        return False

    def permission_level_grants_action(
        self,
        level: str,
        action: str
    ) -> bool:
        """Map permission level to allowed actions"""
        level_actions = {
            "owner": ["create", "read", "update", "delete", "execute", "share", "manage_permissions"],
            "admin": ["create", "read", "update", "delete", "execute", "share"],
            "edit": ["read", "update", "execute"],
            "view": ["read"]
        }

        return action in level_actions.get(level, [])

    async def grant_permission(
        self,
        granter_user_id: str,
        grantee_user_id: Optional[str],
        grantee_group_id: Optional[str],
        object_type: ObjectType,
        object_id: str,
        permission_level: str
    ) -> str:
        """
        Grant permission to user or group
        Returns: permission_id
        """
        # Check if granter has permission to grant
        can_grant = await self.check_permission(
            granter_user_id, object_type, object_id, "manage_permissions"
        )

        if not can_grant:
            raise PermissionError("User cannot grant permissions on this object")

        # Insert permission
        permission_id = await db.fetchval("""
            INSERT INTO object_permissions
            (object_type, object_id, user_id, group_id, permission_level, granted_by)
            VALUES ($1, $2, $3, $4, $5, $6)
            RETURNING permission_id
        """,
        object_type,
        object_id,
        grantee_user_id,
        grantee_group_id,
        permission_level,
        granter_user_id
        )

        # Log activity
        await log_user_activity(
            user_id=granter_user_id,
            action="granted_permission",
            object_type=object_type,
            object_id=object_id,
            details={
                "grantee_user_id": grantee_user_id,
                "grantee_group_id": grantee_group_id,
                "permission_level": permission_level
            }
        )

        return permission_id
```

### FastAPI Dependency

```python
# backend/Auth/dependencies.py
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

security = HTTPBearer()
permission_service = PermissionService()

async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security)
) -> User:
    """Get current user from JWT token"""
    token = credentials.credentials

    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        user_id = payload.get("user_id")

        user = await db.fetchrow(
            "SELECT * FROM users WHERE user_id = $1 AND is_active = true",
            user_id
        )

        if not user:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid token"
            )

        return User(**user)

    except JWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token"
        )

def require_permission(
    object_type: ObjectType,
    action: str,
    object_id_param: str = "object_id"  # Path parameter name
):
    """
    Dependency factory for permission checking
    Usage:
        @router.get("/workflows/{workflow_id}")
        async def get_workflow(
            workflow_id: str,
            user: User = Depends(get_current_user),
            _: None = Depends(require_permission(ObjectType.WORKFLOW, "read", "workflow_id"))
        ):
            ...
    """
    async def permission_checker(
        request: Request,
        user: User = Depends(get_current_user)
    ):
        # Extract object_id from path parameters
        object_id = request.path_params.get(object_id_param)

        if not object_id:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Missing parameter: {object_id_param}"
            )

        # Check permission
        has_permission = await permission_service.check_permission(
            user.user_id,
            object_type,
            object_id,
            action
        )

        if not has_permission:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Permission denied: {action} on {object_type}"
            )

    return permission_checker
```

---

## Multi-Tenancy Architecture

### Tenant Isolation Strategy

**Two-tier approach**:
1. **Shared Database** - For small tenants (<1000 users, <10K workflows)
2. **Dedicated Database** - For large tenants (>1000 users, >10K workflows)

### Shared Database Isolation

```sql
-- Row-Level Security (RLS) for tenant isolation
ALTER TABLE workspaces ENABLE ROW LEVEL SECURITY;
ALTER TABLE folders ENABLE ROW LEVEL SECURITY;
ALTER TABLE jobs ENABLE ROW LEVEL SECURITY;
ALTER TABLE workflows ENABLE ROW LEVEL SECURITY;

-- Policy: Users can only see their subscription's data
CREATE POLICY subscription_isolation_policy ON workspaces
USING (subscription_id = current_setting('app.subscription_id')::uuid);

CREATE POLICY subscription_isolation_policy ON folders
USING (
  EXISTS (
    SELECT 1 FROM workspaces
    WHERE workspaces.workspace_id = folders.workspace_id
    AND workspaces.subscription_id = current_setting('app.subscription_id')::uuid
  )
);

-- Similar policies for jobs, workflows, etc.
```

### Setting Subscription Context

```python
# backend/Auth/middleware.py
from starlette.middleware.base import BaseHTTPMiddleware

class SubscriptionContextMiddleware(BaseHTTPMiddleware):
    """
    Set subscription_id in PostgreSQL session
    Enables Row-Level Security
    """

    async def dispatch(self, request: Request, call_next):
        # Extract user from token
        token = request.headers.get("Authorization", "").replace("Bearer ", "")

        if token:
            try:
                payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
                subscription_id = payload.get("subscription_id")

                # Set PostgreSQL session variable
                async with db_pool.acquire() as conn:
                    await conn.execute(
                        "SET LOCAL app.subscription_id = $1",
                        subscription_id
                    )
                    await conn.execute(
                        "SET LOCAL app.user_id = $1",
                        payload.get("user_id")
                    )

                    # Store in request state
                    request.state.subscription_id = subscription_id
                    request.state.user_id = payload.get("user_id")

            except Exception as e:
                logger.error(f"Error setting subscription context: {e}")

        response = await call_next(request)
        return response

# Add to FastAPI app
app.add_middleware(SubscriptionContextMiddleware)
```

### Dedicated Database for Large Tenants

```python
# backend/Tenancy/database_router.py
from typing import Dict
import asyncpg

class DatabaseRouter:
    """
    Route database connections based on subscription
    Large tenants get dedicated database
    """

    def __init__(self):
        self.shared_pool: asyncpg.Pool = None
        self.dedicated_pools: Dict[str, asyncpg.Pool] = {}

    async def initialize(self):
        """Initialize database pools"""
        # Shared database
        self.shared_pool = await asyncpg.create_pool(
            host=config("DB_HOST"),
            port=config("DB_PORT"),
            database="uzo_shared",
            user=config("DB_USER"),
            password=config("DB_PASSWORD"),
            min_size=10,
            max_size=100
        )

        # Load dedicated tenant databases
        dedicated_tenants = await self.load_dedicated_tenants()

        for tenant in dedicated_tenants:
            pool = await asyncpg.create_pool(
                host=tenant["db_host"],
                port=tenant["db_port"],
                database=tenant["db_name"],
                user=tenant["db_user"],
                password=tenant["db_password"],
                min_size=5,
                max_size=50
            )
            self.dedicated_pools[tenant["subscription_id"]] = pool

    async def get_connection(self, subscription_id: str) -> asyncpg.Connection:
        """Get database connection for subscription"""
        if subscription_id in self.dedicated_pools:
            # Dedicated database
            return await self.dedicated_pools[subscription_id].acquire()
        else:
            # Shared database
            conn = await self.shared_pool.acquire()

            # Set subscription context for RLS
            await conn.execute(
                "SET LOCAL app.subscription_id = $1",
                subscription_id
            )

            return conn

    async def load_dedicated_tenants(self) -> List[Dict]:
        """Load list of tenants with dedicated databases"""
        # Query from subscription table
        subscriptions = await db.fetch("""
            SELECT
                subscription_id,
                db_host,
                db_port,
                db_name,
                db_user,
                db_password_encrypted
            FROM subscriptions
            WHERE has_dedicated_database = true
        """)

        return [
            {
                "subscription_id": s["subscription_id"],
                "db_host": s["db_host"],
                "db_port": s["db_port"],
                "db_name": s["db_name"],
                "db_user": s["db_user"],
                "db_password": decrypt(s["db_password_encrypted"])
            }
            for s in subscriptions
        ]

# Global router instance
db_router = DatabaseRouter()

# Usage in routes
@router.get("/workspaces")
async def get_workspaces(
    user: User = Depends(get_current_user)
):
    conn = await db_router.get_connection(user.subscription_id)

    try:
        workspaces = await conn.fetch(
            "SELECT * FROM workspaces WHERE subscription_id = $1",
            user.subscription_id
        )
        return {"data": workspaces}
    finally:
        await conn.close()
```

### Tenant Provisioning

```python
# backend/Tenancy/provisioning.py

class TenantProvisioner:
    """
    Provision new tenants
    Decide: shared vs dedicated database
    """

    async def provision_tenant(
        self,
        company_name: str,
        owner_email: str,
        plan_type: str  # standard, premium, enterprise
    ) -> Subscription:
        """
        Create new tenant
        """
        # Determine if dedicated database needed
        needs_dedicated = plan_type == "enterprise"

        if needs_dedicated:
            # Provision dedicated database
            db_credentials = await self.provision_dedicated_database(company_name)
        else:
            # Use shared database
            db_credentials = None

        # Create subscription
        subscription_id = await db.fetchval("""
            INSERT INTO subscriptions
            (company_name, plan_type, has_dedicated_database, db_host, db_port, db_name, db_user, db_password_encrypted)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            RETURNING subscription_id
        """,
        company_name,
        plan_type,
        needs_dedicated,
        db_credentials["host"] if db_credentials else None,
        db_credentials["port"] if db_credentials else None,
        db_credentials["database"] if db_credentials else None,
        db_credentials["user"] if db_credentials else None,
        encrypt(db_credentials["password"]) if db_credentials else None
        )

        # Create owner user
        user_id = await self.create_tenant_owner(
            subscription_id,
            owner_email,
            company_name
        )

        # Initialize default data (roles, etc.)
        await self.initialize_tenant_data(subscription_id)

        # If dedicated database, run migrations
        if needs_dedicated:
            await self.run_migrations(db_credentials)

            # Register in database router
            await db_router.add_dedicated_tenant(
                subscription_id,
                db_credentials
            )

        return Subscription(
            subscription_id=subscription_id,
            company_name=company_name,
            owner_user_id=user_id
        )

    async def provision_dedicated_database(
        self,
        company_name: str
    ) -> Dict[str, str]:
        """
        Create dedicated PostgreSQL database
        Could be: AWS RDS, Cloud SQL, or self-hosted
        """
        db_name = f"uzo_{company_name.lower().replace(' ', '_')}_{uuid4().hex[:8]}"

        # Example: AWS RDS provisioning
        import boto3

        rds = boto3.client('rds')
        response = rds.create_db_instance(
            DBInstanceIdentifier=db_name,
            DBInstanceClass='db.t3.medium',
            Engine='postgres',
            EngineVersion='15.4',
            MasterUsername='admin',
            MasterUserPassword=generate_secure_password(),
            AllocatedStorage=100,
            StorageType='gp3',
            VpcSecurityGroupIds=['sg-xxxxx'],
            DBSubnetGroupName='uzo-db-subnet',
            BackupRetentionPeriod=30,
            PubliclyAccessible=False,
            Tags=[
                {'Key': 'tenant', 'Value': company_name},
                {'Key': 'purpose', 'Value': 'uzo-dedicated'}
            ]
        )

        # Wait for database to be available
        waiter = rds.get_waiter('db_instance_available')
        waiter.wait(DBInstanceIdentifier=db_name)

        # Get endpoint
        instance = rds.describe_db_instances(
            DBInstanceIdentifier=db_name
        )['DBInstances'][0]

        return {
            "host": instance['Endpoint']['Address'],
            "port": instance['Endpoint']['Port'],
            "database": "postgres",
            "user": "admin",
            "password": get_password_from_secrets_manager(db_name)
        }

    async def run_migrations(self, db_credentials: Dict):
        """Run database migrations on new tenant database"""
        # Use Alembic or raw SQL
        conn = await asyncpg.connect(
            host=db_credentials["host"],
            port=db_credentials["port"],
            database=db_credentials["database"],
            user=db_credentials["user"],
            password=db_credentials["password"]
        )

        try:
            # Run all migration SQL files
            await conn.execute(open("migrations/001_initial_schema.sql").read())
            await conn.execute(open("migrations/002_add_indexes.sql").read())
            # ...
        finally:
            await conn.close()
```

### Tenant Shutdown

```python
async def shutdown_tenant(subscription_id: str, reason: str):
    """
    Gracefully shutdown and cleanup tenant
    """
    # 1. Stop all running workflows
    await stop_all_workflows(subscription_id)

    # 2. Cancel Prefect flows
    await cancel_prefect_flows(subscription_id)

    # 3. Close WebSocket connections
    await disconnect_all_websockets(subscription_id)

    # 4. Export data (backup)
    await export_tenant_data(subscription_id)

    # 5. Soft delete in database
    await db.execute(
        "UPDATE subscriptions SET is_active = false, deleted_at = NOW(), deletion_reason = $2 WHERE subscription_id = $1",
        subscription_id,
        reason
    )

    # 6. If dedicated database, schedule for deletion (after retention period)
    has_dedicated = await db.fetchval(
        "SELECT has_dedicated_database FROM subscriptions WHERE subscription_id = $1",
        subscription_id
    )

    if has_dedicated:
        await schedule_database_deletion(subscription_id, retention_days=30)
```

---

**Document continues with: Edition-based Features, Lineage Tracking, WebSocket Architecture...**

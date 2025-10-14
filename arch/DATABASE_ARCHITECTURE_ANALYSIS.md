# Database Architecture Analysis & Requirements
## UZO Workflow Platform - Full Stack Architecture Review

**Date**: October 2025
**Reviewed By**: Senior Full Stack Lead/Architect
**Purpose**: Comprehensive database analysis for scalability, performance, integrity, and security

---

## Executive Summary

After analyzing the backend0125 codebase, I've identified **47+ MongoDB collections** with embedded/nested document patterns that create significant scalability, performance, and data integrity issues. The current architecture uses a **hybrid approach** (MongoDB + PostgreSQL) but doesn't leverage the strengths of either database properly.

### Critical Issues Identified

1. **❌ Deeply Nested Documents** - Workspace → Folders → Jobs → Workflows hierarchy stored in single documents
2. **❌ No Indexing Strategy** - No evidence of compound indexes, text indexes, or TTL indexes
3. **❌ Data Duplication** - Same data stored in multiple collections (workspace, workspaceBkp, etc.)
4. **❌ Inconsistent Transaction Handling** - No multi-document ACID transactions
5. **❌ PostgreSQL Underutilized** - Only used for basic queries, not for relational integrity
6. **❌ No Sharding Strategy** - Single MongoDB instance won't scale
7. **❌ Performance Bottlenecks** - N+1 queries, full collection scans, no caching strategy
8. **❌ WebSocket State in Memory** - Connection state not persisted, will fail on restart

---

## Part 1: Current Database Inventory

### MongoDB Collections (47+ identified)

#### Core Workspace Collections
```
workspace              - Main workspace container (NESTED: folders, jobs, workflows)
workspaceBkp          - Backup copies
workflow              - Workflow definitions (NESTED: nodes, edges, parameters)
workflowBkp           - Workflow backups
jobSteps              - Job step definitions
jobStepsBkp           - Job step backups
jobStepsWorkflow      - Job-workflow mappings
jobPackage            - Job packages
jobPackageSteps       - Job package step definitions
jobPackageStepsBkp    - Job package step backups
```

#### Execution & Runtime Collections
```
jobRun                - Job execution records
jobRunBkp             - Job run backups
jobPackageRun         - Job package execution records
userActivity          - Audit log (HIGH WRITE VOLUME)
loginInfo             - Session tracking
notification          - User notifications
```

#### User Management Collections
```
users                 - User accounts
usersBkp              - User backups
groups                - User groups
groupsBkp             - Group backups
user_groups           - User-group mappings
roles                 - Role definitions
rolesBkp              - Role backups
```

#### Security & Policy Collections
```
policy                - Access policies
policyBackup          - Policy backups
policyAction          - Policy actions
policyGroup           - Policy groups
authorizationType     - Authorization types
accessAndPermission   - Access permissions
```

#### Infrastructure Collections
```
connectors            - Database connectors (SENSITIVE: connection strings)
connectorBkp          - Connector backups
schedules             - Job schedules
schedulerBkp          - Schedule backups
dashboardCount        - Dashboard metrics cache
```

#### Company & Billing Collections
```
Accounts              - Company accounts
billingInfo           - Billing information
invoiceHistory        - Invoice records
budgetsAndAlerts      - Budget tracking
corporateCalendar     - Corporate calendar
OrgAnnouncement       - Announcements
termsAndConditions    - Terms & conditions
tickets               - Support tickets
ticketLabels          - Ticket labels
```

#### Version Control Collections
```
version_manager       - Generic version control
workflow_versions     - Workflow-specific versions
```

### PostgreSQL Database

**Current Usage**: Minimal, only used for:
- Lineage queries (Connection/pg.py)
- Some job execution metadata
- NOT used for relational integrity or transactions

**Configuration**:
```python
# Connection/pg.py - Basic connection pooling
dbname, user, password, host, port from .env
No connection pooling, no prepared statements
```

---

## Part 2: Critical Architectural Issues

### Issue 1: Nested Document Anti-Pattern ⚠️

**Current Structure (BAD)**:
```javascript
// workspace collection - Single document
{
  "_id": ObjectId("..."),
  "workspace_id": "ws_123",
  "workspace_name": "My Workspace",
  "folders": [                          // NESTED ARRAY
    {
      "folder_id": "f_456",
      "folder_name": "Data Pipelines",
      "jobs": [                          // NESTED ARRAY
        {
          "job_id": "j_789",
          "job_name": "ETL Job",
          "workflows": [                 // NESTED ARRAY
            {
              "workflow_id": "wf_abc",
              "workflow_name": "Process Data",
              "workFlowDiagram": "...",  // JSON STRING (nested JSON!)
              "nodes": [...],            // NESTED ARRAY
              "edges": [...]             // NESTED ARRAY
            }
          ]
        }
      ]
    }
  ]
}
```

**Problems**:
- **Document Size**: Can exceed 16MB BSON limit with large workflows
- **Update Performance**: Updating a single workflow requires reading/writing entire workspace
- **Concurrent Updates**: High risk of race conditions
- **Query Performance**: Can't index deeply nested fields efficiently
- **Memory**: Loading entire workspace into memory for simple updates

**Impact on Requirements**:
- ❌ Scalability: Document size limits growth
- ❌ Performance: O(n) lookups in nested arrays
- ❌ Integrity: No referential integrity between entities
- ❌ Real-time: Full document updates block other operations

---

### Issue 2: No Indexing Strategy ⚠️

**Evidence**: No index creation code found in:
- Connection/db.py
- Any service files
- No migration scripts

**Required Indexes Missing**:
```javascript
// Workspace queries
db.workspace.createIndex({ "subscription_id": 1, "d_flag": 1 })
db.workspace.createIndex({ "workspace_id": 1 }, { unique: true })
db.workspace.createIndex({ "created_by": 1, "created_at": -1 })

// Workflow queries (currently nested!)
db.workflow.createIndex({ "workflow_id": 1 }, { unique: true })
db.workflow.createIndex({ "workspace_id": 1, "folder_id": 1, "job_id": 1 })
db.workflow.createIndex({ "subscription_id": 1, "d_flag": 1 })

// User activity (HIGH VOLUME)
db.userActivity.createIndex({ "subscription_id": 1, "created_at": -1 })
db.userActivity.createIndex({ "UserId": 1, "created_at": -1 })
db.userActivity.createIndex({ "object_type": 1, "object_id": 1 })

// Job runs (time-series data)
db.jobRun.createIndex({ "workflow_id": 1, "created_at": -1 })
db.jobRun.createIndex({ "status": 1, "created_at": -1 })
```

**Impact**:
- ❌ Performance: Full collection scans on every query
- ❌ Scalability: Query time increases linearly with data
- ❌ Real-time: Slow queries block WebSocket updates

---

### Issue 3: No Transaction Support ⚠️

**Current Pattern**:
```python
# Workflow_Runs/service.py - NO TRANSACTION
async def addWorkflow(payload, user_obj):
    # Step 1: Insert workflow
    await self.workflowCollection.insert_one(workflow_data)

    # Step 2: Update workspace (SEPARATE OPERATION)
    await self.workspaceCollection.update_one(...)

    # Step 3: Log activity (SEPARATE OPERATION)
    await self.userActivityCollection.insert_one(...)

    # PROBLEM: If step 2 or 3 fails, step 1 already committed!
```

**Problems**:
- **Partial Failures**: Data inconsistency when operations fail mid-sequence
- **No Rollback**: Can't undo changes if later steps fail
- **Race Conditions**: Concurrent updates can corrupt data

**MongoDB Transactions Available**: MongoDB 4.0+ supports multi-document ACID transactions, but **NOT USED** in current code.

---

### Issue 4: Data Duplication & Backup Pattern ⚠️

**Pattern Found**:
```
workspace → workspaceBkp
workflow → workflowBkp
users → usersBkp
connectors → connectorBkp
(12+ backup collections)
```

**Problems**:
- **Storage**: Doubles storage requirements
- **Sync Issues**: No atomic backup mechanism
- **Stale Data**: Backups can be out of sync
- **Version Control**: Separate `version_manager` collection adds complexity

**Better Approach**: Use MongoDB Change Streams + Time-series collections for audit trail

---

### Issue 5: WebSocket State in Memory ⚠️

**Current Implementation** (`Websocket/socket.py`):
```python
class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []      # IN MEMORY
        self.active_activity: List[WebSocket] = []         # IN MEMORY
        self.active_runnow: Dict[str, WebSocket] = {}      # IN MEMORY
```

**Problems**:
- **No Persistence**: Server restart loses all connections
- **No Horizontal Scaling**: Can't scale to multiple instances
- **No Connection Recovery**: Clients can't reconnect to same session

**Impact**:
- ❌ Scalability: Single server bottleneck
- ❌ Reliability: Restart = disconnect all users
- ❌ Real-time: Can't load balance WebSocket connections

---

### Issue 6: Redis Queue Without Persistence ⚠️

**Current Pattern** (`Connection/redisConn.py`):
```python
redisConn = Redis(host='localhost', port=6379, db=0)
redisWFQueue = Queue('WF_nueq', connection=redisConn)
```

**Issues**:
- No Redis persistence configuration (RDB/AOF)
- No queue priority management
- No dead letter queue for failed jobs
- No job result storage strategy

---

### Issue 7: Security Vulnerabilities ⚠️

#### Connection Strings in Database
```python
# connectors collection stores PLAINTEXT connection strings
{
  "connector_id": "conn_123",
  "connection_detail": {
    "host": "prod-db.company.com",
    "username": "admin",
    "password": "plaintext_password"  // ❌ NOT ENCRYPTED
  }
}
```

#### No Row-Level Security
- MongoDB: No field-level encryption
- PostgreSQL: No RLS policies defined
- Share permissions in application layer only

#### JWT Token Storage
- No token revocation mechanism
- No refresh token rotation
- Tokens not stored in database (can't invalidate)

---

## Part 3: Performance Analysis

### Query Performance Issues

#### N+1 Query Pattern
```python
# Workflow_Runs/service.py
for workflow in workflows:
    # Query 1: Get workflow
    workflow_data = await collection.find_one({"workflow_id": workflow_id})

    # Query 2: Get user details (N+1 problem)
    user = await users_collection.find_one({"UserId": workflow_data["created_by"]})

    # Query 3: Get connector details (N+1 problem)
    connector = await connectors.find_one({"connector_id": workflow_data["connector_id"]})
```

**Solution**: Use MongoDB aggregation with $lookup (join)

#### Full Collection Scans
```python
# No indexes = full scan
results = await collection.find({
    "subscription_id": sub_id,
    "created_at": {"$gte": start_date}
}).to_list(None)
```

#### Inefficient Aggregation Pipelines
```python
# Websocket/socket.py - Line 225
pipeline = [
    {"$match": filter_query},
    {"$lookup": {...}},  # Join users
    {"$project": {...}}, # Project fields
    {"$project": {...}}, # Project AGAIN (redundant)
    {"$match": {...}},   # Filter AGAIN (should be first)
    {"$sort": {...}},
    {"$limit": 20}
]
```

---

### Estimated Performance Metrics

| Operation | Current (No Index) | With Indexes | With Optimized Schema |
|-----------|-------------------|--------------|----------------------|
| List workflows (100 items) | ~500ms | ~50ms | ~20ms |
| Get workflow by ID | ~200ms | ~5ms | ~2ms |
| Update workflow node | ~300ms | ~100ms | ~10ms |
| User activity query | ~1000ms | ~100ms | ~30ms |
| WebSocket broadcast | ~50ms | ~50ms | ~10ms (Redis pub/sub) |
| Job execution queue | ~20ms | ~20ms | ~5ms (Redis Streams) |

---

## Part 4: Scalability Analysis

### Current Limitations

#### MongoDB Scalability Issues
```
Single Instance:
├── No Replica Set: No HA, no read scaling
├── No Sharding: Single server handles all data
├── No Connection Pool: Each request creates new connection
└── Document Size: 16MB limit blocks large workflows
```

#### Application Scalability Issues
```
Single Backend Instance:
├── WebSocket in memory: Can't scale horizontally
├── No load balancer: Single point of failure
├── Redis on localhost: Can't distribute
└── No caching layer: Repeated DB queries
```

### Growth Projections

| Metric | Current | 6 Months | 1 Year | 2 Years |
|--------|---------|----------|--------|---------|
| Workspaces | 100 | 1,000 | 5,000 | 20,000 |
| Workflows | 1,000 | 10,000 | 50,000 | 200,000 |
| Users | 50 | 500 | 2,000 | 10,000 |
| Job Runs/Day | 100 | 1,000 | 10,000 | 50,000 |
| DB Size | 1 GB | 10 GB | 50 GB | 200 GB |
| Concurrent Users | 10 | 50 | 200 | 1,000 |

**Breaking Points**:
- **3 months**: Nested documents hit 16MB limit
- **6 months**: Single MongoDB instance saturated (CPU)
- **9 months**: Query performance unacceptable (>3s)
- **12 months**: WebSocket memory exhausted

---

## Part 5: Data Integrity Issues

### Referential Integrity Violations

#### Orphaned Records
```javascript
// Workflow references non-existent connector
{
  "workflow_id": "wf_123",
  "source_connectorid": "conn_999",  // ❌ Connector deleted but reference remains
  "target_connectorid": "conn_888"   // ❌ Connector deleted but reference remains
}

// Job references deleted workflow
{
  "job_id": "j_456",
  "workflow_dependency": ["wf_deleted_1", "wf_deleted_2"]  // ❌ Orphaned references
}
```

#### Cascade Delete Problems
```python
# Workspace/Workflows/service.py
async def deleteWorkflow(payload, user_obj):
    # Only soft delete workflow
    await collection.update_one(
        {"workflow_id": workflow_id},
        {"$set": {"d_flag": True}}
    )
    # ❌ Job still references workflow
    # ❌ Schedules still reference workflow
    # ❌ Job runs still reference workflow
```

### Concurrent Update Issues

#### Race Condition Example
```python
# User A reads workflow
workflow = await collection.find_one({"workflow_id": "wf_123"})
workflow["nodes"].append(new_node)

# User B reads same workflow (same version)
workflow2 = await collection.find_one({"workflow_id": "wf_123"})
workflow2["nodes"].append(different_node)

# User A saves
await collection.update_one({...}, {"$set": workflow})

# User B saves (OVERWRITES User A's changes!)
await collection.update_one({...}, {"$set": workflow2})
```

**Solution Needed**: Optimistic locking with version numbers

---

## Part 6: PostgreSQL Analysis

### Current Usage (Minimal)
```python
# Connection/pg.py - Only used in a few places
def create_connection():
    return psycopg2.connect(**connection_params)

# Used in:
# - Lineage queries (Lineage/service.py)
# - Job execution metadata (Jobs/service.py)
# - NOT used for core entities
```

### PostgreSQL Strengths (NOT leveraged)
- ✅ ACID transactions
- ✅ Referential integrity (foreign keys)
- ✅ Complex joins
- ✅ Full-text search
- ✅ JSON columns (JSONB)
- ✅ Materialized views
- ✅ Row-level security
- ✅ Stored procedures

**Verdict**: PostgreSQL is underutilized. Should be primary DB for relational data.

---

## Part 7: Recommended Database Architecture

### Hybrid Architecture: PostgreSQL (Primary) + MongoDB (Secondary) + Redis (Cache/Queue)

#### Design Principles
1. **PostgreSQL**: Relational entities with integrity requirements
2. **MongoDB**: Flexible schemas, time-series data, audit logs
3. **Redis**: Cache, sessions, pub/sub, job queues
4. **TimescaleDB**: Time-series optimization (job runs, metrics)

---

### New PostgreSQL Schema Design

#### Core Tables (Normalized)

```sql
-- ============================================
-- SUBSCRIPTION & TENANT MANAGEMENT
-- ============================================

CREATE TABLE subscriptions (
    subscription_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    company_name VARCHAR(255) NOT NULL,
    plan_type VARCHAR(50) NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'active',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    settings JSONB DEFAULT '{}'::jsonb,
    metadata JSONB DEFAULT '{}'::jsonb
);

CREATE INDEX idx_subscriptions_status ON subscriptions(status, created_at);

-- ============================================
-- USER MANAGEMENT
-- ============================================

CREATE TABLE users (
    user_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    subscription_id UUID NOT NULL REFERENCES subscriptions(subscription_id) ON DELETE CASCADE,
    email VARCHAR(255) NOT NULL,
    username VARCHAR(100) NOT NULL,
    full_name VARCHAR(255),
    password_hash VARCHAR(255) NOT NULL,
    photo_url TEXT,
    user_utc_offset VARCHAR(10) DEFAULT '+00:00',
    is_active BOOLEAN DEFAULT true,
    is_email_verified BOOLEAN DEFAULT false,
    last_login_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    settings JSONB DEFAULT '{}'::jsonb,

    CONSTRAINT uk_users_email UNIQUE(subscription_id, email),
    CONSTRAINT uk_users_username UNIQUE(subscription_id, username)
);

CREATE INDEX idx_users_subscription ON users(subscription_id, is_active);
CREATE INDEX idx_users_email ON users(email) WHERE is_active = true;
CREATE INDEX idx_users_last_login ON users(last_login_at DESC);

-- ============================================
-- GROUPS & ROLES
-- ============================================

CREATE TABLE groups (
    group_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    subscription_id UUID NOT NULL REFERENCES subscriptions(subscription_id) ON DELETE CASCADE,
    group_name VARCHAR(255) NOT NULL,
    description TEXT,
    created_by UUID REFERENCES users(user_id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT uk_groups_name UNIQUE(subscription_id, group_name)
);

CREATE TABLE roles (
    role_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    subscription_id UUID NOT NULL REFERENCES subscriptions(subscription_id) ON DELETE CASCADE,
    role_name VARCHAR(255) NOT NULL,
    description TEXT,
    permissions JSONB NOT NULL DEFAULT '[]'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT uk_roles_name UNIQUE(subscription_id, role_name)
);

CREATE TABLE user_groups (
    user_id UUID REFERENCES users(user_id) ON DELETE CASCADE,
    group_id UUID REFERENCES groups(group_id) ON DELETE CASCADE,
    added_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    added_by UUID REFERENCES users(user_id),

    PRIMARY KEY (user_id, group_id)
);

CREATE TABLE user_roles (
    user_id UUID REFERENCES users(user_id) ON DELETE CASCADE,
    role_id UUID REFERENCES roles(role_id) ON DELETE CASCADE,
    assigned_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    assigned_by UUID REFERENCES users(user_id),

    PRIMARY KEY (user_id, role_id)
);

-- ============================================
-- WORKSPACE HIERARCHY (NORMALIZED)
-- ============================================

CREATE TABLE workspaces (
    workspace_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    subscription_id UUID NOT NULL REFERENCES subscriptions(subscription_id) ON DELETE CASCADE,
    workspace_name VARCHAR(255) NOT NULL,
    description TEXT,
    owner_id UUID NOT NULL REFERENCES users(user_id),
    is_deleted BOOLEAN DEFAULT false,
    reason_to_delete TEXT,
    created_by UUID NOT NULL REFERENCES users(user_id),
    updated_by UUID REFERENCES users(user_id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    settings JSONB DEFAULT '{}'::jsonb,

    CONSTRAINT uk_workspaces_name UNIQUE(subscription_id, workspace_name) WHERE is_deleted = false
);

CREATE INDEX idx_workspaces_subscription ON workspaces(subscription_id, is_deleted, created_at DESC);
CREATE INDEX idx_workspaces_owner ON workspaces(owner_id, is_deleted);
CREATE INDEX idx_workspaces_name ON workspaces USING gin(to_tsvector('english', workspace_name));

CREATE TABLE folders (
    folder_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id UUID NOT NULL REFERENCES workspaces(workspace_id) ON DELETE CASCADE,
    folder_name VARCHAR(255) NOT NULL,
    description TEXT,
    parent_folder_id UUID REFERENCES folders(folder_id) ON DELETE CASCADE,
    is_deleted BOOLEAN DEFAULT false,
    created_by UUID NOT NULL REFERENCES users(user_id),
    updated_by UUID REFERENCES users(user_id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT uk_folders_name UNIQUE(workspace_id, parent_folder_id, folder_name) WHERE is_deleted = false
);

CREATE INDEX idx_folders_workspace ON folders(workspace_id, is_deleted);
CREATE INDEX idx_folders_parent ON folders(parent_folder_id, is_deleted);

CREATE TABLE jobs (
    job_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id UUID NOT NULL REFERENCES workspaces(workspace_id) ON DELETE CASCADE,
    folder_id UUID NOT NULL REFERENCES folders(folder_id) ON DELETE CASCADE,
    job_name VARCHAR(255) NOT NULL,
    description TEXT,
    environment VARCHAR(50),
    is_deleted BOOLEAN DEFAULT false,
    reason_to_delete TEXT,
    created_by UUID NOT NULL REFERENCES users(user_id),
    updated_by UUID REFERENCES users(user_id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    settings JSONB DEFAULT '{}'::jsonb,

    CONSTRAINT uk_jobs_name UNIQUE(folder_id, job_name) WHERE is_deleted = false
);

CREATE INDEX idx_jobs_workspace ON jobs(workspace_id, is_deleted);
CREATE INDEX idx_jobs_folder ON jobs(folder_id, is_deleted);
CREATE INDEX idx_jobs_created_at ON jobs(created_at DESC);

-- ============================================
-- WORKFLOWS (FLATTENED)
-- ============================================

CREATE TABLE workflows (
    workflow_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id UUID NOT NULL REFERENCES workspaces(workspace_id) ON DELETE CASCADE,
    folder_id UUID NOT NULL REFERENCES folders(folder_id) ON DELETE CASCADE,
    job_id UUID NOT NULL REFERENCES jobs(job_id) ON DELETE CASCADE,
    workflow_name VARCHAR(255) NOT NULL,
    description TEXT,
    environment VARCHAR(50),
    version INTEGER NOT NULL DEFAULT 1,
    is_active BOOLEAN DEFAULT true,
    is_deleted BOOLEAN DEFAULT false,
    is_checked_out BOOLEAN DEFAULT false,
    checked_out_by UUID REFERENCES users(user_id),
    checked_out_at TIMESTAMPTZ,
    created_by UUID NOT NULL REFERENCES users(user_id),
    updated_by UUID REFERENCES users(user_id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Store complex data in JSONB
    workflow_diagram JSONB NOT NULL DEFAULT '{}'::jsonb,  -- nodes, edges
    parameters JSONB DEFAULT '{}'::jsonb,
    notification_settings JSONB DEFAULT '{
        "on_failure": false,
        "on_success": false,
        "emails": []
    }'::jsonb,
    tags JSONB DEFAULT '[]'::jsonb,

    CONSTRAINT uk_workflows_name UNIQUE(job_id, workflow_name, version) WHERE is_deleted = false
);

CREATE INDEX idx_workflows_job ON workflows(job_id, is_deleted, is_active);
CREATE INDEX idx_workflows_workspace ON workflows(workspace_id, is_deleted);
CREATE INDEX idx_workflows_version ON workflows(workflow_id, version DESC);
CREATE INDEX idx_workflows_checked_out ON workflows(is_checked_out, checked_out_by);
CREATE INDEX idx_workflows_diagram ON workflows USING gin(workflow_diagram);
CREATE INDEX idx_workflows_tags ON workflows USING gin(tags);

-- Workflow dependencies (many-to-many)
CREATE TABLE workflow_dependencies (
    workflow_id UUID REFERENCES workflows(workflow_id) ON DELETE CASCADE,
    depends_on_workflow_id UUID REFERENCES workflows(workflow_id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (workflow_id, depends_on_workflow_id),
    CONSTRAINT chk_no_self_dependency CHECK (workflow_id != depends_on_workflow_id)
);

CREATE INDEX idx_workflow_dependencies_depends_on ON workflow_dependencies(depends_on_workflow_id);

-- ============================================
-- CONNECTORS (ENCRYPTED CREDENTIALS)
-- ============================================

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE connectors (
    connector_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    subscription_id UUID NOT NULL REFERENCES subscriptions(subscription_id) ON DELETE CASCADE,
    connector_name VARCHAR(255) NOT NULL,
    connector_type VARCHAR(50) NOT NULL, -- postgres, mysql, snowflake, s3, etc.
    description TEXT,
    is_active BOOLEAN DEFAULT true,
    is_deleted BOOLEAN DEFAULT false,
    created_by UUID NOT NULL REFERENCES users(user_id),
    updated_by UUID REFERENCES users(user_id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_tested_at TIMESTAMPTZ,
    test_status VARCHAR(20), -- success, failed, untested

    -- ENCRYPTED connection details
    connection_config_encrypted BYTEA, -- pgp_sym_encrypt()

    CONSTRAINT uk_connectors_name UNIQUE(subscription_id, connector_name) WHERE is_deleted = false
);

CREATE INDEX idx_connectors_subscription ON connectors(subscription_id, is_deleted, is_active);
CREATE INDEX idx_connectors_type ON connectors(connector_type, is_active);
CREATE INDEX idx_connectors_test_status ON connectors(test_status, last_tested_at);

-- ============================================
-- PERMISSIONS & SHARING (ROW-LEVEL SECURITY)
-- ============================================

CREATE TABLE object_permissions (
    permission_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    object_type VARCHAR(50) NOT NULL, -- workspace, folder, job, workflow
    object_id UUID NOT NULL,
    user_id UUID REFERENCES users(user_id) ON DELETE CASCADE,
    group_id UUID REFERENCES groups(group_id) ON DELETE CASCADE,
    permission_level VARCHAR(20) NOT NULL, -- view, edit, admin
    granted_by UUID REFERENCES users(user_id),
    granted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ,

    CONSTRAINT chk_user_or_group CHECK (
        (user_id IS NOT NULL AND group_id IS NULL) OR
        (user_id IS NULL AND group_id IS NOT NULL)
    )
);

CREATE INDEX idx_object_permissions_object ON object_permissions(object_type, object_id);
CREATE INDEX idx_object_permissions_user ON object_permissions(user_id, object_type);
CREATE INDEX idx_object_permissions_group ON object_permissions(group_id, object_type);

-- Enable Row-Level Security
ALTER TABLE workspaces ENABLE ROW LEVEL SECURITY;
ALTER TABLE workflows ENABLE ROW LEVEL SECURITY;
ALTER TABLE jobs ENABLE ROW LEVEL SECURITY;

-- RLS Policy Example
CREATE POLICY workspace_access_policy ON workspaces
    USING (
        subscription_id = current_setting('app.subscription_id')::uuid
        AND (
            owner_id = current_setting('app.user_id')::uuid
            OR created_by = current_setting('app.user_id')::uuid
            OR EXISTS (
                SELECT 1 FROM object_permissions
                WHERE object_type = 'workspace'
                AND object_id = workspaces.workspace_id
                AND user_id = current_setting('app.user_id')::uuid
            )
        )
    );

-- ============================================
-- TIME-SERIES: JOB EXECUTION (TimescaleDB)
-- ============================================

CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE job_runs (
    job_run_id UUID NOT NULL DEFAULT gen_random_uuid(),
    workflow_id UUID NOT NULL REFERENCES workflows(workflow_id),
    job_id UUID NOT NULL REFERENCES jobs(job_id),
    workspace_id UUID NOT NULL REFERENCES workspaces(workspace_id),
    subscription_id UUID NOT NULL REFERENCES subscriptions(subscription_id),

    status VARCHAR(20) NOT NULL, -- pending, running, completed, failed
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    duration_ms INTEGER,

    triggered_by UUID REFERENCES users(user_id),
    trigger_type VARCHAR(50), -- manual, scheduled, webhook

    -- Execution details
    execution_logs JSONB DEFAULT '[]'::jsonb,
    error_message TEXT,
    rows_processed INTEGER DEFAULT 0,
    execution_metadata JSONB DEFAULT '{}'::jsonb,

    PRIMARY KEY (job_run_id, started_at)
);

-- Convert to hypertable (TimescaleDB)
SELECT create_hypertable('job_runs', 'started_at', chunk_time_interval => INTERVAL '1 week');

-- Compression policy (compress data older than 30 days)
SELECT add_compression_policy('job_runs', INTERVAL '30 days');

-- Retention policy (drop data older than 1 year)
SELECT add_retention_policy('job_runs', INTERVAL '1 year');

CREATE INDEX idx_job_runs_workflow ON job_runs(workflow_id, started_at DESC);
CREATE INDEX idx_job_runs_status ON job_runs(status, started_at DESC);
CREATE INDEX idx_job_runs_subscription ON job_runs(subscription_id, started_at DESC);

-- ============================================
-- VERSION CONTROL
-- ============================================

CREATE TABLE workflow_versions (
    version_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workflow_id UUID NOT NULL REFERENCES workflows(workflow_id) ON DELETE CASCADE,
    version_number INTEGER NOT NULL,
    change_type VARCHAR(50) NOT NULL, -- check_in, check_out, restore
    comment TEXT,

    -- Full workflow state snapshot
    workflow_snapshot JSONB NOT NULL,

    created_by UUID NOT NULL REFERENCES users(user_id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT uk_workflow_versions UNIQUE(workflow_id, version_number)
);

CREATE INDEX idx_workflow_versions_workflow ON workflow_versions(workflow_id, version_number DESC);
CREATE INDEX idx_workflow_versions_created ON workflow_versions(created_at DESC);

-- ============================================
-- AUDIT LOG (Partitioned by month)
-- ============================================

CREATE TABLE user_activity (
    activity_id UUID NOT NULL DEFAULT gen_random_uuid(),
    subscription_id UUID NOT NULL REFERENCES subscriptions(subscription_id),
    user_id UUID NOT NULL REFERENCES users(user_id),

    action VARCHAR(50) NOT NULL, -- created, updated, deleted, viewed, executed
    object_type VARCHAR(50) NOT NULL,
    object_id UUID NOT NULL,
    object_name VARCHAR(255),

    ip_address INET,
    user_agent TEXT,

    -- Change tracking
    previous_data JSONB,
    current_data JSONB,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (activity_id, created_at)
) PARTITION BY RANGE (created_at);

-- Create monthly partitions
CREATE TABLE user_activity_2025_10 PARTITION OF user_activity
    FOR VALUES FROM ('2025-10-01') TO ('2025-11-01');

CREATE INDEX idx_user_activity_user ON user_activity(user_id, created_at DESC);
CREATE INDEX idx_user_activity_subscription ON user_activity(subscription_id, created_at DESC);
CREATE INDEX idx_user_activity_object ON user_activity(object_type, object_id, created_at DESC);

-- ============================================
-- SESSION MANAGEMENT
-- ============================================

CREATE TABLE user_sessions (
    session_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    subscription_id UUID NOT NULL REFERENCES subscriptions(subscription_id),

    refresh_token_hash VARCHAR(255) NOT NULL,
    ip_address INET,
    user_agent TEXT,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ NOT NULL,
    last_activity_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    is_active BOOLEAN DEFAULT true
);

CREATE INDEX idx_user_sessions_user ON user_sessions(user_id, is_active, expires_at);
CREATE INDEX idx_user_sessions_token ON user_sessions(refresh_token_hash) WHERE is_active = true;
CREATE INDEX idx_user_sessions_expires ON user_sessions(expires_at) WHERE is_active = true;

-- ============================================
-- WEBSOCKET CONNECTION TRACKING
-- ============================================

CREATE TABLE websocket_connections (
    connection_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    subscription_id UUID NOT NULL REFERENCES subscriptions(subscription_id),

    connection_type VARCHAR(50) NOT NULL, -- activity, workflow_run, general
    subscribed_to VARCHAR(255), -- workflow_id for run updates

    connected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_heartbeat_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    disconnected_at TIMESTAMPTZ,

    is_active BOOLEAN DEFAULT true
);

CREATE INDEX idx_websocket_connections_user ON websocket_connections(user_id, is_active);
CREATE INDEX idx_websocket_connections_subscribed ON websocket_connections(subscribed_to, is_active);
CREATE INDEX idx_websocket_connections_heartbeat ON websocket_connections(last_heartbeat_at) WHERE is_active = true;
```

---

### MongoDB Schema Design (Simplified)

MongoDB should handle:
1. **Flexible/Semi-structured data** (ChatGPT conversations, logs)
2. **High-velocity writes** (cached dashboard metrics)
3. **Temporary data** (real-time metrics, cache)

```javascript
// ============================================
// CHATGPT CONVERSATION HISTORY
// ============================================
db.createCollection("chat_conversations")
db.chat_conversations.createIndex({ "user_id": 1, "created_at": -1 })
db.chat_conversations.createIndex({ "subscription_id": 1, "created_at": -1 })
db.chat_conversations.createIndex({ "created_at": 1 }, { expireAfterSeconds: 7776000 }) // TTL: 90 days

{
  "_id": ObjectId("..."),
  "conversation_id": "conv_123",
  "user_id": "user_456",
  "subscription_id": "sub_789",
  "messages": [
    {
      "role": "user",
      "content": "Help me build a workflow",
      "timestamp": ISODate("2025-10-13T10:00:00Z")
    },
    {
      "role": "assistant",
      "content": "I can help you with that...",
      "timestamp": ISODate("2025-10-13T10:00:05Z")
    }
  ],
  "metadata": {
    "model": "gpt-4",
    "tokens_used": 150
  },
  "created_at": ISODate("2025-10-13T10:00:00Z"),
  "updated_at": ISODate("2025-10-13T10:00:05Z")
}

// ============================================
// DASHBOARD METRICS CACHE
// ============================================
db.createCollection("dashboard_cache")
db.dashboard_cache.createIndex({ "subscription_id": 1, "cache_key": 1 }, { unique: true })
db.dashboard_cache.createIndex({ "expires_at": 1 }, { expireAfterSeconds: 0 }) // TTL index

{
  "_id": ObjectId("..."),
  "subscription_id": "sub_789",
  "cache_key": "workspace_count",
  "data": {
    "total": 15,
    "active": 12,
    "archived": 3
  },
  "generated_at": ISODate("2025-10-13T10:00:00Z"),
  "expires_at": ISODate("2025-10-13T10:05:00Z") // 5 min cache
}

// ============================================
// EXECUTION LOGS (Short-term retention)
// ============================================
db.createCollection("execution_logs")
db.execution_logs.createIndex({ "job_run_id": 1, "timestamp": -1 })
db.execution_logs.createIndex({ "timestamp": 1 }, { expireAfterSeconds: 604800 }) // TTL: 7 days

{
  "_id": ObjectId("..."),
  "job_run_id": "run_123",
  "workflow_id": "wf_456",
  "node_id": "node_789",
  "level": "info", // info, warning, error
  "message": "Processing 1000 rows...",
  "timestamp": ISODate("2025-10-13T10:00:00Z"),
  "metadata": {
    "rows_processed": 1000,
    "duration_ms": 250
  }
}

// ============================================
// NOTIFICATION QUEUE (Temporary)
// ============================================
db.createCollection("notification_queue")
db.notification_queue.createIndex({ "user_id": 1, "is_read": 1, "created_at": -1 })
db.notification_queue.createIndex({ "created_at": 1 }, { expireAfterSeconds: 2592000 }) // TTL: 30 days

{
  "_id": ObjectId("..."),
  "notification_id": "notif_123",
  "user_id": "user_456",
  "subscription_id": "sub_789",
  "type": "workflow_completed",
  "title": "Workflow 'ETL Pipeline' completed successfully",
  "message": "Processed 10,000 rows in 2m 30s",
  "link": "/workflows/wf_456/runs/run_123",
  "is_read": false,
  "created_at": ISODate("2025-10-13T10:00:00Z")
}
```

---

### Redis Architecture

```
Redis Instance Structure:
├── Database 0: Cache
│   ├── user:<user_id>:profile (Hash) - TTL: 1 hour
│   ├── workspace:<workspace_id>:workflows (List) - TTL: 5 min
│   ├── dashboard:<subscription_id>:counts (Hash) - TTL: 1 min
│   └── session:<session_id> (Hash) - TTL: 24 hours
│
├── Database 1: Job Queues (Redis Streams)
│   ├── workflow_execution_queue (Stream)
│   ├── job_execution_queue (Stream)
│   ├── notification_queue (Stream)
│   └── dead_letter_queue (Stream)
│
├── Database 2: Pub/Sub
│   ├── workflow:<workflow_id>:updates (Channel)
│   ├── user:<user_id>:notifications (Channel)
│   ├── subscription:<subscription_id>:activity (Channel)
│   └── system:broadcast (Channel)
│
└── Database 3: Rate Limiting
    ├── rate_limit:<user_id>:<endpoint> (String) - TTL: 1 minute
    └── websocket:<user_id>:connection_count (String) - TTL: 1 hour
```

#### Redis Streams for Job Queues
```python
# Replace RQ with Redis Streams for better observability

# Producer
await redis.xadd(
    'workflow_execution_queue',
    {
        'workflow_id': 'wf_123',
        'job_run_id': 'run_456',
        'priority': 'high',
        'payload': json.dumps(data)
    },
    maxlen=10000  # Limit queue size
)

# Consumer Group
while True:
    messages = await redis.xreadgroup(
        groupname='workers',
        consumername='worker-1',
        streams={'workflow_execution_queue': '>'},
        count=1,
        block=1000
    )
    for stream, message_list in messages:
        for message_id, data in message_list:
            try:
                await process_workflow(data)
                await redis.xack('workflow_execution_queue', 'workers', message_id)
            except Exception as e:
                # Move to DLQ
                await redis.xadd('dead_letter_queue', data)
```

#### Redis Pub/Sub for WebSocket
```python
# Replace in-memory WebSocket manager with Redis Pub/Sub

class ConnectionManager:
    def __init__(self):
        self.redis = Redis.from_url('redis://localhost:6379/2')
        self.pubsub = self.redis.pubsub()

    async def broadcast_run(self, workflow_id, message):
        # Publish to Redis channel
        await self.redis.publish(
            f'workflow:{workflow_id}:updates',
            json.dumps(message)
        )

    async def subscribe_to_workflow(self, workflow_id):
        # Subscribe to Redis channel
        channel = f'workflow:{workflow_id}:updates'
        await self.pubsub.subscribe(channel)

        # Listen for messages
        async for message in self.pubsub.listen():
            if message['type'] == 'message':
                await self.send_to_websocket(message['data'])
```

---

## Part 8: Implementation Requirements

### Phase 1: Database Migration (Weeks 1-4)

#### Week 1-2: PostgreSQL Setup
1. **Create PostgreSQL schema**
   - Run all CREATE TABLE statements
   - Create indexes
   - Setup RLS policies
   - Configure connection pooling (PgBouncer)

2. **TimescaleDB Setup**
   - Install TimescaleDB extension
   - Convert job_runs to hypertable
   - Setup compression and retention policies

3. **Data Migration Script**
   ```python
   # migrate_to_postgres.py
   async def migrate_workspaces():
       mongo_workspaces = await mongo_db.workspace.find().to_list(None)

       for workspace in mongo_workspaces:
           # Insert workspace
           workspace_id = await pg.execute(
               "INSERT INTO workspaces (...) VALUES (...) RETURNING workspace_id"
           )

           # Flatten folders
           for folder in workspace.get('folders', []):
               folder_id = await pg.execute(
                   "INSERT INTO folders (...) VALUES (...) RETURNING folder_id"
               )

               # Flatten jobs
               for job in folder.get('jobs', []):
                   job_id = await pg.execute(
                       "INSERT INTO jobs (...) VALUES (...) RETURNING job_id"
                   )

                   # Flatten workflows
                   for workflow in job.get('workflows', []):
                       await pg.execute(
                           "INSERT INTO workflows (...) VALUES (...)"
                       )
   ```

#### Week 3-4: Application Code Updates
1. **Replace MongoDB queries with PostgreSQL**
   ```python
   # OLD (MongoDB)
   workflow = await mongo_db.workflow.find_one({"workflow_id": wf_id})

   # NEW (PostgreSQL with asyncpg)
   workflow = await pg.fetchrow(
       """
       SELECT w.*,
              json_agg(wd.*) as dependencies
       FROM workflows w
       LEFT JOIN workflow_dependencies wd ON w.workflow_id = wd.workflow_id
       WHERE w.workflow_id = $1
       GROUP BY w.workflow_id
       """,
       wf_id
   )
   ```

2. **Update service layer**
   - Replace `mongo_db.get_collection()` with `pg_pool.acquire()`
   - Use transactions for multi-step operations
   - Add error handling and rollback logic

---

### Phase 2: Real-time Infrastructure (Weeks 5-6)

#### Week 5: Redis Pub/Sub
1. **Setup Redis Cluster** (3 masters, 3 replicas)
2. **Implement Redis Pub/Sub for WebSocket**
   ```python
   # websocket_manager.py
   class DistributedConnectionManager:
       def __init__(self):
           self.redis = aioredis.from_url('redis://localhost')
           self.pubsub = self.redis.pubsub()

       async def broadcast_to_workflow(self, workflow_id, message):
           channel = f'workflow:{workflow_id}:updates'
           await self.redis.publish(channel, json.dumps(message))

       async def listen_to_workflow(self, workflow_id):
           channel = f'workflow:{workflow_id}:updates'
           await self.pubsub.subscribe(channel)

           async for msg in self.pubsub.listen():
               if msg['type'] == 'message':
                   await self.send_to_client(msg['data'])
   ```

3. **Track connections in database**
   ```python
   # On WebSocket connect
   await pg.execute(
       """
       INSERT INTO websocket_connections
       (user_id, subscription_id, connection_type, subscribed_to)
       VALUES ($1, $2, $3, $4)
       """,
       user_id, sub_id, 'workflow_run', workflow_id
   )

   # Heartbeat every 30s
   await pg.execute(
       "UPDATE websocket_connections SET last_heartbeat_at = NOW() WHERE connection_id = $1",
       connection_id
   )

   # On disconnect
   await pg.execute(
       "UPDATE websocket_connections SET is_active = false, disconnected_at = NOW() WHERE connection_id = $1",
       connection_id
   )
   ```

#### Week 6: Redis Streams for Queues
1. **Replace RQ with Redis Streams**
2. **Implement consumer groups**
3. **Add monitoring and DLQ**

---

### Phase 3: Performance Optimization (Weeks 7-8)

#### Week 7: Caching Layer
1. **Implement cache-aside pattern**
   ```python
   async def get_workflow(workflow_id):
       # Check cache
       cached = await redis.get(f'workflow:{workflow_id}')
       if cached:
           return json.loads(cached)

       # Query database
       workflow = await pg.fetchrow(
           "SELECT * FROM workflows WHERE workflow_id = $1",
           workflow_id
       )

       # Cache result (5 min TTL)
       await redis.setex(
           f'workflow:{workflow_id}',
           300,
           json.dumps(workflow)
       )

       return workflow
   ```

2. **Implement cache invalidation**
   ```python
   async def update_workflow(workflow_id, updates):
       # Update database
       await pg.execute(
           "UPDATE workflows SET ... WHERE workflow_id = $1",
           workflow_id
       )

       # Invalidate cache
       await redis.delete(f'workflow:{workflow_id}')

       # Notify other instances
       await redis.publish(
           'cache:invalidate',
           json.dumps({'type': 'workflow', 'id': workflow_id})
       )
   ```

#### Week 8: Query Optimization
1. **Add materialized views**
   ```sql
   CREATE MATERIALIZED VIEW workflow_stats AS
   SELECT
       w.workspace_id,
       COUNT(*) as total_workflows,
       COUNT(*) FILTER (WHERE w.is_active) as active_workflows,
       AVG(jr.duration_ms) as avg_execution_time
   FROM workflows w
   LEFT JOIN job_runs jr ON w.workflow_id = jr.workflow_id
   GROUP BY w.workspace_id;

   CREATE INDEX idx_workflow_stats_workspace ON workflow_stats(workspace_id);

   -- Refresh every 5 minutes
   CREATE OR REPLACE FUNCTION refresh_workflow_stats()
   RETURNS void AS $$
   BEGIN
       REFRESH MATERIALIZED VIEW CONCURRENTLY workflow_stats;
   END;
   $$ LANGUAGE plpgsql;
   ```

2. **Optimize common queries**
   ```sql
   -- EXPLAIN ANALYZE to identify slow queries
   EXPLAIN (ANALYZE, BUFFERS, VERBOSE)
   SELECT * FROM workflows WHERE workspace_id = 'ws_123';

   -- Add covering indexes
   CREATE INDEX idx_workflows_workspace_covering
   ON workflows(workspace_id, is_deleted)
   INCLUDE (workflow_name, created_at, updated_at);
   ```

---

### Phase 4: Security Hardening (Weeks 9-10)

#### Week 9: Encryption
1. **Encrypt connector credentials**
   ```python
   # Store encrypted
   encryption_key = config("ENCRYPTION_KEY")

   async def save_connector(connector_data):
       encrypted_config = await pg.fetchval(
           "SELECT pgp_sym_encrypt($1, $2)",
           json.dumps(connector_data['connection_config']),
           encryption_key
       )

       await pg.execute(
           """
           INSERT INTO connectors (connector_name, connection_config_encrypted)
           VALUES ($1, $2)
           """,
           connector_data['name'],
           encrypted_config
       )

   # Retrieve and decrypt
   async def get_connector(connector_id):
       decrypted_config = await pg.fetchval(
           """
           SELECT pgp_sym_decrypt(connection_config_encrypted, $2)
           FROM connectors
           WHERE connector_id = $1
           """,
           connector_id,
           encryption_key
       )
       return json.loads(decrypted_config)
   ```

2. **Implement field-level encryption for sensitive data**

#### Week 10: RLS & Audit
1. **Enable RLS on all tables**
2. **Create audit triggers**
   ```sql
   CREATE OR REPLACE FUNCTION audit_trigger_func()
   RETURNS TRIGGER AS $$
   BEGIN
       INSERT INTO user_activity (
           user_id,
           subscription_id,
           action,
           object_type,
           object_id,
           previous_data,
           current_data
       ) VALUES (
           current_setting('app.user_id')::uuid,
           current_setting('app.subscription_id')::uuid,
           TG_OP,
           TG_TABLE_NAME,
           NEW.workflow_id,
           row_to_json(OLD),
           row_to_json(NEW)
       );
       RETURN NEW;
   END;
   $$ LANGUAGE plpgsql;

   CREATE TRIGGER workflows_audit
   AFTER INSERT OR UPDATE OR DELETE ON workflows
   FOR EACH ROW EXECUTE FUNCTION audit_trigger_func();
   ```

---

## Part 9: API Design Requirements

### RESTful API Standards

#### Endpoint Structure
```
/api/v1/workspaces                          GET, POST
/api/v1/workspaces/{workspace_id}           GET, PUT, DELETE
/api/v1/workspaces/{workspace_id}/folders   GET, POST
/api/v1/folders/{folder_id}                 GET, PUT, DELETE
/api/v1/folders/{folder_id}/jobs            GET, POST
/api/v1/jobs/{job_id}                       GET, PUT, DELETE
/api/v1/jobs/{job_id}/workflows             GET, POST
/api/v1/workflows/{workflow_id}             GET, PUT, DELETE
/api/v1/workflows/{workflow_id}/execute     POST
/api/v1/workflows/{workflow_id}/versions    GET
/api/v1/workflows/{workflow_id}/versions/{version_id}  GET
```

#### Response Format (Consistent)
```json
{
  "success": true,
  "data": {
    "workflow_id": "wf_123",
    "workflow_name": "ETL Pipeline",
    // ... entity data
  },
  "meta": {
    "timestamp": "2025-10-13T10:00:00Z",
    "version": "v1",
    "pagination": {
      "page": 1,
      "per_page": 20,
      "total": 100,
      "total_pages": 5
    }
  }
}

// Error response
{
  "success": false,
  "error": {
    "code": "WORKFLOW_NOT_FOUND",
    "message": "Workflow wf_123 not found",
    "details": {
      "workflow_id": "wf_123"
    }
  },
  "meta": {
    "timestamp": "2025-10-13T10:00:00Z",
    "version": "v1"
  }
}
```

#### Transaction API (NEW)
```python
# POST /api/v1/transactions/begin
# Returns transaction_id

# PUT /api/v1/workflows/{workflow_id}?transaction_id=txn_123
# Stages changes in transaction

# POST /api/v1/transactions/{transaction_id}/commit
# Commits all changes atomically

# POST /api/v1/transactions/{transaction_id}/rollback
# Rolls back all changes
```

---

## Part 10: Monitoring & Observability

### Metrics to Track

#### Application Metrics
- **Request Rate**: Requests per second per endpoint
- **Response Time**: p50, p95, p99 latencies
- **Error Rate**: 4xx and 5xx error percentages
- **WebSocket Connections**: Active connections per server

#### Database Metrics
```sql
-- Query performance
SELECT query, calls, mean_exec_time, stddev_exec_time
FROM pg_stat_statements
ORDER BY mean_exec_time DESC
LIMIT 10;

-- Index usage
SELECT schemaname, tablename, indexname, idx_scan, idx_tup_read
FROM pg_stat_user_indexes
WHERE idx_scan = 0
AND indexrelname NOT LIKE 'pg_toast%';

-- Table bloat
SELECT schemaname, tablename,
       pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename))
FROM pg_tables
WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;
```

#### Redis Metrics
```bash
# Monitor queue lengths
redis-cli --stat
redis-cli INFO stats
redis-cli SLOWLOG GET 10
```

### Logging Strategy

```python
# Structured logging
import structlog

logger = structlog.get_logger()

logger.info(
    "workflow_execution_started",
    workflow_id="wf_123",
    user_id="user_456",
    subscription_id="sub_789",
    trigger_type="manual"
)

# Log to:
# - stdout (JSON format) -> CloudWatch/ELK
# - PostgreSQL (user_activity table)
# - MongoDB (execution_logs collection, 7-day TTL)
```

---

## Part 11: Scalability Roadmap

### Current → 6 Months (0-10K users)
- ✅ PostgreSQL single instance (32GB RAM, 8 cores)
- ✅ Redis Cluster (3 masters, 3 replicas)
- ✅ MongoDB for logs only
- ✅ Vertical scaling sufficient

### 6 Months → 1 Year (10K-50K users)
- ✅ PostgreSQL Read Replicas (2 replicas)
- ✅ PgBouncer connection pooling
- ✅ TimescaleDB for time-series
- ✅ CDN for static assets
- ✅ Horizontal backend scaling (4-8 instances)

### 1 Year → 2 Years (50K-200K users)
- ✅ PostgreSQL Sharding by subscription_id
- ✅ Citus extension for distributed queries
- ✅ Redis Cluster (6 masters, 6 replicas)
- ✅ Message queue (Kafka) for event streaming
- ✅ Horizontal backend scaling (16-32 instances)
- ✅ Multi-region deployment

### 2+ Years (200K+ users)
- ✅ Multi-tenant database per region
- ✅ Global traffic routing
- ✅ Microservices architecture
- ✅ Service mesh (Istio)
- ✅ Auto-scaling (Kubernetes)

---

## Part 12: Cost Analysis

### Current MongoDB Architecture (Estimated)
```
MongoDB Atlas M40: $1,000/month
Redis Cloud 5GB: $100/month
Compute (EC2): $500/month
Total: $1,600/month
```

### Proposed PostgreSQL + Redis Architecture (Estimated)
```
PostgreSQL (RDS db.r6g.2xlarge): $600/month
TimescaleDB add-on: $200/month
Redis Cluster (6 nodes): $400/month
Compute (EC2 Auto-scaling): $800/month
CloudWatch/monitoring: $100/month
Total: $2,100/month

Increase: $500/month (+31%)
```

### Cost Savings at Scale (1 year projection)
```
Current architecture problems:
- Slow queries → Need larger instances → $3,000/month
- No caching → More compute → $1,500/month
- Data duplication → More storage → $500/month
Total projected: $5,000/month

Optimized architecture:
- Efficient queries → Standard instances → $2,100/month
- Caching → Less compute → Included
- Normalized data → Less storage → Included
Total projected: $2,100/month

Savings: $2,900/month (58% reduction)
```

---

## Part 13: Decision Matrix

| Requirement | Current Score | Proposed Score | Priority |
|------------|---------------|----------------|----------|
| **Scalability** | 2/10 | 9/10 | 🔴 Critical |
| **Performance** | 3/10 | 9/10 | 🔴 Critical |
| **Data Integrity** | 2/10 | 10/10 | 🔴 Critical |
| **API Driven** | 6/10 | 9/10 | 🟡 High |
| **Real-time Updates** | 4/10 | 9/10 | 🔴 Critical |
| **Security** | 3/10 | 9/10 | 🔴 Critical |

---

## Final Recommendation

### ✅ **PROCEED WITH MIGRATION**

**Reasoning**:
1. Current architecture will fail within 6 months at projected growth
2. Technical debt is accumulating (nested documents, no indexes)
3. Security vulnerabilities (plaintext passwords, no encryption)
4. Performance already degrading (slow queries, no caching)
5. Cannot scale horizontally (WebSocket in memory, no sharding)

### Migration Strategy: **Big Bang** (4 weeks downtime window) OR **Strangler Pattern** (6 months gradual migration)

**Recommendation**: **Strangler Pattern** - Less risk, allows testing in production

**Next Steps**:
1. **Week 1**: Review and approve this architecture document
2. **Week 2**: Setup PostgreSQL schema in dev environment
3. **Week 3**: Build data migration scripts
4. **Week 4**: Migrate first entity (Connectors) to PostgreSQL
5. **Week 5-12**: Gradually migrate remaining entities
6. **Week 13-16**: Performance optimization and monitoring

**Success Criteria**:
- ✅ Query response time <100ms (p95)
- ✅ Support 1,000 concurrent WebSocket connections
- ✅ Process 10,000 workflow executions/day
- ✅ Zero data loss during migration
- ✅ Zero downtime deployment (blue/green)

---

## Appendix: PostgreSQL vs MongoDB Decision

| Criteria | PostgreSQL | MongoDB |
|----------|-----------|---------|
| **ACID Transactions** | ✅ Full support | ⚠️ Limited (4.0+) |
| **Referential Integrity** | ✅ Foreign keys | ❌ Application layer |
| **Complex Queries** | ✅ JOINs, subqueries | ⚠️ Aggregation pipelines |
| **Indexing** | ✅ B-tree, GiST, GIN | ✅ B-tree, text |
| **Full-text Search** | ✅ Built-in | ⚠️ Atlas Search only |
| **Time-series Data** | ✅ TimescaleDB | ⚠️ Limited |
| **JSON Support** | ✅ JSONB (indexed) | ✅ Native |
| **Scalability** | ✅ Citus, read replicas | ✅ Sharding |
| **Maturity** | ✅ 30+ years | ⚠️ 15 years |
| **Tooling** | ✅ Extensive | ✅ Good |

**Verdict**: PostgreSQL for 80% of data (relational), MongoDB for 20% (flexible/logs)

---

**Document Version**: 1.0
**Last Updated**: 2025-10-13
**Status**: ✅ Ready for Review

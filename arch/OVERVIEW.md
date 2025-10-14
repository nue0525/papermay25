# UZO Workflow Automation Platform - Complete Architecture Overview

## 🎯 Executive Summary

**UZO** is an enterprise-grade workflow automation platform (similar to Informatica, Matillion, n8n, Zapier) with advanced ETL/ELT capabilities, GenAI integration, and real-time collaboration features.

### Platform Vision

- **100+ Connectors**: Databases, NoSQL, Cloud Storage, GenAI, Agentic, Business Apps
- **Visual Workflow Builder**: ReactFlow-based drag-and-drop canvas
- **Scalable Execution**: Prefect 3 orchestration with Polars transformations
- **Enterprise Features**: RBAC, multi-tenancy, 15-level lineage, audit logging, versioning
- **Real-time Updates**: WebSocket integration for live workflow monitoring
- **Edition-based**: Standard, Premium, Enterprise with feature flags

---

## 📋 Project Context

### Migration from backend0125

**🔴 CRITICAL**: The existing `backend0125` was built for **AntV X6** (deprecated). The new platform uses **ReactFlow**.

| Aspect | Old (backend0125) | New (backend) |
|--------|-------------------|---------------|
| **Frontend Canvas** | AntV X6 | ReactFlow 11+ |
| **Data Format** | `connectionInformation_v1` (JSON string) | `workflow_diagram` (JSONB, ReactFlow native) |
| **Database** | MongoDB (nested documents) | PostgreSQL (normalized) + MongoDB (logs only) |
| **Orchestration** | Redis RQ | Prefect 3 + PostgreSQL backend |
| **Transformations** | Pandas | Polars (7-16x faster) |
| **Permissions** | Basic JWT | RBAC + Row-Level Security |
| **Real-time** | In-memory WebSocket | Redis pub/sub + WebSocket |
| **Lineage** | Basic tracking | 15-level recursive lineage |
| **Multi-tenancy** | Single tenant | Shared + dedicated infrastructure |
| **Features** | Hardcoded | Feature flags + edition control |

**⚠️ NEVER reference `connectionInformation_v1` or copy patterns from backend0125**

---

## 📚 Architecture Documentation Index

All comprehensive architecture documents are located in `/Users/meek/Documents/nue_nueproj/`:

### 1. **[DATABASE_ARCHITECTURE_ANALYSIS.md](./DATABASE_ARCHITECTURE_ANALYSIS.md)**

**Purpose**: Database strategy and schema design

**Key Contents**:
- Analysis of backend0125 issues (nested documents, 16MB limit, no indexing)
- Complete PostgreSQL normalized schema (25+ tables)
- MongoDB simplified schema (execution logs only)
- TimescaleDB for time-series data
- Migration roadmap (10 weeks, 12 phases)
- Performance metrics (500ms → 20ms queries)
- Cost analysis ($1,600/mo → $2,100/mo)

**Key Tables**:
```
Subscriptions → Workspaces → Folders → Jobs → Workflows → Nodes
                                     ↓
                                JobPackages
Users → Groups → Roles → Permissions
Connectors, Lineage, Audit, Versions, Feature Flags
```

### 2. **[COMPREHENSIVE_ARCHITECTURE_V2.md](./COMPREHENSIVE_ARCHITECTURE_V2.md)**

**Purpose**: Product vision and ReactFlow integration

**Key Contents**:
- Product requirements and feature set
- ReactFlow node architecture (TypeScript interfaces)
- Individual node execution (dev preview)
- 100+ connector plugin system
- Connector categories: Database, NoSQL, File, Cloud, GenAI, Agentic, Business Apps
- Node types: Connector, Transformation, Join, Conditional, Loop, GenAI
- Complete API design for workflow CRUD
- Node execution API

**Key Interfaces**:
```typescript
WorkflowNode, WorkflowEdge, NodeConfig, ConnectorConfig
BaseConnector (abstract class for all connectors)
```

### 3. **[PREFECT_POLARS_ARCHITECTURE.md](./PREFECT_POLARS_ARCHITECTURE.md)**

**Purpose**: Workflow orchestration and transformation engine

**Key Contents**:
- Why Prefect 3 over Redis RQ (comparison table)
- Prefect 3 setup with PostgreSQL backend
- Dynamic flow generation from UZO workflows
- Topological sort for dependency resolution
- Polars transformation engine (filter, aggregate, join, pivot, window)
- Performance comparison (Polars 7-16x faster than Pandas)
- Real-time monitoring and completion forecasting
- Prefect UI integration

**Key Components**:
```python
PrefectWorkflowEngine: Convert UZO workflow → Prefect flow
PolarsTransformEngine: All transformation operations
WorkflowContext: Pass data between nodes
```

### 4. **[RBAC_MULTITENANCY_ARCHITECTURE.md](./RBAC_MULTITENANCY_ARCHITECTURE.md)**

**Purpose**: Security, permissions, and tenant isolation

**Key Contents**:
- Permission model hierarchy (User → Group → Role → Permission)
- Object-level permissions (workspace, folder, job, workflow)
- Permission inheritance (workspace permissions flow down)
- Complete PostgreSQL RBAC schema
- PermissionService with 5-layer checking
- Multi-tenancy architecture (shared vs dedicated databases)
- Row-Level Security (RLS) implementation
- DatabaseRouter for tenant-aware queries
- Tenant provisioning and shutdown automation

**Permission Levels**:
```
- Subscription Owner: Full access
- Read: View only
- Read-Write: View and edit
- Execute: Run workflows
- Admin: Manage permissions
```

### 5. **[LINEAGE_TRACKING_ARCHITECTURE.md](./LINEAGE_TRACKING_ARCHITECTURE.md)**

**Purpose**: Data lineage tracking and impact analysis

**Key Contents**:
- 15-level table lineage (upstream/downstream)
- Automatic lineage capture during workflow execution
- PostgreSQL schema with recursive CTEs
- Data assets (tables, files, APIs)
- Lineage edges (source → target relationships)
- Impact analysis ("What breaks if I change this table?")
- Table profiling (row count, column stats, data quality)
- ReactFlow-based lineage visualization
- Edition control (disabled in Standard, enabled in Premium)

**Key Features**:
```python
LineageService.capture_node_lineage()  # Auto-capture during execution
LineageService.get_upstream_lineage()  # 15 levels upstream
LineageService.get_downstream_lineage()  # 15 levels downstream
LineageService.get_impact_analysis()  # What's affected?
LineageService.profile_table()  # Data quality metrics
```

### 6. **[FEATURE_FLAG_ARCHITECTURE.md](./FEATURE_FLAG_ARCHITECTURE.md)**

**Purpose**: Edition-based features and usage limits

**Key Contents**:
- Product editions (Standard $99, Premium $299, Enterprise Custom)
- Feature matrix (lineage, monitoring, SSO, etc.)
- PostgreSQL schema (plans, features, plan_features, subscription_feature_overrides)
- FeatureFlagService with 3-level priority (override → plan → default)
- Usage limit enforcement (executions, workflows, users)
- FastAPI FeatureGate middleware
- React hooks (useFeatureFlag, FeatureGate component)
- Subscription upgrade flow with automatic feature enablement

**Edition Features**:
```
Standard: 10 workflows, 10 connectors, 100 executions/mo
Premium: + Lineage, monitoring, 50 connectors, 1000 executions/mo
Enterprise: + Everything unlimited, SSO, dedicated infra
```

### 7. **[WEBSOCKET_REALTIME_ARCHITECTURE.md](./WEBSOCKET_REALTIME_ARCHITECTURE.md)**

**Purpose**: Real-time updates without page refresh

**Key Contents**:
- WebSocket + Redis pub/sub architecture
- WebSocketManager with connection registry
- Channel-based subscriptions (workflow:id, user:id, subscription:id)
- Real-time workflow execution updates (node status every 100ms)
- Theme and dashboard updates without refresh
- Horizontal scaling (multiple FastAPI instances)
- React WebSocketContext and hooks
- Message persistence with Redis Streams
- Automatic reconnection with exponential backoff

**Message Types**:
```javascript
workflow_update, node_update, theme_updated,
dashboard_updated, subscription_updated, notification
```

### 8. **[AUDIT_LOGGING_VERSIONING_ARCHITECTURE.md](./AUDIT_LOGGING_VERSIONING_ARCHITECTURE.md)**

**Purpose**: Comprehensive audit logging and version control

**Key Contents**:
- Multi-level logging (server, workflow, node, job, jobpackage)
- PostgreSQL + TimescaleDB schema (audit_logs, execution_logs, server_logs)
- AuditService with automatic tracking
- FastAPI AuditMiddleware for all requests
- Data change history (complete before/after snapshots)
- Version control system (check-in/check-out)
- VersionService with version tagging and comments
- Cascade delete tracking with soft delete
- Deletion logs with restore capability (30 days)

**Audit Levels**:
```
- Server: API requests, auth events, errors (90 days)
- Workflow: CRUD, executions, status (1 year)
- Node: Execution, data processed, errors (90 days)
- Job/JobPackage: CRUD, executions (1 year)
- User Actions: All CRUD, permissions (permanent)
```

---

## 🏗️ Backend Implementation Roadmap

### Visual Roadmap Diagram

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                     UZO BACKEND IMPLEMENTATION PLAN                         │
│                          (20 weeks, 8 phases)                               │
└─────────────────────────────────────────────────────────────────────────────┘

PHASE 1: FOUNDATION (Weeks 1-2)
├── Database Setup
│   ├── PostgreSQL schema creation (25+ tables)
│   ├── TimescaleDB setup for time-series
│   ├── MongoDB setup for execution logs
│   └── Database connection pooling
├── Core Infrastructure
│   ├── FastAPI app structure
│   ├── Environment configuration
│   ├── Logging setup (structlog)
│   └── Error handling middleware
└── Authentication & Authorization
    ├── JWT token service
    ├── User registration/login API
    ├── Password hashing (bcrypt)
    └── API key authentication

PHASE 2: USER & SUBSCRIPTION MANAGEMENT (Weeks 3-4)
├── User Management
│   ├── User CRUD APIs
│   ├── User profile management
│   └── Password reset flow
├── Subscription Management
│   ├── Subscription plans (Standard/Premium/Enterprise)
│   ├── Subscription CRUD APIs
│   ├── Feature flag system (FeatureFlagService)
│   └── Usage limit enforcement
└── RBAC System
    ├── Groups, roles, permissions schema
    ├── PermissionService implementation
    ├── Permission checking middleware
    └── Row-Level Security setup

PHASE 3: WORKSPACE & ORGANIZATION (Weeks 5-6)
├── Workspace APIs
│   ├── Workspace CRUD (with soft delete)
│   ├── Workspace permissions
│   └── Workspace settings
├── Folder APIs
│   ├── Folder CRUD (nested hierarchy)
│   ├── Folder permissions (inherited)
│   └── Folder navigation
└── Multi-tenant Router
    ├── DatabaseRouter implementation
    ├── Tenant context middleware
    └── Shared vs dedicated DB routing

PHASE 4: CONNECTOR SYSTEM (Weeks 7-9)
├── Connector Framework
│   ├── BaseConnector abstract class
│   ├── Connector plugin loader
│   ├── Connection pooling
│   └── Connection testing
├── Database Connectors (15+)
│   ├── PostgreSQL, MySQL, Oracle
│   ├── SQL Server, Teradata, Snowflake
│   ├── ClickHouse, BigQuery, Redshift
│   └── MongoDB, Cassandra, DynamoDB
├── File & Cloud Connectors (10+)
│   ├── CSV, JSON, Parquet, Excel
│   ├── Google Cloud Storage, AWS S3
│   ├── Azure Blob, SFTP, FTP
│   └── Google Drive, Dropbox
└── Connector CRUD APIs
    ├── Connector creation/testing
    ├── Connector credential encryption
    └── Connector permissions

PHASE 5: WORKFLOW ENGINE (Weeks 10-12)
├── Workflow Management
│   ├── Workflow CRUD APIs (ReactFlow format)
│   ├── Workflow validation (cycles, orphans)
│   ├── Workflow permissions
│   └── Workflow versioning
├── Job Management
│   ├── Job CRUD APIs
│   ├── Job scheduling (cron, manual, trigger)
│   └── JobPackage CRUD APIs
├── Prefect Integration
│   ├── Prefect 3 setup + PostgreSQL backend
│   ├── PrefectWorkflowEngine implementation
│   ├── Dynamic flow generation
│   ├── Topological sort for node order
│   └── Prefect UI integration
└── Node Execution
    ├── Individual node execution (dev preview)
    ├── Node status tracking
    └── Node output preview

PHASE 6: TRANSFORMATION ENGINE (Weeks 13-14)
├── Polars Integration
│   ├── PolarsTransformEngine implementation
│   ├── Filter, aggregate, join operations
│   ├── Pivot, window, union operations
│   └── Custom transformation functions
├── Transformation Nodes
│   ├── Filter node
│   ├── Aggregate node (group by)
│   ├── Join node (left, right, inner, outer)
│   ├── Pivot/Unpivot nodes
│   └── Window function node
└── Data Type Handling
    ├── Type inference
    ├── Type conversion
    └── Schema validation

PHASE 7: LINEAGE & MONITORING (Weeks 15-17)
├── Lineage Tracking
│   ├── LineageService implementation
│   ├── Automatic lineage capture during execution
│   ├── 15-level upstream/downstream queries
│   ├── Impact analysis APIs
│   ├── Table profiling APIs
│   └── Lineage visualization data
├── Execution Monitoring
│   ├── Real-time execution status
│   ├── Execution logs (TimescaleDB)
│   ├── Performance metrics (duration, rows, memory)
│   ├── Completion time forecasting
│   └── Execution history APIs
└── WebSocket Integration
    ├── WebSocketManager implementation
    ├── Redis pub/sub setup
    ├── Channel subscriptions
    ├── Workflow execution broadcasts
    └── Theme/dashboard update broadcasts

PHASE 8: AUDIT, VERSIONING & ADVANCED FEATURES (Weeks 18-20)
├── Audit System
│   ├── AuditService implementation
│   ├── AuditMiddleware for all requests
│   ├── Audit log query APIs
│   ├── Execution log APIs
│   └── Server log APIs
├── Versioning System
│   ├── VersionService implementation
│   ├── Check-in/check-out APIs
│   ├── Version history APIs
│   ├── Version diff/compare APIs
│   └── Rollback functionality
├── Cascade Delete
│   ├── Soft delete implementation
│   ├── Cascade delete tracking
│   ├── Deletion log APIs
│   └── Restore from soft delete
├── Advanced Features
│   ├── Notifications (email, in-app)
│   ├── Webhooks (workflow events)
│   ├── API rate limiting
│   ├── Data export (workflows, logs)
│   └── Analytics dashboard APIs

┌─────────────────────────────────────────────────────────────────────────────┐
│                          POST-IMPLEMENTATION                                │
└─────────────────────────────────────────────────────────────────────────────┘

PHASE 9: TESTING & OPTIMIZATION (Weeks 21-22)
├── Unit tests (pytest)
├── Integration tests
├── Load testing (Locust)
├── Performance optimization
├── Query optimization
└── Caching strategy (Redis)

PHASE 10: FRONTEND INTEGRATION (Weeks 23-25)
├── API documentation (OpenAPI/Swagger)
├── Frontend API client generation
├── WebSocket client integration
├── Error handling and retry logic
└── End-to-end testing

PHASE 11: DEPLOYMENT (Week 26)
├── Docker containerization
├── Kubernetes manifests
├── CI/CD pipeline (GitHub Actions)
├── Environment configuration
├── Database migrations (Alembic)
└── Monitoring setup (Prometheus, Grafana)
```

---

## 📊 Database Schema Overview

### Core Tables (PostgreSQL)

```sql
-- Authentication & Subscriptions
subscriptions (plan_id, company_name, status, infrastructure_type)
subscription_plans (plan_name, base_price, max_workflows, max_executions)
features (feature_key, feature_name, default_enabled)
plan_features (plan_id, feature_id, is_enabled)
subscription_feature_overrides (subscription_id, feature_id, is_enabled)

-- Users & Permissions
users (user_id, username, email, password_hash, subscription_id)
groups (group_id, group_name, subscription_id)
user_groups (user_id, group_id)
roles (role_id, role_name, subscription_id)
permissions (permission_id, resource_type, action, role_id)
object_permissions (object_type, object_id, user_id/group_id, permission_level)

-- Workspace Hierarchy
workspaces (workspace_id, workspace_name, subscription_id, soft_deleted)
folders (folder_id, folder_name, workspace_id, parent_folder_id, soft_deleted)
jobs (job_id, job_name, folder_id, schedule_type, cron_expression, soft_deleted)
jobpackages (jobpackage_id, package_name, job_ids, soft_deleted)

-- Workflows & Nodes
workflows (workflow_id, workflow_name, job_id, workflow_diagram JSONB, soft_deleted)
  -- workflow_diagram: {nodes: [...], edges: [...], viewport: {...}}
  -- nodes: {id, type, position, data: {nodeType, config}}

-- Connectors
connectors (connector_id, connector_name, connector_type, connection_config JSONB)
connector_types (type_id, type_name, category, plugin_class)

-- Lineage
data_assets (asset_id, asset_qualified_name, connector_id, is_source, is_target)
lineage_edges (edge_id, source_asset_id, target_asset_id, workflow_id, node_id)
lineage_impact_cache (asset_id, upstream_assets, downstream_assets, related_workflows)
table_profiles (profile_id, asset_id, row_count, column_profiles JSONB)

-- Execution & Monitoring
workflow_executions (execution_id, workflow_id, status, started_at, completed_at)
node_executions (node_execution_id, execution_id, node_id, status, rows_processed)
prefect_flow_runs (flow_run_id, workflow_id, prefect_state, deployment_id)

-- Audit & Logging (TimescaleDB hypertables)
audit_logs (audit_id, user_id, action, resource_type, resource_id, changes JSONB)
execution_logs (log_id, execution_id, log_level, log_message, performance_metrics)
server_logs (log_id, request_id, endpoint, response_status, response_time_ms)
data_change_history (change_id, table_name, record_id, old_data, new_data, version_number)

-- Versioning
versions (version_id, resource_type, resource_id, version_number, version_data JSONB)
version_comments (comment_id, version_id, comment_text, commented_by)

-- Cascade Delete
deletion_logs (deletion_id, resource_type, resource_id, cascaded_counts, is_soft_delete)
```

### MongoDB Collections (Execution Logs Only)

```javascript
// Detailed execution logs (verbose, high-volume)
workflow_execution_logs: {
  execution_id, workflow_id, node_id, timestamp,
  log_level, log_message, log_details
}

// Prefect task logs
prefect_task_logs: {
  flow_run_id, task_run_id, timestamp,
  log_level, message, metadata
}
```

---

## 🔧 Technology Stack

### Backend

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Web Framework** | FastAPI 0.100+ | Async API, auto docs, validation |
| **Database (Primary)** | PostgreSQL 15+ | Normalized data, JSONB, RLS |
| **Time-series** | TimescaleDB | Logs, metrics, execution history |
| **Database (Logs)** | MongoDB 7.0+ | High-volume execution logs |
| **Caching** | Redis 7.0+ | Sessions, pub/sub, rate limiting |
| **Orchestration** | Prefect 3 | Workflow execution, DAGs |
| **Transformations** | Polars | DataFrame operations (7-16x faster) |
| **WebSocket** | FastAPI WebSocket | Real-time updates |
| **Message Broker** | Redis Pub/Sub | WebSocket scaling |
| **ORM** | asyncpg (raw SQL) | Async PostgreSQL |
| **Validation** | Pydantic v2 | Request/response schemas |
| **Auth** | JWT + bcrypt | Token-based auth, password hashing |
| **Encryption** | cryptography | Connector credentials |
| **Logging** | structlog | Structured logging |

### Frontend (Reference)

| Component | Technology |
|-----------|-----------|
| **Framework** | Next.js 14 (App Router) |
| **Language** | TypeScript 5.x |
| **Workflow Canvas** | ReactFlow 11+ |
| **State Management** | Zustand |
| **UI Components** | Radix UI + Tailwind CSS |
| **WebSocket Client** | Native WebSocket API |
| **API Client** | Fetch API + SWR |

---

## 🚀 API Implementation Phases

### Phase 1: Authentication APIs (Week 1)

```python
POST   /api/auth/register          # Register new user
POST   /api/auth/login             # Login (returns JWT)
POST   /api/auth/logout            # Logout
POST   /api/auth/refresh           # Refresh token
POST   /api/auth/forgot-password   # Request password reset
POST   /api/auth/reset-password    # Reset password with token
GET    /api/auth/me                # Get current user
```

### Phase 2: Subscription & Feature APIs (Week 3)

```python
# Subscriptions
GET    /api/subscriptions          # List subscriptions (admin)
POST   /api/subscriptions          # Create subscription (admin)
GET    /api/subscriptions/{id}     # Get subscription details
PATCH  /api/subscriptions/{id}     # Update subscription
DELETE /api/subscriptions/{id}     # Delete subscription

# Feature Flags
GET    /api/features/check/{key}   # Check if feature enabled
GET    /api/features/enabled       # Get all enabled features
GET    /api/features/usage/{type}  # Check usage limits
POST   /api/subscriptions/upgrade  # Upgrade subscription plan
```

### Phase 3: User & Permission APIs (Week 4)

```python
# Users
GET    /api/users                  # List users in subscription
POST   /api/users                  # Create user
GET    /api/users/{id}             # Get user details
PATCH  /api/users/{id}             # Update user
DELETE /api/users/{id}             # Delete user

# Groups
GET    /api/groups                 # List groups
POST   /api/groups                 # Create group
POST   /api/groups/{id}/members    # Add user to group
DELETE /api/groups/{id}/members/{user_id}  # Remove from group

# Permissions
GET    /api/permissions/check      # Check permission
POST   /api/permissions            # Grant permission
DELETE /api/permissions/{id}       # Revoke permission
```

### Phase 4: Workspace APIs (Week 5)

```python
# Workspaces
GET    /api/workspaces             # List workspaces
POST   /api/workspaces             # Create workspace
GET    /api/workspaces/{id}        # Get workspace details
PATCH  /api/workspaces/{id}        # Update workspace
DELETE /api/workspaces/{id}        # Delete workspace (soft/hard)
POST   /api/workspaces/{id}/restore  # Restore soft-deleted

# Folders
GET    /api/workspaces/{id}/folders       # List folders
POST   /api/workspaces/{id}/folders       # Create folder
GET    /api/folders/{id}                  # Get folder details
PATCH  /api/folders/{id}                  # Update folder
DELETE /api/folders/{id}                  # Delete folder
```

### Phase 5: Connector APIs (Week 7)

```python
# Connectors
GET    /api/connectors             # List connectors
POST   /api/connectors             # Create connector
GET    /api/connectors/{id}        # Get connector details
PATCH  /api/connectors/{id}        # Update connector
DELETE /api/connectors/{id}        # Delete connector
POST   /api/connectors/{id}/test   # Test connection

# Connector Types
GET    /api/connector-types         # List available connector types
GET    /api/connector-types/{type}  # Get connector schema/config
```

### Phase 6: Workflow APIs (Week 10)

```python
# Jobs
GET    /api/folders/{id}/jobs      # List jobs in folder
POST   /api/folders/{id}/jobs      # Create job
GET    /api/jobs/{id}              # Get job details
PATCH  /api/jobs/{id}              # Update job
DELETE /api/jobs/{id}              # Delete job

# Workflows
GET    /api/jobs/{id}/workflows    # List workflows in job
POST   /api/jobs/{id}/workflows    # Create workflow
GET    /api/workflows/{id}         # Get workflow (with diagram)
PATCH  /api/workflows/{id}         # Update workflow diagram
DELETE /api/workflows/{id}         # Delete workflow

# Workflow Execution
POST   /api/workflows/{id}/execute # Execute workflow
GET    /api/workflows/{id}/executions  # List executions
GET    /api/executions/{id}        # Get execution details
POST   /api/executions/{id}/cancel # Cancel execution

# Node Execution (Dev Preview)
POST   /api/workflows/{id}/nodes/{node_id}/execute  # Execute single node
GET    /api/workflows/{id}/nodes/{node_id}/preview  # Preview node output
```

### Phase 7: Lineage APIs (Week 15)

```python
# Lineage
GET    /api/lineage/assets/{id}/upstream    # Get upstream lineage (15 levels)
GET    /api/lineage/assets/{id}/downstream  # Get downstream lineage
GET    /api/lineage/assets/{id}/full        # Get full lineage graph
GET    /api/lineage/assets/{id}/impact      # Get impact analysis

# Table Profiling
POST   /api/lineage/assets/{id}/profile     # Profile table
GET    /api/lineage/assets/{id}/profiles    # Get historical profiles

# Workflow Assets
GET    /api/lineage/workflows/{id}/assets   # Get all assets used by workflow
```

### Phase 8: Audit & Versioning APIs (Week 18)

```python
# Audit Logs
GET    /api/audit/logs             # Query audit logs (filtered)
GET    /api/audit/executions       # Query execution logs
GET    /api/audit/server           # Query server logs

# Versioning
GET    /api/workflows/{id}/versions        # Get version history
POST   /api/workflows/{id}/versions        # Create new version
POST   /api/versions/{id}/checkout         # Check out version
POST   /api/versions/{id}/checkin          # Check in version
POST   /api/versions/{id}/publish          # Publish version
POST   /api/versions/{id}/rollback         # Rollback to version

# Deletion & Restore
GET    /api/audit/deletions        # Query deletion logs
POST   /api/deletions/{id}/restore # Restore soft-deleted resource
```

---

## 🔐 Security Checklist

- ✅ **Authentication**: JWT tokens with expiration, refresh tokens
- ✅ **Authorization**: RBAC with object-level permissions
- ✅ **Multi-tenancy**: Row-Level Security (RLS) for data isolation
- ✅ **Encryption**: Connector credentials encrypted at rest (Fernet)
- ✅ **Password Security**: bcrypt hashing with salt
- ✅ **API Rate Limiting**: Redis-based rate limiter
- ✅ **Input Validation**: Pydantic models for all endpoints
- ✅ **SQL Injection**: Parameterized queries (asyncpg)
- ✅ **CORS**: Configured for specific origins
- ✅ **Audit Logging**: All actions logged with user context
- ✅ **Soft Delete**: Recoverable deletion for 30 days
- ✅ **Session Management**: Redis-based sessions
- ✅ **API Keys**: Alternative auth for programmatic access

---

## 📈 Performance Targets

| Metric | Target | Strategy |
|--------|--------|----------|
| **API Response Time** | < 100ms (p95) | Connection pooling, caching, indexing |
| **Workflow Execution** | 10,000+ nodes | Prefect 3 distributed execution |
| **Concurrent Workflows** | 1,000+ | Horizontal scaling, queue-based |
| **WebSocket Connections** | 10,000+ | Redis pub/sub, multiple instances |
| **Database Queries** | < 50ms (p95) | Indexes, JSONB GIN indexes, materialized views |
| **Lineage Queries** | < 100ms (15 levels) | Pre-computed cache, recursive CTEs |
| **Transformation Speed** | 7-16x faster | Polars vs Pandas |
| **Audit Log Ingestion** | 10,000+ logs/sec | TimescaleDB hypertables |

---

## 🎯 Next Steps

1. **Review architecture documents** (all 8 documents)
2. **Set up development environment** (PostgreSQL, Redis, MongoDB, Prefect)
3. **Start Phase 1 implementation** (Foundation, weeks 1-2)
4. **Follow the 20-week roadmap** sequentially
5. **Test each phase** before moving to next
6. **Integrate with frontend** after Phase 8

---

## 📞 Support & References

### Architecture Documents
- All documents in `/Users/meek/Documents/nue_nueproj/`
- CLAUDE.md in `backend0125/` has quick reference

### Key Technologies
- FastAPI: https://fastapi.tiangolo.com/
- Prefect 3: https://docs.prefect.io/
- Polars: https://pola.rs/
- TimescaleDB: https://docs.timescale.com/
- ReactFlow: https://reactflow.dev/

### Development Principles
1. **Never copy from backend0125** - it has anti-patterns
2. **Use PostgreSQL for app data** - MongoDB only for logs
3. **Use Prefect 3 for workflows** - not Redis RQ
4. **Use Polars for transformations** - not Pandas
5. **Implement RBAC from start** - don't add later
6. **Use WebSocket manager** - not in-memory state
7. **Check feature flags** - gate premium features
8. **Log everything** - audit all actions

---

**Last Updated**: 2025-10-14
**Status**: Architecture Complete, Ready for Implementation
**Timeline**: 20 weeks (5 months)
**Team Size**: 2-3 backend developers recommended

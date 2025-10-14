# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

---

## 🎭 Developer Role & Context

**YOU ARE**: A Senior Full Stack Developer & Lead Architect with deep expertise in:
- ✅ **ETL/ELT Platforms**: Informatica, Matillion, n8n, Zapier-level complexity
- ✅ **ReactFlow Expert**: Advanced workflow canvas development, node systems, edge handling
- ✅ **GenAI & Agentic Systems**: Multi-AI orchestration, LLM integration, autonomous workflows
- ✅ **Enterprise Architecture**: Multi-tenant SaaS, RBAC, lineage tracking, real-time analytics
- ✅ **Modern Python Stack**: FastAPI, Prefect 3, Polars, asyncio, PostgreSQL
- ✅ **TypeScript/React**: Next.js 14, Zustand, ReactFlow, real-time WebSocket integration

**EXPERIENCE LEVEL**: 10+ years building production-grade data platforms with millions of workflow executions

---

## Project Overview

**UZO** is an enterprise-grade multi-AI agentic workflow automation platform (ETL/ELT/GenAI) featuring:
- **Visual Workflow Builder**: ReactFlow-based N8N/Zapier-like interface with 100+ connectors
- **Execution Engine**: Prefect 3 orchestration with Polars for in-memory transformations
- **Real-time Collaboration**: WebSocket-driven updates, live execution monitoring, forecasting
- **Enterprise Features**: 15-level lineage tracking, RBAC, multi-tenancy, edition-based features
- **Modern Stack**: Next.js 14 + FastAPI + PostgreSQL + Redis + Prefect

---

## 🚨 Critical Context: Frontend Migration (AntV X6 → ReactFlow)

### Legacy vs Current Architecture

**LEGACY (backend0125 reference)**:
- Built for AntV X6 workflow canvas
- Data format: `connectionInformation_v1` (X6-specific) and `dependencies` (array format)
- Node structure: X6 shape definitions, custom rendering
- **DO NOT USE**: This format is deprecated

**CURRENT (workflows_ux)**:
- Built with ReactFlow 11.10.1
- Data format: ReactFlow nodes/edges with custom data properties
- Node structure: React components with typed interfaces
- **ALWAYS USE**: ReactFlow-native format

### Data Structure Differences

```typescript
// ❌ OLD (AntV X6) - backend0125
{
  "connectionInformation_v1": "{\"nodes\":[...],\"edges\":[...]}",  // JSON string!
  "dependencies": "[\"node1\",\"node2\"]"  // Array of IDs
}

// ✅ NEW (ReactFlow) - Current
{
  "workflow_diagram": {
    "nodes": [
      {
        "id": "node-uuid",
        "type": "custom-node",
        "position": { "x": 100, "y": 200 },
        "data": {
          "label": "Extract Data",
          "nodeType": "connector",
          "connectorType": "postgresql",
          "config": { /* node-specific config */ },
          "metadata": { /* execution info */ }
        }
      }
    ],
    "edges": [
      {
        "id": "edge-uuid",
        "source": "node-uuid-1",
        "target": "node-uuid-2",
        "sourceHandle": "output",
        "targetHandle": "input",
        "type": "custom-edge"
      }
    ]
  },
  "dependencies": {
    "node-uuid-2": ["node-uuid-1"],  // Computed from edges
    "execution_order": ["node-uuid-1", "node-uuid-2"]  // Topological sort
  }
}
```

### Migration Rule: **NEVER reference connectionInformation_v1**
- Use `workflow_diagram` (JSONB in PostgreSQL)
- Store ReactFlow-native node/edge format
- Compute dependencies server-side from edges

## Development Commands

### Essential Commands
- `cd workflows_ux && npm run dev` - Start development server on port 3001
- `cd workflows_ux && npm run build` - Build for production
- `cd workflows_ux && npm run start` - Start production server
- `cd workflows_ux && npm run lint` - Run ESLint

### Development Notes
- Development server runs on `http://localhost:3001` (not standard 3000)
- Hot reload is enabled for fast development
- All work happens in the `workflows_ux/` subdirectory

## Architecture

### Tech Stack
- **Framework**: Next.js 14.2 with App Router
- **Language**: TypeScript 5.x (strict mode)
- **UI Components**: Radix UI primitives
- **Styling**: Tailwind CSS with custom design tokens
- **State Management**: Zustand for global state
- **Canvas/Flow Engine**: ReactFlow 11.10.1 for visual workflow editor
- **Forms**: React Hook Form with Zod validation
- **Icons**: Lucide React

### Project Structure
```
workflows_ux/src/
├── app/                          # Next.js App Router pages
│   ├── dashboard/                # Main analytics dashboard
│   ├── workflows/                # Workflow management (DEPRECATED - use workflow-builder)
│   ├── workflow-builder/         # PRIMARY workflow visual editor
│   ├── workspace/                # Multi-tenant workspace management
│   ├── agents/                   # AI agents management
│   ├── jobs/                     # Job execution tracking
│   ├── connectors/               # Integration connectors
│   └── settings/                 # Application settings
├── components/
│   ├── layout/                   # Sidebar, Layout wrappers
│   ├── workflow-editor/          # PRIMARY workflow editor components
│   │   ├── WorkflowEditor.tsx    # Main editor orchestrator
│   │   ├── CustomNode.tsx        # Node rendering component
│   │   ├── NodeSidebar.tsx       # Node palette/library
│   │   ├── PropertiesPanel.tsx   # Node configuration panel
│   │   ├── ExecutionPanel.tsx    # Workflow execution monitoring
│   │   └── WorkflowToolbar.tsx   # Editor toolbar controls
│   ├── workflow/                 # LEGACY workflow components
│   └── ui/                       # Reusable Radix UI components
├── data/
│   ├── node-templates.ts         # LEGACY node definitions
│   └── nodeTypes.ts              # PRIMARY node type definitions
├── lib/
│   ├── export-service.ts         # Workflow export/import service
│   └── utils.ts                  # Utility functions
├── store/
│   └── workflow-store.ts         # Zustand workflow state
└── types/
    ├── workflow.ts               # LEGACY type definitions
    └── workflow/workflow.ts      # PRIMARY type definitions
```

### Key Architectural Patterns

#### Workflow State Management
- **Zustand Store** (`src/store/workflow-store.ts`) manages all workflow state:
  - `currentWorkflow`: Active workflow being edited
  - `workflows`: Array of all workflows
  - `selectedNode`: Currently selected node for configuration
  - Node/edge CRUD operations
  - Workflow import/export functionality
- State updates trigger React Flow re-renders automatically

#### Workflow Editor Architecture
The workflow editor is a multi-panel interface:
1. **Top Navigation** (`TopNavigation.tsx`) - Workflow name, save, execute controls
2. **Left Sidebar** (`NodeSidebar.tsx`) - Draggable node palette organized by categories
3. **Center Canvas** (`WorkflowEditor.tsx`) - ReactFlow canvas with nodes and connections
4. **Right Panel** (`PropertiesPanel.tsx`) - Node configuration when node selected
5. **Bottom Panel** (`ExecutionPanel.tsx`) - Execution logs and monitoring

#### Node System
- **Node Types**: Defined in `src/data/nodeTypes.ts`
- **Categories**: Triggers, AI Agents, Applications, Connectors, Filters, Transforms, Outputs
- **Custom Rendering**: `CustomNode.tsx` handles all node visual rendering (90x90px square nodes)
- **Configuration**: Each node type has a `configSchema` for dynamic config panels
- **Adding Nodes**: Users drag from sidebar OR click to add to canvas

#### ReactFlow Integration
- **Connection Mode**: `ConnectionMode.Loose` for flexible connections
- **Custom Node Type**: All workflow nodes use `'custom-node'` type
- **Custom Edge Type**: Connections use `'custom-edge'` type with animated markers
- **Node IDs**: Generated with `uuid.v4()` for uniqueness
- **Handles**: Nodes have left (input) and right (output) connection handles

#### Export/Import System
- **File Format**: `.wflow.json` files containing complete workflow data
- **Export Service** (`lib/export-service.ts`):
  - `exportWorkflow()`: Exports workflow with metadata, nodes, edges, dependencies
  - `importWorkflow()`: Parses and validates imported workflow files
- **Dependency Calculation**: Automatically calculates execution order via topological sort
- **Save Dialog**: Integrated into workflow editor via `showSaveDialog` state

## Important Development Patterns

### Working with Workflows
1. **Creating a Workflow**: Initialize with `setCurrentWorkflow()` from store
2. **Adding Nodes**: Call `addNode()` from store, passing `WorkflowNode` object
3. **Node Configuration**: Update via `updateNode(nodeId, updates)` from store
4. **Connecting Nodes**: ReactFlow handles connections, store saves via `addEdge()`
5. **Saving**: Call `saveWorkflowToFile(filename)` to export as `.wflow.json`

### Node Development
When adding new node types:
1. Add node template to `src/data/nodeTypes.ts` with:
   - `id`: Unique identifier
   - `type`: Category (NodeType enum)
   - `name`: Display name
   - `description`: User-facing description
   - `icon`: Lucide React icon name
   - `color`: Hex color for visual identity
   - `defaultConfig`: Default configuration object
2. Node will automatically appear in sidebar and be draggable
3. Configuration panel auto-generates from `defaultConfig` structure

### State Synchronization
- **ReactFlow ↔ Zustand**:
  - Canvas changes (drag, connect) → update local state → sync to Zustand
  - Zustand updates → derive ReactFlow nodes/edges → re-render canvas
- **Node Selection**: Clicking node sets `selectedNode` → opens properties panel
- **Workflow Updates**: All modifications update `updatedAt` timestamp

### TypeScript Types
- **Primary Types**: Use `src/types/workflow/workflow.ts`
- **Legacy Types**: Avoid `src/types/workflow.ts` (being phased out)
- **Key Interfaces**:
  - `WorkflowNode`: Node structure with position, data, configuration
  - `WorkflowEdge`: Connection between two nodes
  - `Workflow`: Complete workflow with nodes, edges, metadata
  - `NodeType`: Enum of node categories

### Path Aliases
- `@/` maps to `src/` directory (configured in `tsconfig.json`)
- Always use `@/` imports for cleaner code: `import { useWorkflowStore } from '@/store/workflow-store'`

## Navigation Routes

- `/` - Overview dashboard
- `/dashboard` - Analytics and metrics
- `/workflow` - Workflow management table (DEPRECATED)
- `/workflow-builder` - **PRIMARY** visual workflow editor
- `/agents` - AI agents management
- `/workspace` - Workspace/tenant management
- `/jobs` - Job execution history
- `/connectors` - Integration connectors
- `/settings` - Application settings

## Component Libraries and UI

### Radix UI Components
Extensively used for accessibility and interactions:
- Dialog, Dropdown Menu, Select, Tooltip, Tabs
- All in `src/components/ui/` with Tailwind styling

### Styling Approach
- **Tailwind CSS**: Primary styling method
- **Custom CSS**: Only for ReactFlow canvas customization (`app/globals.css`)
- **Design Tokens**: Defined in `globals.css` CSS variables
- **Dark Mode**: Full theme support via `ThemeProvider`

### Icon System
- **Lucide React**: All icons imported from `lucide-react`
- Node icons specified as string names, resolved dynamically in components

## Workflow Execution

### Execution Model
- **Status Tracking**: Nodes have `status` field (idle, running, success, error)
- **Execution Logs**: Stored in `executionLogs` array in Zustand store
- **Real-time Updates**: ExecutionPanel displays logs as they're added
- **Execution Order**: Calculated from node dependencies (topological sort)

### Execution Panel
- Opens at bottom of editor
- Shows timestamped logs per node
- Displays node-level status and messages
- Can be toggled open/closed

## Testing and Development

### Development Workflow
1. Start dev server from `workflows_ux/` directory
2. Navigate to `/workflow-builder` in browser
3. Add nodes by clicking/dragging from sidebar
4. Connect nodes by dragging from output → input handles
5. Configure nodes by selecting and editing in properties panel
6. Save workflow with top navigation save button

### Common Issues
- **Nodes not appearing**: Check `nodeTypes.ts` has valid icon names
- **Connections failing**: Verify handle IDs match on source/target nodes
- **State not updating**: Ensure Zustand store methods are called correctly
- **ReactFlow warnings**: Usually related to missing node types in `nodeTypeComponents` map

## File Naming Conventions

- **React Components**: PascalCase (e.g., `WorkflowEditor.tsx`)
- **Utilities/Services**: kebab-case (e.g., `export-service.ts`)
- **Types**: kebab-case (e.g., `workflow.ts`)
- **Stores**: kebab-case with `-store` suffix (e.g., `workflow-store.ts`)

## Key Dependencies

- **reactflow** (11.10.1): Canvas and flow visualization
- **zustand** (4.4.7): State management
- **@radix-ui/react-***: UI component primitives
- **lucide-react**: Icon system
- **react-hook-form** + **zod**: Form handling and validation
- **uuid**: Unique ID generation
- **tailwindcss**: Styling system

## Migration Notes

### Deprecated vs Current
- **Deprecated**: `src/components/workflow/*` and `src/app/workflows/*`
- **Current**: `src/components/workflow-editor/*` and `src/app/workflow-builder/*`
- **Reason**: Architectural improvements, better separation of concerns
- When making changes, always work in `workflow-editor` directory

### Type Definitions
- **Old**: `src/types/workflow.ts`
- **New**: `src/types/workflow/workflow.ts`
- Both exist for backward compatibility during migration

---

# Backend Architecture (FastAPI)

## Backend Development Commands

### Essential Commands (from backend/ directory)
- `python runserver.py` - Start FastAPI dev server on port 8002 with hot reload
- `uvicorn main:app --host 0.0.0.0 --port 8002 --reload` - Alternative direct server start
- `pip install -r requirements.txt` - Install all dependencies
- `./install_redis.sh` - Install and setup Redis for job queues
- `./sqldriver.sh` - Install database drivers for connectors

### Docker Commands
- `docker-compose up` - Run full application stack
- `docker-compose down` - Stop all containers

## Backend Tech Stack

### Core Framework
- **FastAPI** 0.111.0 - Modern async web framework
- **Motor** 3.4.0 - Async MongoDB driver (AsyncIOMotorClient)
- **Redis** 5.0.4 + **RQ** 1.16.2 - Job queue and caching
- **Pydantic** 2.7.1 - Data validation and serialization
- **Uvicorn** 0.29.0 - ASGI server with hot reload

### Database & Storage
- **MongoDB** (Motor) - Primary database for all entities
  - Database: `dataops` - Main application data
  - Database: `nuegpt-db-chat-history` - Chat/AI conversation history
- **Redis** - Caching, sessions, and job queues (RQ)
  - Queue: `WF_nueq` - Workflow execution queue
  - Queue: `J_nueq` - Job execution queue
  - Queue: `JP_nueq` - Job package execution queue
- **PostgreSQL** - Secondary database (asyncpg, psycopg2-binary)

### Multi-Database Connector Support
The platform supports multiple database types through modular connectors:
- **PostgreSQL**: asyncpg, psycopg2-binary
- **MySQL**: PyMySQL, mysql-connector-python
- **Oracle**: oracledb
- **Teradata**: teradatasql
- **Snowflake**: snowflake-connector-python
- **ClickHouse**: clickhouse_driver
- **DuckDB**: duckdb
- **Cloud**: AWS (boto3), GCP (google-cloud-storage, google-cloud-bigquery)

### Authentication & Security
- **JWT**: python-jose, PyJWT - Token-based authentication
- **Encryption**: pycryptodome, cryptography - Data encryption
- **2FA**: pyotp - Two-factor authentication support
- **Password Hashing**: passlib - Secure password storage

## Backend Project Structure

```
backend/
├── main.py                      # FastAPI app initialization, CORS, startup
├── routes.py                    # Central route aggregation from all modules
├── runserver.py                 # Development server launcher
├── requirements.txt             # Python dependencies
├── .env                         # Environment variables (mongo_url, secrets)
├── credentials.json             # Service account credentials (GCP, etc.)
│
├── Connection/                  # Database connection management
│   ├── db.py                    # MongoDB singleton connection (Motor)
│   ├── redisConn.py            # Redis connection + RQ queues
│   ├── pg.py                   # PostgreSQL connection pool
│   └── mongoConn.py            # Legacy MongoDB connection
│
├── Auth/                        # Authentication & Authorization
│   ├── bearer.py               # JWT bearer token validation
│   ├── encrypt_decrypt.py      # Data encryption utilities
│   └── ...
│
├── Workspace/                   # Core workspace entities (modular structure)
│   ├── Workspaces/             # Top-level workspace management
│   │   ├── routes.py           # API endpoints
│   │   ├── service.py          # Business logic
│   │   └── models.py           # Pydantic schemas
│   ├── Folders/                # Folder organization
│   ├── Jobs/                   # Job definitions
│   ├── Workflows/              # Workflow definitions
│   │   ├── routes.py           # Workflow CRUD + version endpoints
│   │   ├── service.py          # Workflow service (extends VersionManager)
│   │   ├── models.py           # WorkflowsSchema, version models
│   │   └── utils.py            # Workflow-specific utilities
│   ├── JobSteps/               # Individual job steps
│   ├── JobPackage/             # Job package bundles
│   └── JobPackageSteps/        # Job package step definitions
│
├── Workflow_Runs/              # Workflow execution tracking
│   ├── routes.py               # Execution endpoints
│   ├── service.py              # Execution orchestration
│   ├── models.py               # Run schemas (JobPackageRunSchema, etc.)
│   └── utils.py                # Execution utilities
│
├── Job_Runs/                   # Job execution tracking
├── JobPackage_Runs/            # Job package execution tracking
│
├── CoreEngine/                 # Execution engine for data processing
│   └── modularize/             # Modular connector implementations
│       ├── amazons3.py         # S3 connector
│       ├── aurorapostgresql.py # Aurora PostgreSQL
│       ├── clickhouse.py       # ClickHouse connector
│       ├── task.py             # Task execution logic
│       └── ...
│
├── Connectors/                 # Connection management
│   ├── routes.py               # Connector CRUD endpoints
│   ├── service.py              # Connection testing, validation
│   └── models.py               # Connector schemas
│
├── Websocket/                  # Real-time communication
│   └── socket.py               # ConnectionManager class
│
├── User_Auth/                  # User authentication routes
├── UserManagement/             # User, group, role management
│   ├── Users/
│   ├── Groups/
│   └── Roles/
│
├── Company/                    # Company-level settings
│   ├── routes.py
│   ├── CorporateCalendar/
│   ├── AccessAndPermissions/
│   ├── TermsAndConditions/
│   └── OrgAnnouncements/
│
├── Billing/                    # Billing and subscription
│   ├── Billing_Information/
│   ├── Invoice_History/
│   └── Budgets_Alerts/
│
├── Settings/                   # Global settings management
│   ├── service.py              # SettingsService (subscription-level)
│   └── models.py               # Setting schemas
│
├── Dashboard/                  # Analytics and metrics
├── Lineage/                    # Data lineage tracking
├── History/                    # Audit logs and user activity
│   ├── user_activity.py        # UserActivity tracking
│   └── utils.py
│
├── Schedulers/                 # Job scheduling (APScheduler)
├── Notification/               # Notification system
├── Email/                      # Email notifications (fastapi_mail)
├── NueGPT/                     # AI/GPT integration
├── Policies/                   # Policy management
│   ├── policy_group/
│   ├── policy_action/
│   ├── authorization_type/
│   └── policy/
│
├── versions/                   # Version management system
│   └── version_manger.py       # VersionManager base class
│
├── Utility/                    # Shared utilities
│   ├── utils.py                # Common utility functions
│   ├── constants.py            # ObjectTypeConstants, UserActivityConstants
│   ├── config.py               # Configuration constants
│   └── helper.py               # Helper functions
│
├── Models/                     # Shared Pydantic models
│   └── models.py               # CommonParams, ConnectorDetails, etc.
│
├── Common_APIs/                # Shared API services
│   ├── routes.py
│   ├── service.py              # Common service layer
│   ├── models.py               # CommonParams base class
│   └── logger.py               # Custom logging (getLogger)
│
├── Downloads/                  # File export and download
├── GCS_Manger/                 # Google Cloud Storage management
├── MetaInformation/            # Metadata management
├── Clone/                      # Object cloning functionality
├── Tickets/                    # Support ticket system
├── report/                     # Reporting system
├── Custom_Editor/              # Custom SQL/code editor
└── static/                     # Static file serving
```

## Backend Architecture Patterns

### Modular Route System
All routes are aggregated in `routes.py` and mounted with prefixes:
```python
from fastapi import APIRouter
from Workspace.Workflows import routes as workflow

router = APIRouter()
router.include_router(workflow.router, prefix="/workflows")
```

### Service-Repository Pattern
Each module follows consistent structure:
- **routes.py** - FastAPI endpoints with dependency injection
- **service.py** - Business logic and database operations
- **models.py** - Pydantic request/response schemas
- **utils.py** - Module-specific utilities

### MongoDB Collections Architecture

#### Singleton Connection Pattern
```python
# Connection/db.py
class MongoConnection:
    _instance = None

    def __new__(cls):
        if not cls._instance:
            client = motor.motor_asyncio.AsyncIOMotorClient(mongo_url)
            cls._instance.client = client.dataops
            cls._instance.nuegpt_client = client["nuegpt-db-chat-history"]
        return cls._instance

mongo_db = MongoConnection()
```

#### Key Collections
- `workspace` - Workspace entities (top-level containers)
- `workflow` - Workflow definitions
- `workflowBkp` - Workflow backups
- `workspaceBkp` - Workspace backups
- `jobSteps` - Individual job step configurations
- `jobRun` - Job execution history
- `userActivity` - User activity audit logs
- `users` - User accounts and profiles
- `connectors` - Database/API connection configurations
- `versions` - Version control for workflows/jobs

### Authentication Flow

#### JWT Bearer Token System
```python
# Auth/bearer.py
class JWTBearer(HTTPBearer):
    async def __call__(self, request: Request):
        credentials = await super().__call__(request)
        token = credentials.credentials
        # Validate token and extract user_obj
        return token

# Usage in routes
@router.get("/workflows/")
async def get_workflows(
    dependencies: str = Depends(JWTBearer())
):
    user_obj = await req_user_obj(dependencies, ObjectTypeConstants.WORKFLOW, ["view"])
    # user_obj contains: UserId, subscription_id, user_utc_offset, permissions
```

#### User Object Structure
```python
user_obj = {
    "UserId": "user_123",
    "subscription_id": "sub_456",
    "user_utc_offset": "+05:30",
    "permissions": ["view", "create", "update", "delete"],
    # ... other user properties
}
```

### WebSocket Real-time Communication

#### ConnectionManager Architecture (`Websocket/socket.py`)
```python
class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []
        self.active_activity: List[WebSocket] = []      # Activity feed subscriptions
        self.active_runnow: Dict[str, WebSocket] = {}   # Workflow run subscriptions

    async def connect(websocket: WebSocket)
    async def disconnect(websocket: WebSocket)
    async def broadcast(message)                         # Broadcast to all
    async def broadcast_activity(message)                # Broadcast activity updates
    async def broadcast_run(msgType, message, Id)        # Broadcast to specific run
```

#### WebSocket Message Types
1. **Authorization**: `{"type": "Authorization", "token": "Bearer ..."}`
2. **Heartbeat**: `{"type": "heartbeat"}` → Response: `{"type": "pong"}`
3. **Activity Subscribe**: `{"type": "activity", "event": "subscribe", "token": "..."}`
4. **Workflow Run Subscribe**: `{"type": "workflowRun", "event": "subscribe", "workflow_id": "..."}`
5. **Workflow Run Logger**: `{"type": "workflowRunLogger", "event": "subscribe", "workflow_id": "..."}`

#### WebSocket Connection Flow
```
1. Client connects to WebSocket endpoint
2. Client sends Authorization message with JWT token
3. Server validates token and stores user_obj in websocket
4. Client subscribes to specific channels (activity, workflowRun, etc.)
5. Server pushes real-time updates to subscribed clients
6. Heartbeat messages keep connection alive
7. Server cleans up dead connections automatically
```

### Workflow CRUD API (`Workspace/Workflows/routes.py`)

#### Primary Endpoints
```python
GET    /workflows/                          # List workflows with filters
POST   /workflows/                          # Create workflow
GET    /workflows/{workflowId}              # Get workflow by ID
PUT    /workflows/                          # Update workflow
DELETE /workflows/{workflow_id}             # Delete workflow

GET    /workflows/{workflowId}/parameters   # Get workflow parameters
PUT    /workflows/{workflow_id}/parameters  # Update workflow parameters
```

#### Version Management Endpoints
```python
POST   /workflows/{workflow_id}/check-in    # Create new version (commit)
POST   /workflows/{workflow_id}/check-out   # Create working copy
GET    /workflows/{workflow_id}/versions    # List all versions
GET    /workflows/{workflow_id}/versions/{version_id}  # Get specific version
POST   /workflows/{workflow_id}/versions/{version_id}/restore  # Restore version
POST   /workflows/{workflow_id}/discard     # Discard changes
```

### Workflow Service Architecture (`Workspace/Workflows/service.py`)

#### Class Structure
```python
class Workflows(VersionManager):
    def __init__(self):
        super().__init__("workflow")  # Inherit version management

        self.workflowCollection = mongo_db.get_collection("workflow")
        self.workflowBkpCollection = mongo_db.get_collection("workflowBkp")
        self.workspaceCollection = mongo_db.get_collection("workspace")

    async def getWorkflowsWithFilters(user_obj, **payload)
    async def addWorkflow(payload, user_obj)
    async def updateWorkflow(payload, query_params, user_obj)
    async def deleteWorkflow(payload, user_obj)
    async def get_workflow(user_obj, workflowId, [], query_params)
    async def get_parameters(user_obj, workflowId, query_params)
    async def update_parameters(user_obj, workflow_id, parameters)

    # Version management (inherited from VersionManager)
    async def check_in_workflow(workflow_id, user_obj, payload)
    async def check_out_workflow(workflow_id, user_obj)
    async def get_workflow_versions(workflow_id, user_obj)
    async def restore_workflow_version(workflow_id, version_id, user_obj)
```

### Workflow Model Schema (`Workspace/Workflows/models.py`)

#### WorkflowsSchema (Pydantic Model)
```python
class WorkflowsSchema(CommonParams):
    workspace_id: str
    workspace_name: str
    folder_id: str
    folder_name: str
    job_id: str
    job_name: str
    workflow_id: str
    workflow_name: str
    description: str
    environment: str
    workFlowDiagram: str              # JSON string of node/edge data
    connectionInformation_v1: str     # Connection metadata
    dependencies: str                 # Workflow dependencies
    workflow_dependency: List[str]    # Dependent workflow IDs
    parameters: str                   # Workflow parameters (JSON)
    onFailureNotify: bool
    onSuccessNotify: bool
    notifyEmail: List[str]
    check_in: bool                    # Version control flag
    check_out: bool                   # Version control flag
    tag: str
    tagList: dict
    share: dict = {"users": {}, "groups": {}}  # Sharing permissions
```

### Execution System Architecture

#### Workflow Execution Flow
```
1. User triggers workflow execution (POST /workflowRuns/)
2. WorkflowRun service creates execution record in MongoDB
3. Execution job queued to Redis (redisWFQueue)
4. RQ worker picks up job from queue
5. CoreEngine executes workflow steps sequentially
6. Real-time updates pushed via WebSocket (broadcast_run)
7. Execution logs stored in MongoDB (jobRun collection)
8. Final status updated in workflow run record
```

#### Redis Queue System (`Connection/redisConn.py`)
```python
def getRedisCon():
    redisConn = Redis(host='localhost', port=6379, db=0)
    redisWFQueue = Queue('WF_nueq', connection=redisConn)    # Workflow queue
    redisJobQueue = Queue('J_nueq', connection=redisConn)    # Job queue
    redisJPQueue = Queue('JP_nueq', connection=redisConn)    # Job package queue
    return redisConn, redisWFQueue, redisJobQueue, redisJPQueue
```

#### Background Job Processing
- **RQ Workers**: Process jobs asynchronously from Redis queues
- **Job Types**: Workflow runs, Job runs, Job package runs
- **Monitoring**: Job status tracked in MongoDB, updates pushed via WebSocket

### Version Management System (`versions/version_manger.py`)

#### VersionManager Base Class
All versionable entities (Workflows, Jobs, etc.) inherit from VersionManager:
```python
class VersionManager:
    def __init__(self, object_type: str):
        self.object_type = object_type  # "workflow", "job", etc.
        self.versionsCollection = mongo_db.get_collection("versions")

    async def check_in(object_id, user_obj, payload)
    async def check_out(object_id, user_obj)
    async def get_versions(object_id, user_obj)
    async def get_version(object_id, version_id, user_obj)
    async def restore_version(object_id, version_id, user_obj)
    async def discard_changes(object_id, user_obj)
```

#### Version Control Flow
1. **Check Out**: Creates working copy, sets `check_out: true` flag
2. **Edit**: User makes changes to working copy
3. **Check In**: Creates version record, clears `check_out` flag, creates backup
4. **Version Record**: Stores `current_values`, `previous_values`, `change_type`
5. **Restore**: Loads previous version, creates new version record for rollback

### Global Settings System (`Settings/service.py`)

#### Settings Management
```python
class GlobalSettingsService:
    async def get_all_settings(user_obj)
    async def get_settings_summary(user_obj)
    async def get_setting(setting_key, user_obj)
    async def update_setting(setting_key, enabled, user_obj)
```

#### Key Settings
- `version_enabled` - Enable/disable version management
- `lineage_enabled` - Enable/disable data lineage tracking
- Settings stored per subscription_id in MongoDB

### User Activity Tracking (`History/user_activity.py`)

#### Activity Logging
All CRUD operations automatically log user activity:
```python
class UserActivity:
    async def log_activity(
        user_obj,
        action: str,          # "Created", "Updated", "Deleted", "Viewed"
        object_type: str,     # "Workflow", "Job", "Connector"
        object_id: str,
        object_name: str,
        previous_data: dict = None,
        current_data: dict = None
    )
```

#### Activity Collection Schema
```python
{
    "histId": str,
    "UserId": str,
    "subscription_id": str,
    "action": str,              # UserActivityConstants
    "object_type": str,         # ObjectTypeConstants
    "object_id": str,
    "object_name": str,
    "category": str,
    "sub_category": str,
    "created_at": datetime,
    "data": {
        "previous_data": dict,
        "current_data": dict
    }
}
```

### Constants and Configuration (`Utility/constants.py`)

#### ObjectTypeConstants
```python
class ObjectTypeConstants:
    WORKSPACE = "Workspace"
    FOLDER = "Folder"
    JOB = "Job"
    WORKFLOW = "Workflow"
    CONNECTOR = "Connector"
    USER = "User"
    # ... etc
```

#### UserActivityConstants
```python
class UserActivityConstants:
    CREATED = "Created"
    UPDATED = "Updated"
    DELETED = "Deleted"
    VIEWED = "Viewed"
    EXECUTED = "Executed"
    # ... etc
```

### Shared Utilities (`Utility/utils.py`)

#### Key Utility Functions
```python
async def get_filter_query(payload, user_obj, obj_category)
async def get_date_filter_query(payload, user_utc_offset)
async def generateSort(sort_val)
async def getDatesFormat(prefix, date_fields)
async def get_user_details_by_id(user_id)
async def convert_to_user_timezone(dt, user_utc_offset)
async def check_share_permissions(user_obj, share_dict, required_permissions)
async def updateModelObject(collection, filter_query, update_data)
```

### CORS and Middleware Configuration (`main.py`)

#### CORS Setup
```python
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*", "http://localhost:3001"],  # Frontend URL
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
```

#### Static File Serving
```python
app.mount("/static", StaticFiles(directory="./static"), name="static")
```

## Backend-Frontend Integration

### API Communication Flow
```
Frontend (port 3001) → Backend API (port 8002)
├── HTTP REST: CRUD operations on workflows, nodes, connectors
├── WebSocket: Real-time execution updates, activity feed
└── File Upload/Download: Workflow import/export, data files
```

### Request/Response Patterns

#### Creating a Workflow (Frontend → Backend)
```typescript
// Frontend: workflows_ux/src/store/workflow-store.ts
const response = await fetch('http://localhost:8002/workflows/', {
  method: 'POST',
  headers: {
    'Authorization': `Bearer ${token}`,
    'Content-Type': 'application/json'
  },
  body: JSON.stringify({
    workspace_id: "ws_123",
    folder_id: "folder_456",
    job_id: "job_789",
    workflow_name: "My Workflow",
    workFlowDiagram: JSON.stringify({ nodes, edges }),
    connectionInformation_v1: JSON.stringify(metadata),
    // ... other fields
  })
});
```

#### Backend Response Structure
```python
# Backend returns standardized response
{
    "success": true,
    "message": "Workflow created successfully",
    "data": {
        "workflow_id": "wf_abc123",
        "workflow_name": "My Workflow",
        "created_at": "2024-01-01T00:00:00Z",
        # ... other workflow fields
    }
}
```

### WebSocket Integration

#### Frontend WebSocket Connection
```typescript
// Frontend connects to WebSocket
const ws = new WebSocket('ws://localhost:8002/ws');

// Authorize connection
ws.send(JSON.stringify({
  type: 'Authorization',
  token: 'Bearer eyJ0eXAiOiJKV1...'
}));

// Subscribe to workflow run updates
ws.send(JSON.stringify({
  type: 'workflowRun',
  event: 'subscribe',
  workflow_id: 'wf_abc123'
}));

// Listen for updates
ws.onmessage = (event) => {
  const data = JSON.parse(event.data);
  if (data.type === 'workflowRun') {
    // Update execution panel with real-time status
    updateExecutionPanel(data.data);
  }
};
```

#### Backend WebSocket Broadcasting
```python
# Backend broadcasts execution updates
await manager.broadcast_run(
    msgType="workflowRun",
    message={
        "workflow_id": "wf_abc123",
        "status": "running",
        "current_step": "node_5",
        "progress": 60,
        "logs": [{"timestamp": "...", "message": "Processing data..."}]
    },
    Id="wf_abc123"
)
```

### Workflow Execution Integration

#### Execution Trigger (Frontend → Backend)
```
1. User clicks "Execute" in workflow editor
2. Frontend POST /workflowRuns/ with workflow_id
3. Backend creates WorkflowRun record
4. Backend queues job to Redis (redisWFQueue)
5. Backend returns execution_id immediately
6. Frontend subscribes to WebSocket for updates
7. RQ worker executes workflow in background
8. Backend pushes updates via WebSocket
9. Frontend displays real-time progress in ExecutionPanel
```

### Data Synchronization Strategy

#### Workflow Save Flow
```
Frontend (Zustand Store)
    ↓ saveWorkflowToFile()
    ↓ Export to .wflow.json
    ↓ User downloads file

User uploads file
    ↓ Frontend reads file
    ↓ POST /workflows/ with parsed data
    ↓
Backend (MongoDB)
    ↓ Validates schema
    ↓ Stores in workflow collection
    ↓ Returns workflow_id
    ↓
Frontend updates Zustand store
```

## Backend Development Best Practices

### Adding New API Endpoints

1. **Create Module Structure**:
```
NewModule/
├── __init__.py
├── routes.py       # FastAPI router with endpoints
├── service.py      # Business logic and DB operations
├── models.py       # Pydantic request/response schemas
└── utils.py        # Module-specific utilities
```

2. **Define Pydantic Models** (`models.py`):
```python
from pydantic import BaseModel
from Common_APIs.models import CommonParams

class NewEntitySchema(CommonParams):
    entity_id: str
    entity_name: str
    description: str
    # ... fields
```

3. **Implement Service Layer** (`service.py`):
```python
from Connection.db import mongo_db

class NewEntityService:
    def __init__(self):
        self.collection = mongo_db.get_collection("new_entity")

    async def create_entity(self, payload, user_obj):
        # Business logic
        result = await self.collection.insert_one(payload.dict())
        return {"entity_id": str(result.inserted_id)}
```

4. **Define Routes** (`routes.py`):
```python
from fastapi import APIRouter, Depends
from Auth.bearer import JWTBearer, req_user_obj
from Utility.constants import ObjectTypeConstants

router = APIRouter()

@router.post("/", tags=["NewEntity"])
async def create_entity(
    payload: NewEntitySchema,
    dependencies: str = Depends(JWTBearer())
):
    user_obj = await req_user_obj(
        dependencies,
        ObjectTypeConstants.NEW_ENTITY,
        ["create"]
    )
    return await service.create_entity(payload, user_obj)
```

5. **Register Routes** (`routes.py` root):
```python
from NewModule import routes as new_entity
router.include_router(new_entity.router, prefix="/new_entity")
```

### Database Query Patterns

#### Filtering with User Permissions
```python
# Always include user/subscription filters
filter_query = {
    "subscription_id": user_obj["subscription_id"],
    "d_flag": False,  # Exclude soft-deleted items
    "$or": [
        {"created_by": user_obj["UserId"]},
        {f"share.users.{user_obj['UserId']}": {"$exists": True}}
    ]
}

results = await collection.find(filter_query).to_list(None)
```

#### Aggregation with Lookups
```python
pipeline = [
    {"$match": filter_query},
    {
        "$lookup": {
            "from": "users",
            "localField": "UserId",
            "foreignField": "UserId",
            "as": "created_by_user"
        }
    },
    {"$project": projection_dict},
    {"$sort": {"created_at": -1}},
    {"$skip": num_of_rows_to_skip},
    {"$limit": rows}
]

results = await collection.aggregate(pipeline).to_list(None)
```

### Error Handling

#### Standard Error Response
```python
from fastapi import HTTPException, status

# Validation errors
if not workflow_id:
    raise HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail="workflow_id is required"
    )

# Not found errors
workflow = await collection.find_one({"workflow_id": workflow_id})
if not workflow:
    raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail=f"Workflow {workflow_id} not found"
    )

# Permission errors
if not has_permission(user_obj, workflow):
    raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail="Insufficient permissions"
    )
```

### Logging Best Practices

#### Using Custom Logger
```python
from APIs.Common.logger import getLogger

logger = getLogger

class MyService:
    def __init__(self):
        self.logger = logger

    async def my_method(self, payload):
        self.logger.info(f"Processing payload: {payload}")
        try:
            result = await self.process()
            self.logger.info(f"Success: {result}")
            return result
        except Exception as ex:
            self.logger.error(f"Error: {ex}", exc_info=True)
            raise
```

## Backend Testing

### Manual Testing
```bash
# Test API endpoint
curl -X GET "http://localhost:8002/workflows/" \
  -H "Authorization: Bearer YOUR_JWT_TOKEN" \
  -H "Content-Type: application/json"

# Test WebSocket
wscat -c ws://localhost:8002/ws
> {"type": "Authorization", "token": "Bearer YOUR_JWT_TOKEN"}
> {"type": "heartbeat"}
```

### Redis Queue Monitoring
```bash
# Connect to Redis CLI
redis-cli

# Check queue lengths
LLEN rq:queue:WF_nueq
LLEN rq:queue:J_nueq
LLEN rq:queue:JP_nueq

# View failed jobs
LLEN rq:queue:failed
```

## Backend Environment Variables

### Required Environment Variables (`.env`)
```bash
# MongoDB
mongo_url=mongodb://localhost:27017/

# Redis
REDIS_HOST=localhost
REDIS_PORT=6379

# JWT
SECRET_KEY=your-secret-key-here
ALGORITHM=HS256

# Server
HOST=0.0.0.0
PORT=8002
```

### Service Credentials (`credentials.json`)
```json
{
  "type": "service_account",
  "project_id": "your-gcp-project",
  "private_key": "-----BEGIN PRIVATE KEY-----\n...",
  "client_email": "service-account@project.iam.gserviceaccount.com"
}
```

## Backend Port Configuration

- **Backend API**: Port 8002 (configurable in `runserver.py`)
- **Frontend**: Port 3001 (Next.js)
- **MongoDB**: Port 27017 (default)
- **Redis**: Port 6379 (default)
- **PostgreSQL**: Port 5432 (default, if used)

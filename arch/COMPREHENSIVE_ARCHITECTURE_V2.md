# UZO Platform - Comprehensive Architecture V2.0
## Enterprise ETL/ELT/GenAI Workflow Automation Platform

**Document Version**: 2.0
**Last Updated**: 2025-10-13
**Status**: ✅ Production Architecture Specification

---

## Table of Contents
1. [Product Vision & Requirements](#product-vision)
2. [ReactFlow Integration & Node Architecture](#reactflow-architecture)
3. [Individual Node Execution (Dev Preview)](#node-execution)
4. [100+ Connector Architecture](#connector-architecture)
5. [Prefect 3 Orchestration](#prefect-architecture)
6. [Polars Transformation Engine](#polars-architecture)
7. [RBAC & Permission System](#rbac-architecture)
8. [15-Level Lineage Tracking](#lineage-architecture)
9. [Real-time Monitoring & Forecasting](#monitoring-architecture)
10. [Multi-tenant Architecture](#multi-tenant-architecture)
11. [Edition-based Features](#feature-flags)
12. [WebSocket Real-time Updates](#websocket-architecture)
13. [Complete Database Schema](#database-schema)
14. [API Design](#api-design)
15. [Implementation Roadmap](#roadmap)

---

<a name="product-vision"></a>
## 1. Product Vision & Core Requirements

### 1.1 Product Hierarchy

```
Subscription (Tenant)
  └── Workspace (Project Container)
      └── Folder (Organization)
          └── Job (Execution Unit)
              └── Workflow (Visual DAG)
                  └── Node (Task)
                      ├── Connector Node (100+ types)
                      ├── Transform Node (Polars operations)
                      ├── GenAI Node (LLM integrations)
                      ├── Agentic Node (Autonomous actions)
                      └── Control Node (Conditionals, loops)

JobPackage (Job Collection)
  └── Job[]
      └── Workflow[]
```

### 1.2 Core Capabilities

| Capability | Description | Reference Platform |
|------------|-------------|-------------------|
| **Visual Workflow Builder** | ReactFlow canvas with drag-drop nodes | n8n, Zapier |
| **100+ Connectors** | Databases, NoSQL, Files, GenAI, Business Apps | Informatica, Matillion |
| **In-memory Transforms** | Polars DataFrame operations | dbt, Spark |
| **GenAI Integration** | OpenAI, Anthropic, local LLMs | LangChain, n8n AI |
| **Agentic Workflows** | Autonomous decision-making nodes | AutoGPT, BabyAGI |
| **15-level Lineage** | Upstream/downstream table tracking | Collibra, Alation |
| **Real-time Execution** | Live progress, forecasting, alerts | Prefect, Airflow |
| **Dev Preview** | Test individual nodes without full run | Snowflake worksheets |
| **Multi-tenancy** | Isolated tenants, shared infra | Salesforce model |
| **RBAC** | Fine-grained permissions | AWS IAM |
| **Edition Control** | Feature flags per subscription tier | GitHub pricing |

### 1.3 Non-Functional Requirements

- **Performance**: <100ms API response (p95), <2s workflow execution start
- **Scalability**: 10K concurrent users, 100K workflows, 1M+ executions/day
- **Availability**: 99.9% uptime, <5min failover
- **Security**: SOC 2, GDPR, encryption at rest/transit
- **Real-time**: <500ms WebSocket latency, live progress updates

---

<a name="reactflow-architecture"></a>
## 2. ReactFlow Integration & Node Architecture

### 2.1 Why ReactFlow?

| Requirement | ReactFlow | AntV X6 (Old) |
|-------------|-----------|---------------|
| React Native | ✅ First-class | ⚠️ Wrapper |
| TypeScript Support | ✅ Full typing | ⚠️ Limited |
| Performance | ✅ 10K+ nodes | ⚠️ Lag at 1K+ |
| Community | ✅ 20K+ stars | ⚠️ 3K stars |
| Plugin System | ✅ Extensive | ⚠️ Limited |
| Documentation | ✅ Excellent | ⚠️ Chinese-first |

### 2.2 ReactFlow Node Data Structure

#### Frontend (TypeScript)
```typescript
// workflows_ux/src/types/workflow.ts
export interface WorkflowNode extends Node {
  id: string;                    // UUID v4
  type: 'custom-node';           // ReactFlow node type
  position: { x: number; y: number };

  data: {
    // Display
    label: string;               // "Extract PostgreSQL Data"
    icon: string;                // Lucide icon name
    color: string;               // Hex color

    // Classification
    nodeType: NodeCategory;      // connector | transform | genai | agentic | control
    category: string;            // Database | NoSQL | File | GenAI

    // Connector-specific (if nodeType === 'connector')
    connectorId?: string;        // Reference to connector table
    connectorType?: ConnectorType; // postgresql | mysql | s3 | openai

    // Configuration
    config: NodeConfig;          // Node-specific settings
    parameters: Record<string, any>; // User-defined parameters

    // Execution
    status?: 'idle' | 'running' | 'success' | 'error' | 'warning';
    lastRunAt?: string;
    duration?: number;           // milliseconds
    rowsProcessed?: number;

    // Metadata
    description?: string;
    tags?: string[];
    version?: number;
    isLocked?: boolean;          // Prevent edits during execution
  };
}

export interface WorkflowEdge extends Edge {
  id: string;                    // UUID v4
  source: string;                // Source node ID
  target: string;                // Target node ID
  sourceHandle?: string;         // 'output' | 'success' | 'error'
  targetHandle?: string;         // 'input'
  type: 'custom-edge';

  data?: {
    label?: string;              // Edge label
    condition?: string;          // Conditional routing expression
    animated?: boolean;
    color?: string;
  };
}

// Node configuration by type
export type NodeConfig =
  | ConnectorNodeConfig
  | TransformNodeConfig
  | GenAINodeConfig
  | AgenticNodeConfig
  | ControlNodeConfig;

// Example: Database connector node
export interface ConnectorNodeConfig {
  // Connection
  connectorId: string;           // Reference to connectors table
  operation: 'select' | 'insert' | 'update' | 'delete' | 'execute';

  // Query/Action
  query?: string;                // SQL query (with parameters)
  table?: string;                // Target table
  columns?: string[];            // Column selection

  // Execution
  batchSize?: number;            // For batch operations
  timeout?: number;              // Seconds
  retryCount?: number;

  // Output
  outputVariable?: string;       // Store result in workflow context
}

// Example: Polars transform node
export interface TransformNodeConfig {
  operation: 'filter' | 'aggregate' | 'join' | 'pivot' | 'window' | 'custom';

  // Input
  inputVariable: string;         // From previous node output

  // Transform
  expression?: string;           // Polars expression
  columns?: string[];
  groupBy?: string[];
  aggregations?: Record<string, string>; // {column: func}

  // Join (if operation === 'join')
  joinType?: 'inner' | 'left' | 'right' | 'outer';
  leftOn?: string[];
  rightOn?: string[];
  rightVariable?: string;

  // Output
  outputVariable: string;
}

// Example: GenAI node
export interface GenAINodeConfig {
  provider: 'openai' | 'anthropic' | 'cohere' | 'local';
  model: string;                 // gpt-4-turbo, claude-3-opus

  // Prompt
  systemPrompt?: string;
  userPrompt: string;            // Can reference {{variables}}

  // Parameters
  temperature?: number;
  maxTokens?: number;
  topP?: number;

  // Output
  outputVariable: string;
  parseJson?: boolean;           // Attempt JSON parsing
}
```

#### Backend Storage (PostgreSQL JSONB)
```sql
CREATE TABLE workflows (
  workflow_id UUID PRIMARY KEY,
  workspace_id UUID NOT NULL REFERENCES workspaces(workspace_id),
  folder_id UUID NOT NULL REFERENCES folders(folder_id),
  job_id UUID NOT NULL REFERENCES jobs(job_id),
  workflow_name VARCHAR(255) NOT NULL,

  -- ReactFlow diagram (JSONB for flexibility)
  workflow_diagram JSONB NOT NULL DEFAULT '{
    "nodes": [],
    "edges": [],
    "viewport": {"x": 0, "y": 0, "zoom": 1}
  }'::jsonb,

  -- Computed dependencies (for execution order)
  dependencies JSONB GENERATED ALWAYS AS (
    -- Trigger function computes this from workflow_diagram.edges
    compute_workflow_dependencies(workflow_diagram)
  ) STORED,

  -- Execution metadata
  parameters JSONB DEFAULT '{}'::jsonb,
  environment_variables JSONB DEFAULT '{}'::jsonb,

  -- Indexing for search
  GIN index on workflow_diagram for node searches
);

-- Index for node searches
CREATE INDEX idx_workflows_diagram_nodes
ON workflows USING gin((workflow_diagram -> 'nodes'));

-- Index for connector references
CREATE INDEX idx_workflows_connectors
ON workflows USING gin((workflow_diagram -> 'nodes' -> 'data' -> 'connectorId'));
```

### 2.3 Backend API for Workflow CRUD

#### Save Workflow (POST /api/v1/workflows)
```python
# backend/Workspace/Workflows/routes.py
from fastapi import APIRouter, Depends
from pydantic import BaseModel
from typing import List, Dict, Any

class WorkflowNodeDTO(BaseModel):
    id: str
    type: str
    position: Dict[str, float]
    data: Dict[str, Any]

class WorkflowEdgeDTO(BaseModel):
    id: str
    source: str
    target: str
    sourceHandle: str | None
    targetHandle: str | None
    type: str
    data: Dict[str, Any] | None

class WorkflowDiagramDTO(BaseModel):
    nodes: List[WorkflowNodeDTO]
    edges: List[WorkflowEdgeDTO]
    viewport: Dict[str, Any] | None

class CreateWorkflowDTO(BaseModel):
    workspace_id: str
    folder_id: str
    job_id: str
    workflow_name: str
    description: str | None
    workflow_diagram: WorkflowDiagramDTO
    parameters: Dict[str, Any] | None = {}

@router.post("/")
async def create_workflow(
    payload: CreateWorkflowDTO,
    user: User = Depends(get_current_user)
):
    # Validate node references
    await validate_workflow_diagram(payload.workflow_diagram)

    # Validate connector references
    await validate_connector_references(payload.workflow_diagram.nodes)

    # Compute dependency graph
    dependencies = compute_dependencies(
        payload.workflow_diagram.nodes,
        payload.workflow_diagram.edges
    )

    # Store in database
    workflow_id = await db.execute(
        """
        INSERT INTO workflows (
            workspace_id, folder_id, job_id, workflow_name,
            workflow_diagram, parameters, created_by
        ) VALUES ($1, $2, $3, $4, $5, $6, $7)
        RETURNING workflow_id
        """,
        payload.workspace_id,
        payload.folder_id,
        payload.job_id,
        payload.workflow_name,
        payload.workflow_diagram.dict(),  # Stored as JSONB
        payload.parameters,
        user.user_id
    )

    # Track lineage (extract table references from nodes)
    await track_lineage_from_workflow(workflow_id, payload.workflow_diagram.nodes)

    # Log activity
    await log_user_activity(
        user_id=user.user_id,
        action="created",
        object_type="workflow",
        object_id=workflow_id
    )

    # Broadcast to WebSocket
    await websocket_manager.broadcast_to_workspace(
        workspace_id=payload.workspace_id,
        event="workflow_created",
        data={"workflow_id": workflow_id}
    )

    return {
        "success": True,
        "data": {
            "workflow_id": workflow_id,
            "dependencies": dependencies
        }
    }

async def validate_connector_references(nodes: List[WorkflowNodeDTO]):
    """Ensure all referenced connectors exist"""
    connector_ids = [
        node.data.get("connectorId")
        for node in nodes
        if node.data.get("nodeType") == "connector" and node.data.get("connectorId")
    ]

    if connector_ids:
        existing = await db.fetch(
            "SELECT connector_id FROM connectors WHERE connector_id = ANY($1)",
            connector_ids
        )
        existing_ids = {row["connector_id"] for row in existing}

        missing = set(connector_ids) - existing_ids
        if missing:
            raise ValueError(f"Connectors not found: {missing}")

def compute_dependencies(
    nodes: List[WorkflowNodeDTO],
    edges: List[WorkflowEdgeDTO]
) -> Dict[str, Any]:
    """
    Compute execution order using topological sort
    Returns: {
        "adjacency": {"node1": ["node2", "node3"]},
        "execution_order": ["node1", "node2", "node3"],
        "levels": [[node1"], ["node2", "node3"], ...]
    }
    """
    from collections import defaultdict, deque

    # Build adjacency list
    graph = defaultdict(list)
    in_degree = defaultdict(int)

    for node in nodes:
        in_degree[node.id] = 0

    for edge in edges:
        graph[edge.source].append(edge.target)
        in_degree[edge.target] += 1

    # Topological sort (Kahn's algorithm)
    queue = deque([node for node, degree in in_degree.items() if degree == 0])
    execution_order = []
    levels = []

    while queue:
        level = []
        for _ in range(len(queue)):
            node = queue.popleft()
            execution_order.append(node)
            level.append(node)

            for neighbor in graph[node]:
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)

        levels.append(level)

    # Check for cycles
    if len(execution_order) != len(nodes):
        raise ValueError("Workflow contains cycles")

    return {
        "adjacency": dict(graph),
        "execution_order": execution_order,
        "levels": levels,
        "node_count": len(nodes),
        "edge_count": len(edges)
    }
```

### 2.4 Dependency Graph Visualization

Frontend can request dependency graph for visualization:

```python
@router.get("/{workflow_id}/dependencies")
async def get_workflow_dependencies(
    workflow_id: str,
    user: User = Depends(get_current_user)
):
    """
    Returns computed dependency graph for visualization
    Frontend can show critical path, parallel branches, etc.
    """
    workflow = await db.fetchrow(
        "SELECT workflow_diagram, dependencies FROM workflows WHERE workflow_id = $1",
        workflow_id
    )

    return {
        "success": True,
        "data": {
            "dependencies": workflow["dependencies"],
            "critical_path": compute_critical_path(workflow["dependencies"]),
            "parallelizable_nodes": find_parallel_branches(workflow["dependencies"])
        }
    }
```

---

<a name="node-execution"></a>
## 3. Individual Node Execution (Dev Preview)

### 3.1 Use Cases

1. **Test Connector**: Verify database connection works
2. **Preview Data**: See sample rows from a SELECT query
3. **Test Transform**: Validate Polars expression on sample data
4. **Debug GenAI**: Test prompt without full workflow run
5. **Validate File**: Check file exists and is readable

### 3.2 Architecture

```
Frontend (Node Context Menu)
  ↓ Click "Preview Data" on node
  ↓
POST /api/v1/nodes/{node_id}/execute
  ↓
Backend creates ephemeral Prefect flow
  ↓
Execute single node with mocked inputs
  ↓
Stream results via WebSocket
  ↓
Frontend displays in modal
```

### 3.3 API Implementation

```python
# backend/Execution/node_preview.py
from prefect import flow, task
from prefect.task_runners import ThreadPoolTaskRunner
import polars as pl

@router.post("/nodes/{node_id}/execute")
async def execute_single_node(
    node_id: str,
    payload: NodeExecutionPayload,
    user: User = Depends(get_current_user)
):
    """
    Execute a single node for preview/testing
    Does NOT save execution to database
    """
    # Get node from workflow
    node = await get_node_from_workflow(node_id)

    # Validate user has permission
    await validate_node_access(node, user)

    # Create ephemeral Prefect flow
    @flow(name=f"preview-{node_id}", task_runner=ThreadPoolTaskRunner(max_workers=1))
    async def preview_node():
        result = await execute_node_task(node, payload.mock_inputs)
        return result

    # Execute and stream results
    execution_id = f"preview-{uuid4()}"

    # Run in background
    task = asyncio.create_task(
        run_preview_execution(execution_id, preview_node)
    )

    return {
        "success": True,
        "data": {
            "execution_id": execution_id,
            "websocket_channel": f"node-preview:{execution_id}"
        }
    }

async def run_preview_execution(execution_id: str, flow_fn):
    """Execute preview and stream results via WebSocket"""
    try:
        # Start execution
        await websocket_manager.publish(
            channel=f"node-preview:{execution_id}",
            event="execution_started",
            data={"status": "running"}
        )

        # Run flow
        result = await flow_fn()

        # Stream results (chunked if large dataset)
        if isinstance(result, pl.DataFrame):
            # Send schema first
            await websocket_manager.publish(
                channel=f"node-preview:{execution_id}",
                event="schema",
                data={
                    "columns": result.columns,
                    "dtypes": [str(dtype) for dtype in result.dtypes],
                    "shape": result.shape
                }
            )

            # Send data in chunks (1000 rows at a time)
            for i in range(0, len(result), 1000):
                chunk = result[i:i+1000]
                await websocket_manager.publish(
                    channel=f"node-preview:{execution_id}",
                    event="data_chunk",
                    data={
                        "rows": chunk.to_dicts(),
                        "offset": i,
                        "total": len(result)
                    }
                )
        else:
            # Non-DataFrame result (GenAI text, file info, etc.)
            await websocket_manager.publish(
                channel=f"node-preview:{execution_id}",
                event="result",
                data={"value": result}
            )

        # Completion
        await websocket_manager.publish(
            channel=f"node-preview:{execution_id}",
            event="execution_completed",
            data={"status": "success"}
        )

    except Exception as e:
        # Error handling
        await websocket_manager.publish(
            channel=f"node-preview:{execution_id}",
            event="execution_failed",
            data={
                "status": "error",
                "error": str(e),
                "traceback": traceback.format_exc()
            }
        )

@task(name="execute_node", retries=0, timeout_seconds=30)
async def execute_node_task(node: WorkflowNode, mock_inputs: Dict[str, Any]):
    """Execute a single node with mocked inputs"""

    node_type = node.data["nodeType"]
    config = node.data["config"]

    if node_type == "connector":
        return await execute_connector_node(node, mock_inputs)
    elif node_type == "transform":
        return await execute_transform_node(node, mock_inputs)
    elif node_type == "genai":
        return await execute_genai_node(node, mock_inputs)
    elif node_type == "agentic":
        return await execute_agentic_node(node, mock_inputs)
    else:
        raise ValueError(f"Unsupported node type: {node_type}")

async def execute_connector_node(node: WorkflowNode, mock_inputs: Dict):
    """Execute database/API connector node"""
    connector_id = node.data["config"]["connectorId"]
    operation = node.data["config"]["operation"]

    # Get connector credentials (decrypted)
    connector = await get_connector(connector_id)

    if connector.type == "postgresql":
        import asyncpg

        conn = await asyncpg.connect(
            host=connector.config["host"],
            port=connector.config["port"],
            database=connector.config["database"],
            user=connector.config["username"],
            password=decrypt(connector.config["password"])
        )

        query = node.data["config"]["query"]

        # Add LIMIT for preview (safety)
        if operation == "select" and "LIMIT" not in query.upper():
            query = f"{query} LIMIT 100"

        result = await conn.fetch(query)
        await conn.close()

        # Convert to Polars DataFrame
        if result:
            df = pl.DataFrame({
                col: [row[col] for row in result]
                for col in result[0].keys()
            })
            return df
        else:
            return pl.DataFrame()

    # Add support for other connector types...

async def execute_transform_node(node: WorkflowNode, mock_inputs: Dict):
    """Execute Polars transformation"""
    operation = node.data["config"]["operation"]
    input_var = node.data["config"]["inputVariable"]

    # Get input data (from mock or previous node)
    input_df = mock_inputs.get(input_var)
    if not isinstance(input_df, pl.DataFrame):
        raise ValueError(f"Input {input_var} is not a DataFrame")

    if operation == "filter":
        expression = node.data["config"]["expression"]
        return input_df.filter(pl.expr(expression))

    elif operation == "aggregate":
        group_by = node.data["config"]["groupBy"]
        aggregations = node.data["config"]["aggregations"]

        return input_df.group_by(group_by).agg([
            getattr(pl.col(col), func)().alias(f"{col}_{func}")
            for col, func in aggregations.items()
        ])

    elif operation == "join":
        right_var = node.data["config"]["rightVariable"]
        right_df = mock_inputs.get(right_var)

        return input_df.join(
            right_df,
            left_on=node.data["config"]["leftOn"],
            right_on=node.data["config"]["rightOn"],
            how=node.data["config"]["joinType"]
        )

    # Add more operations...
```

### 3.4 Frontend Integration

```typescript
// workflows_ux/src/components/workflow-editor/NodeContextMenu.tsx
const handlePreviewData = async (node: WorkflowNode) => {
  // Open WebSocket for streaming results
  const ws = new WebSocket(`ws://localhost:8002/ws`);

  // Subscribe to preview channel
  ws.onopen = () => {
    ws.send(JSON.stringify({
      type: 'Authorization',
      token: authToken
    }));

    ws.send(JSON.stringify({
      type: 'subscribe',
      channel: `node-preview:${executionId}`
    }));
  };

  // Handle streaming results
  ws.onmessage = (event) => {
    const message = JSON.parse(event.data);

    switch (message.event) {
      case 'execution_started':
        setPreviewStatus('running');
        break;

      case 'schema':
        setPreviewSchema(message.data);
        break;

      case 'data_chunk':
        appendPreviewData(message.data.rows);
        setProgress(message.data.offset / message.data.total);
        break;

      case 'execution_completed':
        setPreviewStatus('success');
        ws.close();
        break;

      case 'execution_failed':
        setPreviewStatus('error');
        setPreviewError(message.data.error);
        ws.close();
        break;
    }
  };

  // Trigger execution
  const response = await fetch(`/api/v1/nodes/${node.id}/execute`, {
    method: 'POST',
    headers: {
      'Authorization': `Bearer ${authToken}`,
      'Content-Type': 'application/json'
    },
    body: JSON.stringify({
      mock_inputs: {
        // Provide sample data if node depends on previous nodes
        'upstream_node_output': sampleDataFrame
      }
    })
  });

  const { execution_id } = await response.json();
  setExecutionId(execution_id);
};
```

---

<a name="connector-architecture"></a>
## 4. 100+ Connector Architecture

### 4.1 Connector Categories

```
Databases (25+)
├── Relational: PostgreSQL, MySQL, SQL Server, Oracle, DB2
├── Cloud DW: Snowflake, Redshift, BigQuery, Synapse
├── Distributed: Cassandra, ScyllaDB, CockroachDB
└── Embedded: SQLite, DuckDB

NoSQL (15+)
├── Document: MongoDB, DocumentDB, Couchbase
├── Key-Value: Redis, DynamoDB, Aerospike
├── Column: HBase, Bigtable
└── Graph: Neo4j, Neptune, ArangoDB

File Storage (10+)
├── Cloud: S3, GCS, Azure Blob, Cloudflare R2
├── Protocol: FTP, SFTP, SMB
└── Format Parsers: CSV, JSON, Parquet, Avro, XML

Message Queues (8+)
├── Kafka, RabbitMQ, Pulsar, SQS, SNS, PubSub

GenAI / LLM (15+)
├── OpenAI: GPT-4, GPT-4-turbo, GPT-3.5
├── Anthropic: Claude 3 (Opus, Sonnet, Haiku)
├── Google: Gemini Pro, PaLM 2
├── Cohere, Mistral, Llama 3
└── Local: Ollama, LM Studio

Agentic Systems (5+)
├── LangChain, LlamaIndex, AutoGPT
├── Custom agent frameworks

Business Applications (40+)
├── CRM: Salesforce, HubSpot, Zoho
├── Marketing: Mailchimp, SendGrid, Twilio
├── Productivity: Gmail, Slack, Teams, Notion
├── ERP: SAP, Oracle ERP, Workday
├── E-commerce: Shopify, Magento, WooCommerce
└── Analytics: Google Analytics, Mixpanel, Amplitude
```

### 4.2 Connector Plugin Architecture

#### Base Connector Interface
```python
# backend/Connectors/base.py
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional
import polars as pl

class BaseConnector(ABC):
    """
    Base class for all connectors
    All connectors must implement these methods
    """

    # Metadata
    connector_type: str              # "postgresql", "s3", "openai"
    category: str                    # "database", "file", "genai"
    display_name: str                # "PostgreSQL"
    icon: str                        # Lucide icon name
    color: str                       # Hex color

    # Capabilities
    supports_read: bool = True
    supports_write: bool = True
    supports_streaming: bool = False
    supports_schema_discovery: bool = False

    def __init__(self, config: Dict[str, Any]):
        """Initialize connector with configuration"""
        self.config = config
        self.validate_config()

    @abstractmethod
    def validate_config(self) -> None:
        """Validate connector configuration"""
        pass

    @abstractmethod
    async def test_connection(self) -> Dict[str, Any]:
        """
        Test if connection works
        Returns: {"success": bool, "message": str, "metadata": dict}
        """
        pass

    @abstractmethod
    async def get_schema(self) -> Dict[str, Any]:
        """
        Discover schema (tables, columns, etc.)
        Returns: {"tables": [...], "views": [...]}
        """
        pass

    @abstractmethod
    async def read(self, **kwargs) -> pl.DataFrame:
        """
        Read data from source
        Returns: Polars DataFrame
        """
        pass

    @abstractmethod
    async def write(self, df: pl.DataFrame, **kwargs) -> Dict[str, Any]:
        """
        Write data to destination
        Returns: {"rows_written": int, "duration_ms": int}
        """
        pass

    async def close(self) -> None:
        """Cleanup resources"""
        pass
```

#### Example: PostgreSQL Connector
```python
# backend/Connectors/plugins/postgresql.py
import asyncpg
import polars as pl
from ..base import BaseConnector

class PostgreSQLConnector(BaseConnector):
    connector_type = "postgresql"
    category = "database"
    display_name = "PostgreSQL"
    icon = "database"
    color = "#336791"

    supports_read = True
    supports_write = True
    supports_streaming = True
    supports_schema_discovery = True

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.pool: Optional[asyncpg.Pool] = None

    def validate_config(self) -> None:
        required = ["host", "port", "database", "username", "password"]
        missing = [k for k in required if k not in self.config]
        if missing:
            raise ValueError(f"Missing required config: {missing}")

    async def test_connection(self) -> Dict[str, Any]:
        try:
            conn = await asyncpg.connect(
                host=self.config["host"],
                port=self.config["port"],
                database=self.config["database"],
                user=self.config["username"],
                password=self.config["password"],
                timeout=5
            )

            # Get version
            version = await conn.fetchval("SELECT version()")
            await conn.close()

            return {
                "success": True,
                "message": "Connection successful",
                "metadata": {
                    "version": version,
                    "host": self.config["host"],
                    "database": self.config["database"]
                }
            }
        except Exception as e:
            return {
                "success": False,
                "message": str(e),
                "metadata": {}
            }

    async def get_schema(self) -> Dict[str, Any]:
        """Discover all tables and columns"""
        conn = await self._get_connection()

        # Get tables
        tables = await conn.fetch("""
            SELECT
                table_schema,
                table_name,
                table_type
            FROM information_schema.tables
            WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
            ORDER BY table_schema, table_name
        """)

        # Get columns for each table
        schema = {}
        for table in tables:
            schema_name = table["table_schema"]
            table_name = table["table_name"]

            columns = await conn.fetch("""
                SELECT
                    column_name,
                    data_type,
                    is_nullable,
                    column_default
                FROM information_schema.columns
                WHERE table_schema = $1 AND table_name = $2
                ORDER BY ordinal_position
            """, schema_name, table_name)

            schema[f"{schema_name}.{table_name}"] = {
                "type": table["table_type"],
                "columns": [
                    {
                        "name": col["column_name"],
                        "type": col["data_type"],
                        "nullable": col["is_nullable"] == "YES",
                        "default": col["column_default"]
                    }
                    for col in columns
                ]
            }

        return {"schema": schema}

    async def read(
        self,
        query: Optional[str] = None,
        table: Optional[str] = None,
        columns: Optional[List[str]] = None,
        filters: Optional[Dict[str, Any]] = None,
        limit: Optional[int] = None,
        **kwargs
    ) -> pl.DataFrame:
        """Read data from PostgreSQL"""
        conn = await self._get_connection()

        if query:
            # Custom query
            sql = query
        else:
            # Build query from parameters
            cols = ", ".join(columns) if columns else "*"
            sql = f"SELECT {cols} FROM {table}"

            if filters:
                where_clauses = [f"{k} = ${i+1}" for i, k in enumerate(filters.keys())]
                sql += " WHERE " + " AND ".join(where_clauses)

            if limit:
                sql += f" LIMIT {limit}"

        # Execute query
        if filters:
            result = await conn.fetch(sql, *filters.values())
        else:
            result = await conn.fetch(sql)

        # Convert to Polars DataFrame
        if result:
            df = pl.DataFrame({
                col: [row[col] for row in result]
                for col in result[0].keys()
            })
            return df
        else:
            return pl.DataFrame()

    async def write(
        self,
        df: pl.DataFrame,
        table: str,
        mode: str = "append",  # append | replace | update
        **kwargs
    ) -> Dict[str, Any]:
        """Write data to PostgreSQL"""
        conn = await self._get_connection()

        if mode == "replace":
            await conn.execute(f"TRUNCATE TABLE {table}")

        # Batch insert
        columns = df.columns
        placeholders = ", ".join([f"${i+1}" for i in range(len(columns))])
        sql = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})"

        rows = df.iter_rows()
        batch_size = 1000
        total_rows = 0

        async with conn.transaction():
            batch = []
            for row in rows:
                batch.append(row)
                if len(batch) >= batch_size:
                    await conn.executemany(sql, batch)
                    total_rows += len(batch)
                    batch = []

            if batch:
                await conn.executemany(sql, batch)
                total_rows += len(batch)

        return {
            "rows_written": total_rows,
            "table": table,
            "mode": mode
        }

    async def _get_connection(self) -> asyncpg.Connection:
        """Get database connection from pool"""
        if not self.pool:
            self.pool = await asyncpg.create_pool(
                host=self.config["host"],
                port=self.config["port"],
                database=self.config["database"],
                user=self.config["username"],
                password=self.config["password"],
                min_size=1,
                max_size=10
            )
        return await self.pool.acquire()

    async def close(self) -> None:
        if self.pool:
            await self.pool.close()
```

#### Example: OpenAI Connector
```python
# backend/Connectors/plugins/openai_connector.py
import openai
import polars as pl
from ..base import BaseConnector

class OpenAIConnector(BaseConnector):
    connector_type = "openai"
    category = "genai"
    display_name = "OpenAI"
    icon = "sparkles"
    color = "#10A37F"

    supports_read = True
    supports_write = False  # Cannot write to LLM
    supports_streaming = True

    def validate_config(self) -> None:
        if "api_key" not in self.config:
            raise ValueError("OpenAI API key required")

    async def test_connection(self) -> Dict[str, Any]:
        try:
            client = openai.AsyncOpenAI(api_key=self.config["api_key"])
            models = await client.models.list()

            return {
                "success": True,
                "message": "API key valid",
                "metadata": {
                    "available_models": [m.id for m in models.data[:5]]
                }
            }
        except Exception as e:
            return {
                "success": False,
                "message": str(e),
                "metadata": {}
            }

    async def get_schema(self) -> Dict[str, Any]:
        """List available models"""
        client = openai.AsyncOpenAI(api_key=self.config["api_key"])
        models = await client.models.list()

        return {
            "models": [
                {
                    "id": model.id,
                    "created": model.created,
                    "owned_by": model.owned_by
                }
                for model in models.data
            ]
        }

    async def read(
        self,
        model: str = "gpt-4-turbo",
        prompt: str = "",
        system_prompt: Optional[str] = None,
        temperature: float = 0.7,
        max_tokens: int = 1000,
        **kwargs
    ) -> str:
        """Generate completion"""
        client = openai.AsyncOpenAI(api_key=self.config["api_key"])

        messages = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": prompt})

        response = await client.chat.completions.create(
            model=model,
            messages=messages,
            temperature=temperature,
            max_tokens=max_tokens
        )

        return response.choices[0].message.content

    async def write(self, df: pl.DataFrame, **kwargs):
        raise NotImplementedError("Cannot write to OpenAI")
```

### 4.3 Connector Registry

```python
# backend/Connectors/registry.py
from typing import Dict, Type
from .base import BaseConnector
from .plugins import *

class ConnectorRegistry:
    """
    Registry for all connector plugins
    Automatically discovers and registers connectors
    """

    _connectors: Dict[str, Type[BaseConnector]] = {}

    @classmethod
    def register(cls, connector_class: Type[BaseConnector]):
        """Register a connector plugin"""
        cls._connectors[connector_class.connector_type] = connector_class

    @classmethod
    def get(cls, connector_type: str) -> Type[BaseConnector]:
        """Get connector class by type"""
        if connector_type not in cls._connectors:
            raise ValueError(f"Unknown connector type: {connector_type}")
        return cls._connectors[connector_type]

    @classmethod
    def list_all(cls) -> List[Dict[str, Any]]:
        """List all available connectors"""
        return [
            {
                "type": conn.connector_type,
                "category": conn.category,
                "display_name": conn.display_name,
                "icon": conn.icon,
                "color": conn.color,
                "capabilities": {
                    "read": conn.supports_read,
                    "write": conn.supports_write,
                    "streaming": conn.supports_streaming,
                    "schema_discovery": conn.supports_schema_discovery
                }
            }
            for conn in cls._connectors.values()
        ]

# Auto-register all connectors
ConnectorRegistry.register(PostgreSQLConnector)
ConnectorRegistry.register(MySQLConnector)
ConnectorRegistry.register(S3Connector)
ConnectorRegistry.register(OpenAIConnector)
# ... register all 100+ connectors
```

### 4.4 Connector API

```python
# backend/Connectors/routes.py

@router.get("/plugins")
async def list_connector_plugins():
    """List all available connector types"""
    return {
        "success": True,
        "data": {
            "connectors": ConnectorRegistry.list_all(),
            "total": len(ConnectorRegistry._connectors)
        }
    }

@router.post("/test")
async def test_connector(
    payload: TestConnectorPayload,
    user: User = Depends(get_current_user)
):
    """Test connector configuration without saving"""
    connector_class = ConnectorRegistry.get(payload.connector_type)
    connector = connector_class(payload.config)

    result = await connector.test_connection()
    await connector.close()

    return {"success": True, "data": result}

@router.post("/{connector_id}/schema")
async def discover_schema(
    connector_id: str,
    user: User = Depends(get_current_user)
):
    """Discover schema from connector"""
    # Get connector from database
    connector_record = await db.fetchrow(
        "SELECT * FROM connectors WHERE connector_id = $1",
        connector_id
    )

    # Decrypt config
    config = decrypt_connector_config(connector_record["connection_config_encrypted"])

    # Instantiate connector
    connector_class = ConnectorRegistry.get(connector_record["connector_type"])
    connector = connector_class(config)

    # Discover schema
    schema = await connector.get_schema()
    await connector.close()

    return {"success": True, "data": schema}
```

---

This document is getting quite large. Should I:
1. Continue with the remaining sections (Prefect 3, Polars, RBAC, Lineage, etc.) in this file?
2. Or split into multiple focused documents?

Let me know and I'll complete the comprehensive architecture documentation!
# Operations Guide - Local/VM Deployment

Complete guide for running all services locally or on a virtual machine.

> **Note:** For Docker deployment, see `operations_guide_docker.md`  
> For Kubernetes deployment, see `operations_guide_kubernetes.md`

---

## Table of Contents

1. [Prerequisites](#1-prerequisites)
2. [Running FastAPI Server](#2-running-fastapi-server)
3. [Running Prefect Server & Worker](#3-running-prefect-server--worker)
4. [Running OPA/Rego](#4-running-oparego)
5. [Tenant Management & Access Control](#5-tenant-management--access-control)
6. [Troubleshooting Guide](#6-troubleshooting-guide)

---

## 1. Prerequisites

### System Requirements

- **Python 3.11+** (preferably 3.11.7 or later)
- **PostgreSQL 14+** (running locally or remote)
- **Node.js 18+** (for OPA installation via npm, optional)
- **Git** (for cloning and updates)

### Required Installations

```bash
# Python and pip
python3 --version  # Should be 3.11+
pip3 --version

# PostgreSQL
psql --version

# Optional: Virtual environment
python3 -m venv venv
source venv/bin/activate

# Install Python dependencies
pip install -r requirements.txt
```

### Database Setup

```bash
# Start PostgreSQL service
# macOS (with Homebrew)
brew services start postgresql

# Linux (systemd)
sudo systemctl start postgresql
sudo systemctl enable postgresql

# Create database and user
psql postgres -c "CREATE DATABASE nue_backend;"
psql postgres -c "CREATE USER nue_user WITH PASSWORD 'your_password';"
psql postgres -c "GRANT ALL PRIVILEGES ON DATABASE nue_backend TO nue_user;"

# Run migrations
psql $POSTGRES_DSN -f backend/migrations/002_workflow_execution_tracking.sql
```

### Environment Configuration

```bash
# Copy and edit environment file
cp .env.example .env
nano .env

# Key variables to set:
# POSTGRES_DSN=postgresql://nue_user:your_password@localhost:5432/nue_backend
# SECRET_KEY=your-secret-key
# OPENAI_API_KEY=sk-...
# ANTHROPIC_API_KEY=sk-ant-...
```

---

## 2. Running FastAPI Server

### Start the API Server

```bash
# Navigate to project root
cd /Users/meek/Documents/nue_gemini_rf

# Activate virtual environment (if using one)
source venv/bin/activate

# Run the server
python runserver.py
```

**Alternative method using uvicorn directly:**

```bash
uvicorn backend.main:app --host 0.0.0.0 --port 8000 --reload
```

### Configuration

The server reads configuration from `.env` file. Make sure it exists:

```bash
# Copy example if needed
cp .env.example .env

# Edit configuration
nano .env
```

### Verify Server is Running

```bash
# Health check
curl http://localhost:8000/api/v1/health

# View API documentation
open http://localhost:8000/docs
```

### Server Options

```bash
# Production mode (no auto-reload)
python runserver.py --no-reload

# Custom port
python runserver.py --port 8080

# Custom host
python runserver.py --host 0.0.0.0

# With workers (for production)
uvicorn backend.main:app --host 0.0.0.0 --port 8000 --workers 4
```

### Background/Daemon Mode

```bash
# Using nohup
nohup python runserver.py > logs/api.log 2>&1 &

# Using screen
screen -dmS api python runserver.py

# Using tmux
tmux new -d -s api 'python runserver.py'

# Using systemd (create service file)
sudo systemctl start nue-api
```

### Stop the Server

```bash
# If running in foreground: Ctrl+C

# If running in background (find process)
ps aux | grep runserver
kill <PID>

# Or using pkill
pkill -f runserver.py
```

---

## 3. Running Prefect Server & Worker

### Prerequisites

```bash
# Install Prefect
pip install prefect

# Verify installation
prefect version
```

### Database Setup for Prefect

```bash
# Create Prefect database
psql postgres -c "CREATE DATABASE prefect;"
psql postgres -c "GRANT ALL PRIVILEGES ON DATABASE prefect TO nue_user;"

# Set database URL
export PREFECT_API_DATABASE_CONNECTION_URL="postgresql+asyncpg://nue_user:your_password@localhost:5432/prefect"
```

### Manual Prefect Server Setup

```bash
# Install Prefect
pip install prefect

# Set up database (PostgreSQL)
export PREFECT_API_DATABASE_CONNECTION_URL="postgresql+asyncpg://prefect:password@localhost:5433/prefect"

# Start Prefect server
prefect server start --host 0.0.0.0 --port 4200

# In another terminal, start worker
export PREFECT_API_URL="http://localhost:4200/api"
prefect worker start --pool default-pool
```

### Environment Variables for Prefect

```bash
# Set in .env file
PREFECT_API_URL=http://localhost:4200/api
PREFECT_UI_URL=http://localhost:4200

# Or export in terminal
export PREFECT_API_URL="http://localhost:4200/api"
export PREFECT_UI_URL="http://localhost:4200"
```

---

## 4. Running OPA/Rego

### Prerequisites

Install OPA (Open Policy Agent):

```bash
# macOS
brew install opa

# Linux
curl -L -o opa https://openpolicyagent.org/downloads/latest/opa_darwin_amd64
chmod +x opa
sudo mv opa /usr/local/bin/

# Verify installation
opa version
```

### Start OPA Server

```bash
# Basic server (port 8181)
opa run --server

# With specific port
opa run --server --addr :8282

# With policy directory
opa run --server --addr :8181 ./policies

# With bundle
opa run --server --addr :8181 --bundle ./policies

# With logging
opa run --server --addr :8181 --log-level debug

# With decision logs
opa run --server --addr :8181 --set decision_logs.console=true
```

### OPA with Configuration File

Create `opa-config.yaml`:

```yaml
services:
  - name: nue-backend
    url: http://localhost:8000

bundles:
  authz:
    service: nue-backend
    resource: /policies/bundle.tar.gz

decision_logs:
  console: true

plugins:
  envoy_ext_authz_grpc:
    addr: :9191
    path: nue/authz/allow
```

Run OPA with config:

```bash
opa run --server --config-file opa-config.yaml
```

### Load Policies

```bash
# Load policy file
opa run --server ./policies/authz.rego

# Load policy directory
opa run --server ./policies/

# Update policy via API
curl -X PUT http://localhost:8181/v1/policies/authz \
  -H "Content-Type: text/plain" \
  --data-binary @policies/authz.rego
```

### Test Policies

```bash
# Evaluate policy
curl -X POST http://localhost:8181/v1/data/nue/authz/allow \
  -H "Content-Type: application/json" \
  -d '{
    "input": {
      "user": {"role": "admin", "tenant_id": "tenant_abc"},
      "resource": {"type": "workflow", "id": "wf_123"},
      "action": "execute"
    }
  }'

# Test with opa eval
opa eval -d policies/ -i input.json "data.nue.authz.allow"

# Run tests
opa test policies/ -v
```

### Background Mode

```bash
# Using nohup
nohup opa run --server --addr :8181 ./policies > logs/opa.log 2>&1 &

# Using screen
screen -dmS opa opa run --server --addr :8181 ./policies

# Using tmux
tmux new -d -s opa 'opa run --server --addr :8181 ./policies'

# Using systemd (create service file)
sudo systemctl start opa-server
```

### Stop OPA Server

```bash
# If running in foreground: Ctrl+C

# If running in background
ps aux | grep opa
kill <PID>

# Or using pkill
pkill -f "opa run"
```

### Verify OPA is Running

```bash
# Health check
curl http://localhost:8181/health

# List policies
curl http://localhost:8181/v1/policies

# Get data
curl http://localhost:8181/v1/data
```

### OPA Policy Development

**Example Policy Structure:**

```
policies/
├── authz.rego          # Authorization rules
├── authz_test.rego     # Tests
├── data.json           # Test data
└── input.json          # Sample input
```

**Reload policies:**

```bash
# OPA watches for file changes automatically
# Or restart server
pkill opa
opa run --server --addr :8181 ./policies
```

---

## 5. Tenant Management & Access Control

### Tenant Creation Process

#### Step 1: Create New Tenant

```bash
# Create tenant via API
curl -X POST http://localhost:8000/api/v1/tenants \
  -H "Content-Type: application/json" \
  -d '{
    "tenant_name": "AcmeCorp",
    "company_name": "Acme Corporation", 
    "billing_plan": "enterprise",
    "contact_email": "admin@acme.com",
    "settings": {
      "max_workspaces": 50,
      "max_users": 500,
      "features": ["advanced_analytics", "custom_connectors", "sla_support"]
    }
  }'

# Response includes tenant_id
# {
#   "tenant_id": "tenant_acme_001",
#   "tenant_name": "AcmeCorp",
#   "status": "active",
#   "created_at": "2025-11-23T10:00:00Z"
# }
```

#### Step 2: Create Super Admin User

```bash
# Create first admin user for the tenant
TENANT_ID="tenant_acme_001"

curl -X POST http://localhost:8000/api/v1/users \
  -H "Content-Type: application/json" \
  -d '{
    "tenant_id": "'$TENANT_ID'",
    "username": "admin",
    "email": "admin@acme.com",
    "full_name": "System Administrator",
    "password": "SecurePassword123!",
    "roles": ["super_admin", "tenant_admin"],
    "permissions": ["*"]
  }'

# Response includes user details and JWT token
```

### Role-Based Access Control (RBAC)

#### Available Roles Hierarchy

```yaml
# roles.yaml - Role definitions
roles:
  super_admin:
    description: "Full system access across all tenants"
    permissions: ["*"]
    inherits: []
    
  tenant_admin:
    description: "Full access within tenant"
    permissions:
      - "tenant:*"
      - "users:*"
      - "workspaces:*"
      - "billing:read"
    inherits: []
    
  workspace_owner:
    description: "Full control over specific workspace"
    permissions:
      - "workspace:read"
      - "workspace:write"
      - "workspace:delete"
      - "workspace:share"
      - "folders:*"
      - "users:invite"
    inherits: ["workspace_admin"]
    
  workspace_admin:
    description: "Administrative access to workspace"
    permissions:
      - "workspace:read"
      - "workspace:write"
      - "folders:*"
      - "jobs:*"
      - "workflows:*"
      - "connectors:*"
    inherits: ["workspace_member"]
    
  workspace_member:
    description: "Standard workspace access"
    permissions:
      - "workspace:read"
      - "folders:read"
      - "jobs:read"
      - "workflows:read"
      - "workflows:execute"
    inherits: ["user"]
    
  folder_owner:
    description: "Full control over specific folder"
    permissions:
      - "folder:read"
      - "folder:write"
      - "folder:delete"
      - "folder:share"
      - "jobs:*"
      - "workflows:*"
    inherits: ["folder_admin"]
    
  folder_admin:
    description: "Administrative access to folder"
    permissions:
      - "folder:read"
      - "folder:write"
      - "jobs:*"
      - "workflows:*"
    inherits: ["folder_member"]
    
  folder_member:
    description: "Standard folder access"
    permissions:
      - "folder:read"
      - "jobs:read"
      - "workflows:read"
      - "workflows:execute"
    inherits: ["user"]
    
  user:
    description: "Basic authenticated user"
    permissions:
      - "profile:read"
      - "profile:write"
    inherits: []
```

#### Step 3: Create User Groups

```bash
# Authenticate as super admin
TOKEN=$(curl -X POST http://localhost:8000/api/v1/auth/login \
  -H "Content-Type: application/json" \
  -d '{
    "username": "admin",
    "password": "SecurePassword123!",
    "tenant_id": "'$TENANT_ID'"
  }' | jq -r '.access_token')

# Create admin group
curl -X POST http://localhost:8000/api/v1/groups \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "tenant_id": "'$TENANT_ID'",
    "group_name": "Administrators",
    "description": "System administrators with elevated privileges",
    "roles": ["tenant_admin"],
    "permissions": [
      "users:create", "users:read", "users:update", "users:delete",
      "workspaces:create", "workspaces:read", "workspaces:update", "workspaces:delete",
      "groups:create", "groups:read", "groups:update", "groups:delete"
    ]
  }'

# Create data engineers group  
curl -X POST http://localhost:8000/api/v1/groups \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "tenant_id": "'$TENANT_ID'",
    "group_name": "Data Engineers",
    "description": "Users who build and maintain data pipelines",
    "roles": ["workspace_admin"],
    "permissions": [
      "workspaces:read", "workspaces:write",
      "folders:create", "folders:read", "folders:write",
      "jobs:create", "jobs:read", "jobs:write", "jobs:execute",
      "workflows:create", "workflows:read", "workflows:write", "workflows:execute",
      "connectors:create", "connectors:read", "connectors:write"
    ]
  }'

# Create analysts group
curl -X POST http://localhost:8000/api/v1/groups \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "tenant_id": "'$TENANT_ID'",
    "group_name": "Data Analysts",
    "description": "Users who analyze data and run workflows",
    "roles": ["workspace_member"],
    "permissions": [
      "workspaces:read",
      "folders:read",
      "jobs:read", "jobs:execute",
      "workflows:read", "workflows:execute",
      "connectors:read"
    ]
  }'
```

#### Step 4: Create Additional Users

```bash
# Create data engineer user
curl -X POST http://localhost:8000/api/v1/users \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "tenant_id": "'$TENANT_ID'",
    "username": "john.doe",
    "email": "john.doe@acme.com",
    "full_name": "John Doe",
    "password": "Engineer123!",
    "groups": ["Data Engineers"],
    "roles": ["workspace_admin"]
  }'

# Create analyst user
curl -X POST http://localhost:8000/api/v1/users \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "tenant_id": "'$TENANT_ID'",
    "username": "jane.smith",
    "email": "jane.smith@acme.com",
    "full_name": "Jane Smith",
    "password": "Analyst123!",
    "groups": ["Data Analysts"],
    "roles": ["workspace_member"]
  }'
```

### Object Creation and Ownership

#### Step 5: Create Workspace

```bash
# Login as data engineer
ENGINEER_TOKEN=$(curl -X POST http://localhost:8000/api/v1/auth/login \
  -H "Content-Type: application/json" \
  -d '{
    "username": "john.doe",
    "password": "Engineer123!",
    "tenant_id": "'$TENANT_ID'"
  }' | jq -r '.access_token')

# Create workspace
WORKSPACE_ID=$(curl -X POST http://localhost:8000/api/v1/workspaces \
  -H "Authorization: Bearer $ENGINEER_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "tenant_id": "'$TENANT_ID'",
    "workspace_name": "Sales Analytics",
    "description": "Workspace for sales data analysis and reporting",
    "settings": {
      "default_permissions": {
        "members": ["read", "execute"],
        "guests": ["read"]
      },
      "auto_approval_workflows": false,
      "data_retention_days": 365
    }
  }' | jq -r '.workspace_id')

echo "Created workspace: $WORKSPACE_ID"
```

#### Step 6: Create Folders

```bash
# Create main folders in workspace
ETL_FOLDER_ID=$(curl -X POST http://localhost:8000/api/v1/folders \
  -H "Authorization: Bearer $ENGINEER_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "tenant_id": "'$TENANT_ID'",
    "workspace_id": "'$WORKSPACE_ID'",
    "folder_name": "ETL Pipelines",
    "description": "Extract, Transform, Load workflows",
    "parent_folder_id": null
  }' | jq -r '.folder_id')

REPORTS_FOLDER_ID=$(curl -X POST http://localhost:8000/api/v1/folders \
  -H "Authorization: Bearer $ENGINEER_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "tenant_id": "'$TENANT_ID'",
    "workspace_id": "'$WORKSPACE_ID'",
    "folder_name": "Reports",
    "description": "Analysis and reporting workflows",
    "parent_folder_id": null
  }' | jq -r '.folder_id')
```

#### Step 7: Create Connectors

```bash
# Create database connector
DB_CONNECTOR_ID=$(curl -X POST http://localhost:8000/api/v1/connectors \
  -H "Authorization: Bearer $ENGINEER_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "tenant_id": "'$TENANT_ID'",
    "workspace_id": "'$WORKSPACE_ID'",
    "connector_name": "Sales Database",
    "connector_type": "postgresql",
    "connector_category": "database",
    "description": "Production sales database",
    "connection_detail": {
      "host": "sales-db.acme.com",
      "port": 5432,
      "database": "sales",
      "username": "readonly_user"
    },
    "status": "active"
  }' | jq -r '.connector_id')

# Create S3 connector for outputs
S3_CONNECTOR_ID=$(curl -X POST http://localhost:8000/api/v1/connectors \
  -H "Authorization: Bearer $ENGINEER_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "tenant_id": "'$TENANT_ID'",
    "workspace_id": "'$WORKSPACE_ID'",
    "connector_name": "Reports S3 Bucket",
    "connector_type": "s3",
    "connector_category": "storage",
    "description": "S3 bucket for report outputs",
    "connection_detail": {
      "bucket": "acme-reports",
      "region": "us-east-1",
      "prefix": "sales-analytics/"
    },
    "status": "active"
  }' | jq -r '.connector_id')
```

#### Step 8: Create Jobs and Workflows

```bash
# Create ETL job
ETL_JOB_ID=$(curl -X POST http://localhost:8000/api/v1/jobs \
  -H "Authorization: Bearer $ENGINEER_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "tenant_id": "'$TENANT_ID'",
    "workspace_id": "'$WORKSPACE_ID'",
    "folder_id": "'$ETL_FOLDER_ID'",
    "job_name": "Daily Sales ETL",
    "description": "Extract daily sales data and transform for analysis",
    "job_type": "extract_transform_load",
    "source_connector_id": "'$DB_CONNECTOR_ID'",
    "target_connector_id": "'$S3_CONNECTOR_ID'",
    "schedule": {
      "type": "cron",
      "expression": "0 2 * * *",
      "timezone": "UTC"
    }
  }' | jq -r '.job_id')

# Create workflow containing the job
WORKFLOW_ID=$(curl -X POST http://localhost:8000/api/v1/workflows \
  -H "Authorization: Bearer $ENGINEER_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "tenant_id": "'$TENANT_ID'",
    "workspace_id": "'$WORKSPACE_ID'",
    "folder_id": "'$ETL_FOLDER_ID'",
    "workflow_name": "Sales Data Pipeline",
    "description": "Complete pipeline for sales data processing",
    "jobs": ["'$ETL_JOB_ID'"],
    "dependencies": [],
    "schedule": {
      "type": "cron",
      "expression": "0 3 * * *",
      "timezone": "UTC"
    }
  }' | jq -r '.workflow_id')
```

### Access Control and Sharing

#### Step 9: Grant Workspace Access to Users

```bash
# Grant analyst access to workspace
curl -X POST http://localhost:8000/api/v1/workspaces/$WORKSPACE_ID/permissions \
  -H "Authorization: Bearer $ENGINEER_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": "jane.smith",
    "permissions": ["read", "execute"],
    "role": "workspace_member",
    "granted_by": "john.doe",
    "expires_at": null,
    "reason": "Analyst needs access to run sales reports"
  }'

# Grant folder-specific permissions
curl -X POST http://localhost:8000/api/v1/folders/$REPORTS_FOLDER_ID/permissions \
  -H "Authorization: Bearer $ENGINEER_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": "jane.smith",
    "permissions": ["read", "write", "execute"],
    "role": "folder_admin",
    "granted_by": "john.doe",
    "expires_at": null,
    "reason": "Analyst responsible for maintaining reports folder"
  }'
```

#### Step 10: Share Specific Objects

```bash
# Share workflow with specific user
curl -X POST http://localhost:8000/api/v1/workflows/$WORKFLOW_ID/share \
  -H "Authorization: Bearer $ENGINEER_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "share_with": {
      "type": "user",
      "user_id": "jane.smith"
    },
    "permissions": ["read", "execute"],
    "message": "Please review this workflow for the new sales report requirements",
    "expires_at": "2025-12-31T23:59:59Z",
    "notify": true
  }'

# Share with group
curl -X POST http://localhost:8000/api/v1/connectors/$DB_CONNECTOR_ID/share \
  -H "Authorization: Bearer $ENGINEER_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "share_with": {
      "type": "group",
      "group_name": "Data Analysts"
    },
    "permissions": ["read"],
    "message": "Read-only access to sales database connector",
    "expires_at": null,
    "notify": true
  }'
```

### Permission Management

#### Grant Object-Specific Access

```bash
# Grant execute permission on specific job
curl -X POST http://localhost:8000/api/v1/jobs/$ETL_JOB_ID/permissions \
  -H "Authorization: Bearer $ENGINEER_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "principal": {
      "type": "user",
      "id": "jane.smith"
    },
    "permissions": ["execute"],
    "conditions": {
      "time_range": {
        "start": "09:00",
        "end": "17:00"
      },
      "weekdays_only": true
    },
    "granted_by": "john.doe",
    "reason": "Analyst needs to run ETL for ad-hoc reports during business hours"
  }'
```

#### Revoke Access

```bash
# Revoke user access from workspace
curl -X DELETE http://localhost:8000/api/v1/workspaces/$WORKSPACE_ID/permissions/jane.smith \
  -H "Authorization: Bearer $ENGINEER_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "reason": "User transferred to different department",
    "revoked_by": "john.doe"
  }'
```

### Access History and Audit

#### View Access History

```bash
# Get workspace access history
curl -X GET "http://localhost:8000/api/v1/workspaces/$WORKSPACE_ID/audit?limit=50&offset=0" \
  -H "Authorization: Bearer $TOKEN"

# Get user activity history
curl -X GET "http://localhost:8000/api/v1/users/jane.smith/activity?from_date=2025-11-01&to_date=2025-11-23" \
  -H "Authorization: Bearer $TOKEN"

# Get object access history
curl -X GET "http://localhost:8000/api/v1/workflows/$WORKFLOW_ID/access_log?limit=100" \
  -H "Authorization: Bearer $TOKEN"

# Get permission changes history
curl -X GET "http://localhost:8000/api/v1/permissions/history?object_type=workflow&object_id=$WORKFLOW_ID" \
  -H "Authorization: Bearer $TOKEN"
```

#### Security Reports

```bash
# Generate access report
curl -X POST http://localhost:8000/api/v1/reports/access \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "tenant_id": "'$TENANT_ID'",
    "report_type": "comprehensive",
    "scope": {
      "workspaces": ["'$WORKSPACE_ID'"],
      "date_range": {
        "from": "2025-11-01",
        "to": "2025-11-23"
      }
    },
    "include": [
      "user_permissions",
      "failed_access_attempts",
      "permission_changes",
      "shared_objects"
    ]
  }'

# Generate compliance report
curl -X POST http://localhost:8000/api/v1/reports/compliance \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "tenant_id": "'$TENANT_ID'",
    "compliance_framework": "SOX",
    "scope": "all_workspaces"
  }'
```

### Policy Examples

#### OPA Policy for Access Control

```rego
# policies/authz.rego
package nue.authz

import future.keywords.if
import future.keywords.in

# Allow if user has super admin role
allow if {
    "super_admin" in input.user.roles
}

# Allow workspace access if user is workspace owner/admin
allow if {
    input.resource.type == "workspace"
    input.user.tenant_id == input.resource.tenant_id
    workspace_permission
}

workspace_permission if {
    input.resource.workspace_id in input.user.workspaces
    required_permission in input.user.workspaces[input.resource.workspace_id].permissions
}

required_permission := sprintf("%s:%s", [input.resource.type, input.action])

# Allow folder access based on inheritance
allow if {
    input.resource.type == "folder"
    folder_access
}

folder_access if {
    # Check direct folder permissions
    input.resource.folder_id in input.user.folders
    required_permission in input.user.folders[input.resource.folder_id].permissions
}

folder_access if {
    # Check workspace permissions (folders inherit)
    input.resource.workspace_id in input.user.workspaces
    required_permission in input.user.workspaces[input.resource.workspace_id].permissions
}

# Time-based access control
allow if {
    input.resource.type == "job"
    input.action == "execute"
    job_time_access
}

job_time_access if {
    input.resource.job_id in input.user.job_permissions
    permissions := input.user.job_permissions[input.resource.job_id]
    
    # Check if current time is within allowed range
    current_hour := time.parse_rfc3339_ns(time.now_ns()).hour
    current_hour >= permissions.time_range.start_hour
    current_hour <= permissions.time_range.end_hour
    
    # Check if current day is allowed
    current_weekday := time.parse_rfc3339_ns(time.now_ns()).weekday
    permissions.weekdays_only == false
}

job_time_access if {
    input.resource.job_id in input.user.job_permissions
    permissions := input.user.job_permissions[input.resource.job_id]
    
    current_hour := time.parse_rfc3339_ns(time.now_ns()).hour
    current_hour >= permissions.time_range.start_hour
    current_hour <= permissions.time_range.end_hour
    
    current_weekday := time.parse_rfc3339_ns(time.now_ns()).weekday
    permissions.weekdays_only == true
    current_weekday >= 1  # Monday
    current_weekday <= 5  # Friday
}

# Deny by default
default allow := false
```

---

## 6. Troubleshooting Guide

### FastAPI Server Issues

#### Server Won't Start

**Issue: Port already in use**
```bash
# Find process using port 8000
lsof -i :8000
# Or
netstat -an | grep 8000

# Kill the process
kill -9 <PID>

# Start server on different port
python runserver.py --port 8080
```

**Issue: Module not found errors**
```bash
# Check Python path
python -c "import sys; print('\n'.join(sys.path))"

# Verify backend module
python -c "import backend; print(backend.__file__)"

# Reinstall dependencies
pip install -r requirements.txt

# Check virtual environment
which python
```

**Issue: Database connection failed**
```bash
# Test database connection
psql $POSTGRES_DSN

# Check environment variable
echo $POSTGRES_DSN

# Verify in .env file
cat .env | grep POSTGRES_DSN

# Test with Python
python -c "
from backend.core.config import get_settings
print(get_settings().POSTGRES_DSN)
"
```

**Issue: Permission errors**
```bash
# Check file permissions
ls -la backend/
ls -la logs/

# Fix permissions
chmod +x runserver.py
sudo chown -R $USER:$USER logs/

# Create missing directories
mkdir -p logs
mkdir -p tmp
```

#### API Endpoints Not Working

**Issue: 404 Not Found**
```bash
# Check available routes
curl http://localhost:8000/docs

# Verify route registration
python -c "
from backend.api.v1.routes import router
print([route.path for route in router.routes])
"

# Check logs
tail -f logs/backend.log
```

**Issue: 500 Internal Server Error**
```bash
# Enable debug mode in .env
LOG_LEVEL=DEBUG

# Restart server and check logs
python runserver.py

# Test specific endpoint
curl -v http://localhost:8000/api/v1/workflows?tenant_id=test
```

**Issue: Authentication failing**
```bash
# Generate new token
python -c "
from backend.core.security import create_access_token
token = create_access_token({'sub': 'user_123', 'tenant_id': 'tenant_abc', 'role': 'admin'})
print(token)
"

# Test with token
TOKEN="<your_token>"
curl -H "Authorization: Bearer $TOKEN" http://localhost:8000/api/v1/workflows/wf_123/execute
```

### Prefect Issues

#### Prefect Server Won't Start

**Issue: Database connection failed**
```bash
# Check PostgreSQL is running
pg_isready -h localhost -p 5432

# Test database connection
psql $PREFECT_API_DATABASE_CONNECTION_URL -c "SELECT version();"

# Check database permissions
psql postgres -c "GRANT ALL PRIVILEGES ON DATABASE prefect TO nue_user;"
```

**Issue: Port conflicts**
```bash
# Prefect UI port (4200) in use
lsof -i :4200
kill -9 <PID>

# Start on different port
prefect server start --host 0.0.0.0 --port 4201
```

#### Workflow Execution Issues

**Issue: Worker can't connect to server**
```bash
# Check server is running
curl http://localhost:4200/api/health

# Verify API URL
echo $PREFECT_API_URL

# Test connectivity
prefect work-pool ls

# Check firewall settings
telnet localhost 4200
```

**Issue: Work pool creation fails**
```bash
# Create work pool manually
prefect work-pool create default-pool --type process

# List existing pools
prefect work-pool ls

# Check worker logs
prefect worker start --pool default-pool --log-level DEBUG
```

### OPA Issues

#### OPA Won't Start

**Issue: OPA not installed**
```bash
# Verify installation
opa version

# Reinstall
brew reinstall opa  # macOS
# Or download from https://www.openpolicyagent.org/downloads/
```

**Issue: Port 8181 in use**
```bash
# Find process
lsof -i :8181

# Kill process
kill -9 <PID>

# Start OPA on different port
opa run --server --addr :8282
```

**Issue: Policy files not found**
```bash
# Check policy directory exists
ls -la policies/

# Use absolute path
opa run --server --addr :8181 /absolute/path/to/policies

# Or navigate to directory first
cd /Users/meek/Documents/nue_gemini_rf
opa run --server --addr :8181 ./policies
```

#### Policy Issues

**Issue: Policy syntax errors**
```bash
# Validate policy
opa check policies/

# Test policy
opa test policies/ -v

# Format policy
opa fmt -w policies/

# Check specific policy
opa parse policies/authz.rego
```

**Issue: Policy not loading**
```bash
# Check OPA logs
opa run --server --addr :8181 --log-level debug ./policies

# List loaded policies
curl http://localhost:8181/v1/policies

# Reload policy
curl -X PUT http://localhost:8181/v1/policies/authz \
  -H "Content-Type: text/plain" \
  --data-binary @policies/authz.rego
```

**Issue: Policy evaluation failing**
```bash
# Test with opa eval
opa eval -d policies/ -i input.json "data.nue.authz.allow"

# Check input format
cat input.json | jq .

# Test via API with verbose
curl -v -X POST http://localhost:8181/v1/data/nue/authz/allow \
  -H "Content-Type: application/json" \
  -d @input.json

# Enable decision logs
opa run --server --addr :8181 --set decision_logs.console=true ./policies
```

### Database Issues

**Issue: Connection timeout**
```bash
# Check PostgreSQL is running
pg_isready -h localhost -p 5432

# Check connection string
echo $POSTGRES_DSN

# Test connection
psql $POSTGRES_DSN -c "SELECT version();"

# Check firewall/network
telnet localhost 5432
```

**Issue: Migrations not applied**
```bash
# Check current schema
psql $POSTGRES_DSN -c "\dt maple.*"

# Run migrations
psql $POSTGRES_DSN -f backend/migrations/002_workflow_execution_tracking.sql

# Check for errors
psql $POSTGRES_DSN -f backend/migrations/002_workflow_execution_tracking.sql 2>&1 | grep ERROR
```

**Issue: Permission denied**
```bash
# Check user permissions
psql $POSTGRES_DSN -c "SELECT current_user, current_database();"

# Grant permissions
psql $POSTGRES_DSN -c "GRANT ALL ON SCHEMA maple TO your_user;"
psql $POSTGRES_DSN -c "GRANT ALL ON ALL TABLES IN SCHEMA maple TO your_user;"
```

### LLM API Issues

**Issue: OpenAI API calls failing**
```bash
# Verify API key
echo $OPENAI_API_KEY

# Test API key
curl https://api.openai.com/v1/models \
  -H "Authorization: Bearer $OPENAI_API_KEY"

# Check rate limits
curl https://api.openai.com/v1/models \
  -H "Authorization: Bearer $OPENAI_API_KEY" \
  -D - | grep -i rate

# Update in .env
nano .env
# Set: OPENAI_API_KEY=sk-...
```

**Issue: Anthropic API calls failing**
```bash
# Verify API key
echo $ANTHROPIC_API_KEY

# Test API key
curl https://api.anthropic.com/v1/messages \
  -H "x-api-key: $ANTHROPIC_API_KEY" \
  -H "anthropic-version: 2023-06-01" \
  -H "content-type: application/json" \
  -d '{"model":"claude-3-5-sonnet-20241022","max_tokens":10,"messages":[{"role":"user","content":"test"}]}'

# Update in .env
nano .env
# Set: ANTHROPIC_API_KEY=sk-ant-...
```

**Issue: Rate limit exceeded**
```bash
# Check rate limit settings in .env
cat .env | grep LLM_RATE_LIMIT

# Increase retry delays
# Edit backend/core/config.py:
# OPENAI_MAX_RETRIES: int = 5
# OPENAI_TIMEOUT_SECONDS: int = 120

# Implement exponential backoff
# Or reduce concurrent requests
```

### General Troubleshooting

#### Check All Services Status

```bash
# FastAPI
curl http://localhost:8000/api/v1/health

# Prefect
curl http://localhost:4200/api/health

# OPA
curl http://localhost:8181/health

# PostgreSQL (main)
pg_isready -h localhost -p 5432

# PostgreSQL (Prefect)
pg_isready -h localhost -p 5433
```

#### Clean Restart Everything

```bash
# Stop all services
pkill -f runserver.py
pkill -f "prefect server"
pkill -f "prefect worker"
pkill -f "opa run"

# Clear caches/temp files
rm -rf __pycache__
rm -rf backend/**/__pycache__
find . -name "*.pyc" -delete

# Restart PostgreSQL
brew services restart postgresql  # macOS
sudo systemctl restart postgresql  # Linux

# Restart services
python runserver.py &
prefect server start --host 0.0.0.0 --port 4200 &
prefect worker start --pool default-pool &
opa run --server --addr :8181 ./policies &

# Verify all running
ps aux | grep -E "runserver|prefect|opa"
```

#### Environment Issues

```bash
# Check Python version
python --version  # Should be 3.11+

# Check installed packages
pip list | grep -E "fastapi|prefect|openai|anthropic"

# Verify virtual environment
which python
echo $VIRTUAL_ENV

# Reinstall everything
pip install -r requirements.txt --force-reinstall

# Check environment variables
env | grep -E "POSTGRES|OPENAI|ANTHROPIC|PREFECT"
```

---

## Quick Reference Commands

### Start All Services
```bash
# Terminal 1: FastAPI
python runserver.py

# Terminal 2: Prefect Server
prefect server start --host 0.0.0.0 --port 4200

# Terminal 3: Prefect Worker  
prefect worker start --pool default-pool

# Terminal 4: OPA
opa run --server --addr :8181 ./policies
```

### Stop All Services
```bash
pkill -f runserver.py
pkill -f "prefect server"
pkill -f "prefect worker"
pkill -f "opa run"
```

### Check Status
```bash
curl http://localhost:8000/api/v1/health    # FastAPI
curl http://localhost:4200/api/health       # Prefect
curl http://localhost:8181/health           # OPA
pg_isready -h localhost -p 5432             # PostgreSQL
```

### View Logs
```bash
tail -f logs/backend.log                    # FastAPI
tail -f logs/prefect.log                    # Prefect (if redirected)
tail -f logs/opa.log                        # OPA (if redirected)
```

---

## Support

If you encounter issues not covered here:

1. Check service logs for specific error messages
2. Verify all environment variables are set correctly in `.env`
3. Ensure all prerequisites are installed (Python 3.11+, PostgreSQL)
4. Review `SETUP.md` for initial configuration steps
5. Check `IMPLEMENTATION_COMPLETE.md` for feature details

**For other deployment methods:**
- Docker: See `operations_guide_docker.md`
- Kubernetes: See `operations_guide_kubernetes.md`

**Common Resolution Steps:**
1. Restart the affected service
2. Check and fix configuration
3. Clear caches and restart
4. Verify network connectivity  
5. Check resource usage (CPU, memory, disk)

---

**Last Updated:** November 23, 2025

# deployment_setup.py
"""
Deployment setup and configuration for Prefect workflow orchestration
"""

import os
import asyncio
from pathlib import Path
import yaml
from typing import Dict, Any

# Environment configuration
class Config:
    """Configuration class for the workflow orchestration system"""
    
    # Database Configuration
    POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
    POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
    POSTGRES_DB = os.getenv("POSTGRES_DB", "neondb")
    POSTGRES_USER = os.getenv("POSTGRES_USER", "your_username")
    POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "your_password")
    
    # MongoDB Configuration
    MONGODB_URL = os.getenv("MONGODB_URL", "mongodb://localhost:27017")
    MONGODB_DATABASE = os.getenv("MONGODB_DATABASE", "your_workflow_db")
    
    # Prefect Configuration
    PREFECT_API_URL = os.getenv("PREFECT_API_URL", "http://localhost:4200/api")
    PREFECT_WORK_POOL = os.getenv("PREFECT_WORK_POOL", "default-agent-pool")
    
    # Application Configuration
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
    ENABLE_PREFECT_BY_DEFAULT = os.getenv("ENABLE_PREFECT_BY_DEFAULT", "true").lower() == "true"

    @classmethod
    def get_postgres_connection_string(cls) -> str:
        return f"postgresql://{cls.POSTGRES_USER}:{cls.POSTGRES_PASSWORD}@{cls.POSTGRES_HOST}:{cls.POSTGRES_PORT}/{cls.POSTGRES_DB}"


# Docker Compose configuration
DOCKER_COMPOSE_CONTENT = """
version: '3.8'

services:
  # Prefect Server
  prefect-server:
    image: prefecthq/prefect:2.15-python3.11
    command: prefect server start --host 0.0.0.0 --port 4200
    ports:
      - "4200:4200"
    environment:
      - PREFECT_UI_URL=http://localhost:4200/ui
      - PREFECT_API_URL=http://localhost:4200/api
      - PREFECT_SERVER_API_HOST=0.0.0.0
    volumes:
      - prefect_data:/root/.prefect
    networks:
      - workflow_network

  # Prefect Agent/Worker
  prefect-worker:
    image: prefecthq/prefect:2.15-python3.11
    command: prefect worker start --pool default-agent-pool
    environment:
      - PREFECT_API_URL=http://prefect-server:4200/api
    volumes:
      - ./:/app
      - prefect_data:/root/.prefect
    working_dir: /app
    depends_on:
      - prefect-server
    networks:
      - workflow_network

  # PostgreSQL Database
  postgres:
    image: postgres:15
    environment:
      - POSTGRES_DB=neondb
      - POSTGRES_USER=workflow_user
      - POSTGRES_PASSWORD=workflow_password
    ports:
      - "5432:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data
      - ./init.sql:/docker-entrypoint-initdb.d/init.sql
    networks:
      - workflow_network

  # MongoDB
  mongodb:
    image: mongo:7
    ports:
      - "27017:27017"
    environment:
      - MONGO_INITDB_DATABASE=workflow_db
    volumes:
      - mongodb_data:/data/db
    networks:
      - workflow_network

  # Your Application (Optional)
  workflow-app:
    build: .
    ports:
      - "8000:8000"
    environment:
      - POSTGRES_HOST=postgres
      - POSTGRES_PORT=5432
      - POSTGRES_DB=neondb
      - POSTGRES_USER=workflow_user
      - POSTGRES_PASSWORD=workflow_password
      - MONGODB_URL=mongodb://mongodb:27017
      - MONGODB_DATABASE=workflow_db
      - PREFECT_API_URL=http://prefect-server:4200/api
    depends_on:
      - postgres
      - mongodb
      - prefect-server
    volumes:
      - ./:/app
    networks:
      - workflow_network

volumes:
  prefect_data:
  postgres_data:
  mongodb_data:

networks:
  workflow_network:
    driver: bridge
"""

# Requirements file
REQUIREMENTS_CONTENT = """
prefect>=2.15.0
fastapi>=0.104.0
uvicorn>=0.24.0
pandas>=2.0.0
sqlalchemy>=2.0.0
psycopg2-binary>=2.9.0
motor>=3.3.0
pymongo>=4.6.0
pydantic>=2.5.0
python-multipart>=0.0.6
aiofiles>=23.2.0
"""

# Dockerfile
DOCKERFILE_CONTENT = """
FROM python:3.11

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]
"""

# Initialize SQL script
INIT_SQL_CONTENT = """
-- Create tables for workflow management
CREATE TABLE IF NOT EXISTS WORKFLOW_DETAILS (
    workspace_id UUID,
    folder_id UUID,
    job_id UUID,
    workflow_id UUID PRIMARY KEY,
    workflow_name VARCHAR(255),
    created_at TIMESTAMP,
    created_by UUID,
    workflow_diagram JSONB,
    dependencies JSONB,
    d_flag BOOLEAN,
    share JSONB,
    workflow_dependency UUID[],
    check_in JSONB,
    tag VARCHAR(255),
    version_id INTEGER,
    created_by_name VARCHAR(255),
    created_by_photourl VARCHAR(500),
    workspace_name VARCHAR(255),
    folder_name VARCHAR(255),
    job_name VARCHAR(255),
    node_count INTEGER
);

CREATE TABLE IF NOT EXISTS NODE_DETAILS (
    workspace_id UUID,
    folder_id UUID,
    job_id UUID,
    workflow_id UUID,
    connector_id UUID,
    connector_type VARCHAR(100),
    connector_category VARCHAR(100),
    node_name VARCHAR(255),
    node_id VARCHAR(255),
    db_name VARCHAR(255),
    schema_name VARCHAR(255),
    filter_text TEXT,
    column_names JSONB,
    table_name VARCHAR(255),
    sql_text TEXT,
    transformations JSONB,
    field_mapping JSONB,
    sequence_id INTEGER,
    version_id INTEGER,
    FOREIGN KEY (workflow_id) REFERENCES WORKFLOW_DETAILS(workflow_id)
);

-- Create sample tables for testing
CREATE TABLE IF NOT EXISTS girish_groupby_src (
    employee_id SERIAL PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    place VARCHAR(100),
    country VARCHAR(100),
    hire_date DATE,
    job_id INTEGER,
    salary NUMERIC,
    bonus NUMERIC,
    hike NUMERIC,
    department_id INTEGER,
    manager_id INTEGER,
    pincode INTEGER,
    marks INTEGER,
    score INTEGER
);

CREATE TABLE IF NOT EXISTS girish_groupby_tgt (
    employee_id INTEGER,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    place VARCHAR(100)
);

-- Insert sample data
INSERT INTO girish_groupby_src (first_name, last_name, place, country, hire_date, job_id, salary, bonus, hike, department_id, manager_id, pincode, marks, score) VALUES
('John', 'Doe', 'New York', 'USA', '2023-01-15', 1, 75000, 5000, 3000, 1, 10, 10001, 85, 92),
('Jane', 'Smith', 'London', 'UK', '2023-02-20', 2, 68000, 4000, 2500, 2, 11, 20001, 78, 88),
('Bob', 'Johnson', 'Toronto', 'Canada', '2023-03-10', 3, 72000, 4500, 2800, 1, 10, 30001, 82, 90),
('Alice', 'Williams', 'Sydney', 'Australia', '2023-04-05', 4, 70000, 4200, 2600, 3, 12, 40001, 80, 87),
('Charlie', 'Brown', 'Berlin', 'Germany', '2023-05-12', 5, 65000, 3800, 2200, 2, 11, 50001, 75, 85);
"""

# Environment file template
ENV_TEMPLATE = """
# Database Configuration
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=neondb
POSTGRES_USER=workflow_user
POSTGRES_PASSWORD=workflow_password

# MongoDB Configuration
MONGODB_URL=mongodb://localhost:27017
MONGODB_DATABASE=workflow_db

# Prefect Configuration
PREFECT_API_URL=http://localhost:4200/api
PREFECT_WORK_POOL=default-agent-pool

# Application Configuration
LOG_LEVEL=INFO
ENABLE_PREFECT_BY_DEFAULT=true
"""

# Main application file template
MAIN_APP_CONTENT = """
# main.py
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import asyncio
import logging
from motor.motor_asyncio import AsyncIOMotorClient
from prefect_workflow_orchestrator import PrefectWorkflowOrchestrator
from workflow_management_api import WorkflowManager, create_workflow_router
import os

# Configure logging
logging.basicConfig(level=getattr(logging, os.getenv("LOG_LEVEL", "INFO")))
logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI(
    title="Workflow Orchestration API",
    description="Prefect-based workflow orchestration system",
    version="1.0.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global variables
mongodb_client = None
workflow_collection = None
workflow_manager = None

@app.on_event("startup")
async def startup_event():
    global mongodb_client, workflow_collection, workflow_manager
    
    # Initialize MongoDB connection
    mongodb_url = os.getenv("MONGODB_URL", "mongodb://localhost:27017")
    mongodb_database = os.getenv("MONGODB_DATABASE", "workflow_db")
    
    mongodb_client = AsyncIOMotorClient(mongodb_url)
    database = mongodb_client[mongodb_database]
    workflow_collection = database["workflows"]
    
    # Initialize workflow manager
    workflow_manager = WorkflowManager(workflow_collection, logger)
    
    # Add workflow management routes
    workflow_router = create_workflow_router(workflow_manager)
    app.include_router(workflow_router)
    
    logger.info("Application startup completed")

@app.on_event("shutdown")
async def shutdown_event():
    global mongodb_client
    if mongodb_client:
        mongodb_client.close()
    logger.info("Application shutdown completed")

@app.get("/")
async def root():
    return {
        "message": "Workflow Orchestration API",
        "version": "1.0.0",
        "prefect_enabled": os.getenv("ENABLE_PREFECT_BY_DEFAULT", "true").lower() == "true"
    }

@app.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "timestamp": "2024-01-01T00:00:00Z"
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
"""

# Setup script
SETUP_SCRIPT_CONTENT = """
#!/bin/bash

# setup.sh - Setup script for Prefect workflow orchestration

echo "Setting up Prefect Workflow Orchestration System..."

# Create project directory structure
mkdir -p workflows
mkdir -p logs
mkdir -p config

# Create .env file if it doesn't exist
if [ ! -f .env ]; then
    echo "Creating .env file..."
    cat > .env << 'EOF'
# Database Configuration
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=neondb
POSTGRES_USER=workflow_user
POSTGRES_PASSWORD=workflow_password

# MongoDB Configuration
MONGODB_URL=mongodb://localhost:27017
MONGODB_DATABASE=workflow_db

# Prefect Configuration
PREFECT_API_URL=http://localhost:4200/api
PREFECT_WORK_POOL=default-agent-pool

# Application Configuration
LOG_LEVEL=INFO
ENABLE_PREFECT_BY_DEFAULT=true
EOF
fi

# Create requirements.txt
cat > requirements.txt << 'EOF'
prefect>=2.15.0
fastapi>=0.104.0
uvicorn>=0.24.0
pandas>=2.0.0
sqlalchemy>=2.0.0
psycopg2-binary>=2.9.0
motor>=3.3.0
pymongo>=4.6.0
pydantic>=2.5.0
python-multipart>=0.0.6
aiofiles>=23.2.0
EOF

# Install Python dependencies
echo "Installing Python dependencies..."
pip install -r requirements.txt

# Start Prefect server if not running
echo "Starting Prefect server..."
prefect server start --host 0.0.0.0 --port 4200 &
PREFECT_PID=$!

# Wait for Prefect server to start
sleep 10

# Create work pool
echo "Creating Prefect work pool..."
prefect work-pool create default-agent-pool --type process

# Start Prefect worker
echo "Starting Prefect worker..."
prefect worker start --pool default-agent-pool &
WORKER_PID=$!

echo "Setup completed!"
echo "Prefect UI available at: http://localhost:4200"
echo "API will be available at: http://localhost:8000"
echo ""
echo "To stop services:"
echo "kill $PREFECT_PID $WORKER_PID"

# Save PIDs for cleanup
echo "$PREFECT_PID $WORKER_PID" > .pids
"""

# Cleanup script
CLEANUP_SCRIPT_CONTENT = """
#!/bin/bash

# cleanup.sh - Cleanup script for stopping services

echo "Stopping Prefect services..."

if [ -f .pids ]; then
    PIDS=$(cat .pids)
    for PID in $PIDS; do
        if kill -0 $PID 2>/dev/null; then
            echo "Stopping process $PID"
            kill $PID
        fi
    done
    rm .pids
fi

# Stop Docker containers if running
if command -v docker-compose &> /dev/null; then
    echo "Stopping Docker containers..."
    docker-compose down
fi

echo "Cleanup completed!"
"""

# Test script
TEST_SCRIPT_CONTENT = """
# test_workflow.py
import asyncio
import json
import uuid
from prefect_workflow_orchestrator import PrefectWorkflowOrchestrator

async def test_workflow_creation():
    \"\"\"Test workflow creation with sample data\"\"\"
    
    # Sample workflow data based on your structure
    test_workflow = {
        "workflow_id": str(uuid.uuid4()),
        "workflow_name": "Test_Employee_Pipeline",
        "workspace_id": "test-workspace-123",
        "folder_id": "test-folder-456", 
        "job_id": "test-job-789",
        "connectionInformation_v1": json.dumps({
            "source_node": {
                "connector_type": "postgresql",
                "node_name": "Employee_Source",
                "connector_category": "Databases",
                "dbDetails": {
                    "db": "neondb",
                    "schema": "public",
                    "table": "girish_groupby_src",
                    "filter": "employee_id > 0"
                },
                "sequence": 1
            },
            "filter_node": {
                "connector_type": "filter",
                "node_name": "High_Salary_Filter",
                "connector_category": "Transformations",
                "transformations": {
                    "transformations": [{
                        "value": "salary",
                        "selected": True,
                        "expression": {"value": "salary > 70000"}
                    }]
                },
                "field_mapping": [
                    {"value": "employee_id", "selected": True, "target": "employee_id"},
                    {"value": "first_name", "selected": True, "target": "first_name"},
                    {"value": "last_name", "selected": True, "target": "last_name"},
                    {"value": "salary", "selected": True, "target": "salary"}
                ],
                "sequence": 2
            },
            "target_node": {
                "connector_type": "postgresql",
                "node_name": "Filtered_Target",
                "connector_category": "Databases",
                "dbDetails": {
                    "db": "neondb",
                    "schema": "public",
                    "table": "girish_groupby_tgt",
                    "truncateTable": True
                },
                "field_mapping": [
                    {"value": "employee_id", "selected": True, "target": "employee_id"},
                    {"value": "first_name", "selected": True, "target": "first_name"},
                    {"value": "last_name", "selected": True, "target": "last_name"}
                ],
                "sequence": 3
            }
        }),
        "dependencies": json.dumps({
            "source_node": [],
            "filter_node": ["source_node"],
            "target_node": ["filter_node"]
        })
    }
    
    # Test workflow creation
    orchestrator = PrefectWorkflowOrchestrator()
    result = await orchestrator.process_add_workflow(
        test_workflow, 
        schedule="cron:0 */2 * * *"  # Every 2 hours
    )
    
    print("Workflow Creation Result:")
    print(json.dumps(result, indent=2))
    
    return result

if __name__ == "__main__":
    asyncio.run(test_workflow_creation())
"""

class DeploymentManager:
    """Manages deployment setup and configuration"""
    
    def __init__(self, project_dir: str = "."):
        self.project_dir = Path(project_dir)
    
    def create_files(self):
        """Create all necessary deployment files"""
        
        files_to_create = {
            "docker-compose.yml": DOCKER_COMPOSE_CONTENT,
            "requirements.txt": REQUIREMENTS_CONTENT,
            "Dockerfile": DOCKERFILE_CONTENT,
            "init.sql": INIT_SQL_CONTENT,
            ".env.template": ENV_TEMPLATE,
            "main.py": MAIN_APP_CONTENT,
            "setup.sh": SETUP_SCRIPT_CONTENT,
            "cleanup.sh": CLEANUP_SCRIPT_CONTENT,
            "test_workflow.py": TEST_SCRIPT_CONTENT
        }
        
        for filename, content in files_to_create.items():
            file_path = self.project_dir / filename
            
            # Don't overwrite existing .env file
            if filename == ".env" and file_path.exists():
                continue
                
            with open(file_path, 'w') as f:
                f.write(content.strip())
            
            # Make shell scripts executable
            if filename.endswith('.sh'):
                os.chmod(file_path, 0o755)
            
            print(f"Created: {filename}")
    
    def setup_project(self):
        """Set up the complete project structure"""
        
        print("Setting up Prefect Workflow Orchestration project...")
        
        # Create directories
        directories = [
            "workflows",
            "logs",
            "config",
            "tests"
        ]
        
        for directory in directories:
            dir_path = self.project_dir / directory
            dir_path.mkdir(exist_ok=True)
            print(f"Created directory: {directory}")
        
        # Create files
        self.create_files()
        
        print("\nProject setup completed!")
        print("\nNext steps:")
        print("1. Copy .env.template to .env and update with your configuration")
        print("2. Run: chmod +x setup.sh && ./setup.sh")
        print("3. Or use Docker: docker-compose up -d")
        print("4. Test with: python test_workflow.py")
        print("\nPrefect UI will be available at: http://localhost:4200")
        print("API will be available at: http://localhost:8000")

def main():
    """Main setup function"""
    manager = DeploymentManager()
    manager.setup_project()

if __name__ == "__main__":
    main()

# worker_management.py
"""
Multi-worker management system for Prefect workflows
"""

import asyncio
import json
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from enum import Enum
from prefect import get_client
from prefect.client.schemas.filters import WorkPoolFilter, WorkerFilter
import logging

logger = logging.getLogger(__name__)

class WorkerType(Enum):
    GENERAL = "general"
    ETL = "etl" 
    HIGH_MEMORY = "high_memory"

class WorkPoolConfig:
    """Configuration for different work pools"""
    
    POOLS = {
        "default-agent-pool": {
            "type": "process",
            "description": "General purpose worker pool",
            "worker_type": WorkerType.GENERAL,
            "concurrency_limit": 5,
            "tags": ["general", "default"]
        },
        "etl-pool": {
            "type": "process", 
            "description": "ETL and database operations",
            "worker_type": WorkerType.ETL,
            "concurrency_limit": 3,
            "tags": ["etl", "database", "transform"]
        },
        "high-memory-pool": {
            "type": "process",
            "description": "High memory intensive tasks",
            "worker_type": WorkerType.HIGH_MEMORY,
            "concurrency_limit": 2,
            "tags": ["high-memory", "analytics", "ml"]
        }
    }

class WorkerManager:
    """Manages multiple Prefect workers and work pools"""
    
    def __init__(self):
        self.client = None
        
    async def __aenter__(self):
        self.client = get_client()
        await self.client.__aenter__()
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.client:
            await self.client.__aexit__(exc_type, exc_val, exc_tb)
    
    async def setup_work_pools(self):
        """Create and configure work pools"""
        logger.info("Setting up work pools...")
        
        for pool_name, config in WorkPoolConfig.POOLS.items():
            try:
                # Check if pool exists
                try:
                    existing_pool = await self.client.read_work_pool(pool_name)
                    logger.info(f"Work pool '{pool_name}' already exists")
                    continue
                except Exception:
                    pass
                
                # Create work pool
                work_pool = await self.client.create_work_pool(
                    name=pool_name,
                    type=config["type"],
                    description=config["description"],
                    concurrency_limit=config["concurrency_limit"]
                )
                
                logger.info(f"Created work pool: {pool_name}")
                
            except Exception as e:
                logger.error(f"Failed to create work pool {pool_name}: {str(e)}")
    
    async def get_worker_status(self) -> Dict[str, Any]:
        """Get status of all workers and work pools"""
        try:
            # Get work pools
            work_pools = await self.client.read_work_pools()
            
            # Get workers
            workers = await self.client.read_workers()
            
            status = {
                "timestamp": datetime.now().isoformat(),
                "work_pools": [],
                "workers": [],
                "summary": {
                    "total_pools": len(work_pools),
                    "total_workers": len(workers),
                    "active_workers": 0,
                    "total_capacity": 0
                }
            }
            
            # Process work pools
            for pool in work_pools:
                pool_info = {
                    "name": pool.name,
                    "type": pool.type,
                    "is_paused": pool.is_paused,
                    "concurrency_limit": pool.concurrency_limit,
                    "description": pool.description
                }
                status["work_pools"].append(pool_info)
            
            # Process workers
            for worker in workers:
                worker_info = {
                    "id": str(worker.id),
                    "name": worker.name,
                    "work_pool_name": worker.work_pool_name,
                    "status": worker.status,
                    "last_heartbeat": worker.last_heartbeat_time.isoformat() if worker.last_heartbeat_time else None,
                    "heartbeat_interval": worker.heartbeat_interval_seconds
                }
                
                if worker.status == "ONLINE":
                    status["summary"]["active_workers"] += 1
                
                status["workers"].append(worker_info)
            
            return status
            
        except Exception as e:
            logger.error(f"Failed to get worker status: {str(e)}")
            return {"error": str(e)}
    
    async def distribute_workflow_to_pool(self, workflow_type: str, workflow_data: Dict[str, Any]) -> str:
        """Determine which work pool should handle a workflow"""
        
        connection_info = json.loads(workflow_data.get("connectionInformation_v1", "{}"))
        
        # Count different node types
        db_nodes = 0
        transform_nodes = 0
        total_nodes = len(connection_info)
        
        for node_info in connection_info.values():
            connector_type = node_info.get("connector_type", "")
            if connector_type in ["postgresql", "mysql", "oracle"]:
                db_nodes += 1
            elif connector_type in ["filter", "join", "aggregate"]:
                transform_nodes += 1
        
        # Decision logic for work pool assignment
        if total_nodes > 10 or any("high-memory" in str(node).lower() for node in connection_info.values()):
            return "high-memory-pool"
        elif db_nodes > 2 or transform_nodes > 3:
            return "etl-pool"
        else:
            return "default-agent-pool"
    
    async def get_pool_capacity(self, pool_name: str) -> Dict[str, Any]:
        """Get current capacity and load for a work pool"""
        try:
            # Get active flow runs for this pool
            flow_runs = await self.client.read_flow_runs(
                limit=100,
                sort="CREATED_DESC"
            )
            
            # Filter runs for this pool (approximate - would need better filtering)
            pool_runs = [run for run in flow_runs if run.state and run.state.type in ["RUNNING", "PENDING"]]
            
            pool_config = WorkPoolConfig.POOLS.get(pool_name, {})
            concurrency_limit = pool_config.get("concurrency_limit", 5)
            
            return {
                "pool_name": pool_name,
                "concurrency_limit": concurrency_limit,
                "active_runs": len(pool_runs),
                "available_capacity": max(0, concurrency_limit - len(pool_runs)),
                "utilization_percent": (len(pool_runs) / concurrency_limit) * 100 if concurrency_limit > 0 else 0
            }
            
        except Exception as e:
            logger.error(f"Failed to get pool capacity for {pool_name}: {str(e)}")
            return {"error": str(e)}

# Enhanced Prefect Workflow Orchestrator with multi-worker support
class MultiWorkerPrefectOrchestrator:
    """Enhanced orchestrator with multi-worker support"""
    
    def __init__(self):
        self.worker_manager = None
        
    async def process_add_workflow(self, workflow_dict: Dict[str, Any], schedule: Optional[str] = None, preferred_pool: Optional[str] = None):
        """Process workflow with intelligent worker pool assignment"""
        
        async with WorkerManager() as worker_manager:
            try:
                # Parse workflow data
                workflow_data = self.parse_workflow_data(workflow_dict)
                
                # Determine work pool if not specified
                if not preferred_pool:
                    preferred_pool = await worker_manager.distribute_workflow_to_pool(
                        "data_pipeline", workflow_data
                    )
                
                # Check pool capacity
                capacity = await worker_manager.get_pool_capacity(preferred_pool)
                if capacity.get("available_capacity", 0) <= 0:
                    # Fallback to default pool if preferred is full
                    preferred_pool = "default-agent-pool"
                    logger.warning(f"Preferred pool full, using default pool: {preferred_pool}")
                
                # Create workflow flow with pool-specific tags
                workflow_flow = self.create_workflow_flow(workflow_data, preferred_pool)
                
                # Create deployment with specific work pool
                deployment = self.create_deployment(
                    workflow_flow, 
                    workflow_data, 
                    schedule, 
                    work_pool=preferred_pool
                )
                
                # Apply deployment
                deployment_id = await deployment.apply()
                
                logger.info(f"Workflow deployed to pool: {preferred_pool}")
                
                return {
                    "status": "success",
                    "workflow_id": workflow_data["workflow_id"],
                    "deployment_id": str(deployment_id),
                    "deployment_name": deployment.name,
                    "flow_name": workflow_flow.name,
                    "work_pool": preferred_pool,
                    "schedule": schedule,
                    "capacity_info": capacity,
                    "message": f"Workflow deployed to work pool: {preferred_pool}"
                }
                
            except Exception as e:
                logger.error(f"Failed to process workflow: {str(e)}")
                return {
                    "status": "error",
                    "error": str(e),
                    "workflow_id": workflow_dict.get("workflow_id", "unknown")
                }
    
    def parse_workflow_data(self, workflow_dict: Dict[str, Any]) -> Dict[str, Any]:
        """Parse workflow data from addWorkflow payload"""
        connection_info = json.loads(workflow_dict.get("connectionInformation_v1", "{}"))
        dependencies = json.loads(workflow_dict.get("dependencies", "{}"))
        
        return {
            "workflow_id": workflow_dict["workflow_id"],
            "workflow_name": workflow_dict["workflow_name"],
            "connection_info": connection_info,
            "dependencies": dependencies,
            "workspace_id": workflow_dict["workspace_id"],
            "folder_id": workflow_dict["folder_id"],
            "job_id": workflow_dict["job_id"]
        }
    
    def create_workflow_flow(self, workflow_data: Dict[str, Any], work_pool: str):
        """Create workflow flow with pool-specific configuration"""
        from prefect import flow, task
        from prefect.task_runners import ConcurrentTaskRunner
        
        workflow_id = workflow_data["workflow_id"]
        workflow_name = workflow_data["workflow_name"]
        
        # Configure task runner based on work pool
        if work_pool == "high-memory-pool":
            task_runner = ConcurrentTaskRunner(max_workers=2)
        elif work_pool == "etl-pool":
            task_runner = ConcurrentTaskRunner(max_workers=3)
        else:
            task_runner = ConcurrentTaskRunner(max_workers=5)
        
        @flow(
            name=f"workflow_{workflow_name}_{workflow_id[:8]}",
            task_runner=task_runner,
            retries=1,
            retry_delay_seconds=30
        )
        async def workflow_flow():
            # Your existing workflow flow logic here
            # ... (same as before)
            pass
        
        return workflow_flow
    
    def create_deployment(self, workflow_flow, workflow_data: Dict[str, Any], schedule: Optional[str] = None, work_pool: str = "default-agent-pool"):
        """Create deployment with specific work pool"""
        from prefect.deployments import Deployment
        from prefect.server.schemas.schedules import CronSchedule, IntervalSchedule
        from datetime import timedelta
        
        workflow_name = workflow_data["workflow_name"]
        workflow_id = workflow_data["workflow_id"]
        
        deployment_name = f"{workflow_name}_{workflow_id[:8]}"
        
        # Configure schedule
        schedule_config = None
        if schedule:
            if schedule.startswith("cron:"):
                cron_expression = schedule.replace("cron:", "")
                schedule_config = CronSchedule(cron=cron_expression)
            elif schedule.startswith("interval:"):
                interval_minutes = int(schedule.replace("interval:", ""))
                schedule_config = IntervalSchedule(interval=timedelta(minutes=interval_minutes))
        
        # Get pool-specific tags
        pool_config = WorkPoolConfig.POOLS.get(work_pool, {})
        pool_tags = pool_config.get("tags", [])
        
        deployment = Deployment.build_from_flow(
            flow=workflow_flow,
            name=deployment_name,
            schedule=schedule_config,
            work_pool_name=work_pool,
            tags=[
                f"workflow_id:{workflow_id}",
                f"workspace_id:{workflow_data['workspace_id']}",
                f"work_pool:{work_pool}",
                "auto-generated"
            ] + pool_tags,
            parameters={
                "workflow_metadata": {
                    "workflow_id": workflow_id,
                    "workflow_name": workflow_name,
                    "work_pool": work_pool,
                    "created_at": datetime.now().isoformat()
                }
            }
        )
        
        return deployment

# Worker monitoring script
WORKER_MONITOR_SCRIPT = '''
# worker_monitor.py
import asyncio
import json
import time
from datetime import datetime
from worker_management import WorkerManager
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def monitor_workers():
    """Monitor worker health and capacity"""
    
    while True:
        try:
            async with WorkerManager() as manager:
                status = await manager.get_worker_status()
                
                logger.info("=== Worker Status ===")
                logger.info(f"Active Workers: {status['summary']['active_workers']}/{status['summary']['total_workers']}")
                
                for worker in status['workers']:
                    logger.info(f"Worker {worker['name']}: {worker['status']} (Pool: {worker['work_pool_name']})")
                
                # Check for unhealthy workers
                for worker in status['workers']:
                    if worker['status'] != 'ONLINE':
                        logger.warning(f"Worker {worker['name']} is {worker['status']}")
                
                # Check pool capacities
                for pool_name in ["default-agent-pool", "etl-pool", "high-memory-pool"]:
                    capacity = await manager.get_pool_capacity(pool_name)
                    utilization = capacity.get('utilization_percent', 0)
                    
                    if utilization > 80:
                        logger.warning(f"Pool {pool_name} is {utilization:.1f}% utilized")
                    
                logger.info("=" * 50)
                
        except Exception as e:
            logger.error(f"Monitor error: {str(e)}")
        
        await asyncio.sleep(30)  # Check every 30 seconds

if __name__ == "__main__":
    asyncio.run(monitor_workers())
'''

# Setup script for multi-worker deployment
MULTI_WORKER_SETUP = '''
#!/bin/bash

# multi_worker_setup.sh
echo "Setting up multi-worker Prefect deployment..."

# Create worker monitor script
cat > worker_monitor.py << 'EOF'
""" + WORKER_MONITOR_SCRIPT + """
EOF

# Start services
echo "Starting multi-worker Prefect setup..."
docker-compose -f docker-compose-multi-worker.yml up -d

# Wait for services to start
echo "Waiting for services to initialize..."
sleep 60

# Check worker status
echo "Checking worker status..."
docker-compose -f docker-compose-multi-worker.yml logs prefect-worker-1 | tail -10
docker-compose -f docker-compose-multi-worker.yml logs prefect-worker-2 | tail -10  
docker-compose -f docker-compose-multi-worker.yml logs prefect-worker-3 | tail -10

echo ""
echo "Multi-worker setup completed!"
echo "Prefect UI: http://localhost:4200"
echo "API: http://localhost:8000"
echo ""
echo "Work Pools:"
echo "- default-agent-pool (General, 5 concurrent)"
echo "- etl-pool (ETL/Database, 3 concurrent)"  
echo "- high-memory-pool (Analytics, 2 concurrent)"
'''

def create_multi_worker_files():
    """Create all files for multi-worker setup"""
    
    files = {
        "worker_monitor.py": WORKER_MONITOR_SCRIPT,
        "multi_worker_setup.sh": MULTI_WORKER_SETUP
    }
    
    for filename, content in files.items():
        with open(filename, 'w') as f:
            f.write(content.strip())
        
        if filename.endswith('.sh'):
            import os
            os.chmod(filename, 0o755)
        
        print(f"Created: {filename}")

if __name__ == "__main__":
    create_multi_worker_files()

# multi_worker_api.py
"""
API endpoints for managing multiple Prefect workers
"""

from fastapi import APIRouter, HTTPException, BackgroundTasks
from pydantic import BaseModel
from typing import Optional, Dict, Any, List
from enum import Enum
import asyncio
from datetime import datetime
from worker_management import WorkerManager, MultiWorkerPrefectOrchestrator, WorkPoolConfig

class WorkPoolType(str, Enum):
    DEFAULT = "default-agent-pool"
    ETL = "etl-pool"
    HIGH_MEMORY = "high-memory-pool"

class WorkflowExecuteRequest(BaseModel):
    workflow_id: str
    parameters: Optional[Dict[str, Any]] = {}
    preferred_pool: Optional[WorkPoolType] = None
    priority: Optional[str] = "normal"  # low, normal, high

class WorkPoolStatusRequest(BaseModel):
    pool_name: WorkPoolType

class WorkerScaleRequest(BaseModel):
    pool_name: WorkPoolType
    desired_workers: int

class MultiWorkerAPI:
    """API for managing multiple Prefect workers"""
    
    def __init__(self, workflow_collection, logger=None):
        self.workflow_collection = workflow_collection
        self.logger = logger
        self.orchestrator = MultiWorkerPrefectOrchestrator()
    
    async def get_worker_overview(self) -> Dict[str, Any]:
        """Get overview of all workers and pools"""
        try:
            async with WorkerManager() as manager:
                status = await manager.get_worker_status()
                
                # Get capacity for each pool
                pool_capacities = {}
                for pool_name in WorkPoolConfig.POOLS.keys():
                    capacity = await manager.get_pool_capacity(pool_name)
                    pool_capacities[pool_name] = capacity
                
                return {
                    "timestamp": datetime.now().isoformat(),
                    "worker_summary": status["summary"],
                    "work_pools": status["work_pools"],
                    "workers": status["workers"],
                    "pool_capacities": pool_capacities,
                    "recommendations": await self._generate_scaling_recommendations(pool_capacities)
                }
                
        except Exception as e:
            if self.logger:
                self.logger.error(f"Failed to get worker overview: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Failed to get worker overview: {str(e)}")
    
    async def execute_workflow_with_pool_selection(self, request: WorkflowExecuteRequest):
        """Execute workflow with intelligent pool selection"""
        try:
            # Get workflow from database
            workflow = await self.workflow_collection.find_one({"workflow_id": request.workflow_id})
            
            if not workflow:
                raise HTTPException(status_code=404, detail=f"Workflow {request.workflow_id} not found")
            
            # Determine best pool if not specified
            if not request.preferred_pool:
                async with WorkerManager() as manager:
                    request.preferred_pool = await manager.distribute_workflow_to_pool(
                        "data_pipeline", workflow
                    )
            
            # Check pool capacity before execution
            async with WorkerManager() as manager:
                capacity = await manager.get_pool_capacity(request.preferred_pool)
                
                if capacity.get("available_capacity", 0) <= 0:
                    # Try to find alternative pool
                    for pool_name in WorkPoolConfig.POOLS.keys():
                        if pool_name != request.preferred_pool:
                            alt_capacity = await manager.get_pool_capacity(pool_name)
                            if alt_capacity.get("available_capacity", 0) > 0:
                                original_pool = request.preferred_pool
                                request.preferred_pool = pool_name
                                if self.logger:
                                    self.logger.info(f"Redirected workflow from {original_pool} to {pool_name}")
                                break
                    else:
                        raise HTTPException(
                            status_code=429, 
                            detail="All worker pools at capacity. Please try again later."
                        )
            
            # Execute workflow
            from prefect import get_client
            from prefect.deployments import run_deployment
            
            deployment_name = workflow.get("prefect_deployment_name")
            if not deployment_name:
                raise HTTPException(
                    status_code=400,
                    detail=f"Workflow {request.workflow_id} does not have Prefect deployment"
                )
            
            async with get_client() as client:
                deployment = await client.read_deployment_by_name(deployment_name)
                
                # Create flow run with pool-specific tags
                flow_run = await client.create_flow_run_from_deployment(
                    deployment.id,
                    parameters=request.parameters,
                    tags=[
                        "adhoc",
                        f"workflow_id:{request.workflow_id}",
                        f"pool:{request.preferred_pool}",
                        f"priority:{request.priority}"
                    ]
                )
                
                if self.logger:
                    self.logger.info(f"Started workflow {request.workflow_id} on pool {request.preferred_pool}")
                
                return {
                    "status": "success",
                    "flow_run_id": str(flow_run.id),
                    "workflow_id": request.workflow_id,
                    "work_pool": request.preferred_pool,
                    "priority": request.priority,
                    "capacity_at_execution": capacity,
                    "message": f"Workflow executing on {request.preferred_pool}"
                }
            
        except HTTPException:
            raise
        except Exception as e:
            if self.logger:
                self.logger.error(f"Workflow execution failed: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Workflow execution failed: {str(e)}")
    
    async def get_pool_status(self, pool_name: WorkPoolType):
        """Get detailed status for a specific work pool"""
        try:
            async with WorkerManager() as manager:
                capacity = await manager.get_pool_capacity(pool_name)
                
                # Get workers for this pool
                from prefect import get_client
                async with get_client() as client:
                    workers = await client.read_workers()
                    pool_workers = [w for w in workers if w.work_pool_name == pool_name]
                    
                    # Get recent flow runs for this pool
                    flow_runs = await client.read_flow_runs(
                        limit=50,
                        sort="CREATED_DESC"
                    )
                    
                    # Filter runs by pool (approximation based on tags)
                    pool_runs = []
                    for run in flow_runs:
                        if any(f"pool:{pool_name}" in tag for tag in (run.tags or [])):
                            pool_runs.append({
                                "flow_run_id": str(run.id),
                                "state": run.state.type if run.state else "Unknown",
                                "start_time": run.start_time.isoformat() if run.start_time else None,
                                "end_time": run.end_time.isoformat() if run.end_time else None,
                                "tags": run.tags
                            })
                
                pool_config = WorkPoolConfig.POOLS.get(pool_name, {})
                
                return {
                    "pool_name": pool_name,
                    "pool_config": pool_config,
                    "capacity": capacity,
                    "workers": [
                        {
                            "id": str(w.id),
                            "name": w.name,
                            "status": w.status,
                            "last_heartbeat": w.last_heartbeat_time.isoformat() if w.last_heartbeat_time else None
                        }
                        for w in pool_workers
                    ],
                    "recent_runs": pool_runs[:10],
                    "performance_metrics": await self._calculate_pool_metrics(pool_runs)
                }
                
        except Exception as e:
            if self.logger:
                self.logger.error(f"Failed to get pool status: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Failed to get pool status: {str(e)}")
    
    async def pause_work_pool(self, pool_name: WorkPoolType):
        """Pause a work pool to prevent new work"""
        try:
            from prefect import get_client
            async with get_client() as client:
                work_pool = await client.read_work_pool(pool_name)
                await client.update_work_pool(
                    work_pool_name=pool_name,
                    work_pool={"is_paused": True}
                )
                
                if self.logger:
                    self.logger.info(f"Paused work pool: {pool_name}")
                
                return {
                    "status": "success",
                    "message": f"Work pool {pool_name} paused",
                    "pool_name": pool_name
                }
                
        except Exception as e:
            if self.logger:
                self.logger.error(f"Failed to pause work pool: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Failed to pause work pool: {str(e)}")
    
    async def resume_work_pool(self, pool_name: WorkPoolType):
        """Resume a paused work pool"""
        try:
            from prefect import get_client
            async with get_client() as client:
                work_pool = await client.read_work_pool(pool_name)
                await client.update_work_pool(
                    work_pool_name=pool_name,
                    work_pool={"is_paused": False}
                )
                
                if self.logger:
                    self.logger.info(f"Resumed work pool: {pool_name}")
                
                return {
                    "status": "success",
                    "message": f"Work pool {pool_name} resumed",
                    "pool_name": pool_name
                }
                
        except Exception as e:
            if self.logger:
                self.logger.error(f"Failed to resume work pool: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Failed to resume work pool: {str(e)}")
    
    async def get_workflow_distribution(self):
        """Get distribution of workflows across pools"""
        try:
            # Get all workflows with Prefect deployments
            workflows = await self.workflow_collection.find({
                "prefect_deployment_name": {"$exists": True}
            }).to_list(None)
            
            distribution = {
                "total_workflows": len(workflows),
                "by_pool": {},
                "by_status": {},
                "recent_activity": []
            }
            
            for workflow in workflows:
                # Try to determine pool from deployment tags or name
                deployment_name = workflow.get("prefect_deployment_name", "")
                
                # This is a simplified way - in practice, you'd query Prefect for actual pool assignments
                pool = "unknown"
                if "etl" in deployment_name.lower():
                    pool = "etl-pool"
                elif "memory" in deployment_name.lower():
                    pool = "high-memory-pool"
                else:
                    pool = "default-agent-pool"
                
                distribution["by_pool"][pool] = distribution["by_pool"].get(pool, 0) + 1
                
                status = workflow.get("prefect_status", "unknown")
                distribution["by_status"][status] = distribution["by_status"].get(status, 0) + 1
            
            return distribution
            
        except Exception as e:
            if self.logger:
                self.logger.error(f"Failed to get workflow distribution: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Failed to get workflow distribution: {str(e)}")
    
    async def _calculate_pool_metrics(self, pool_runs: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Calculate performance metrics for a pool"""
        if not pool_runs:
            return {
                "avg_duration": 0,
                "success_rate": 0,
                "total_runs": 0,
                "failed_runs": 0
            }
        
        total_runs = len(pool_runs)
        successful_runs = len([r for r in pool_runs if r["state"] == "COMPLETED"])
        failed_runs = len([r for r in pool_runs if r["state"] == "FAILED"])
        
        # Calculate average duration for completed runs
        durations = []
        for run in pool_runs:
            if run["start_time"] and run["end_time"]:
                start = datetime.fromisoformat(run["start_time"].replace('Z', '+00:00'))
                end = datetime.fromisoformat(run["end_time"].replace('Z', '+00:00'))
                duration = (end - start).total_seconds()
                durations.append(duration)
        
        avg_duration = sum(durations) / len(durations) if durations else 0
        
        return {
            "avg_duration_seconds": round(avg_duration, 2),
            "success_rate": round((successful_runs / total_runs) * 100, 2) if total_runs > 0 else 0,
            "total_runs": total_runs,
            "successful_runs": successful_runs,
            "failed_runs": failed_runs,
            "running_runs": len([r for r in pool_runs if r["state"] == "RUNNING"])
        }
    
    async def _generate_scaling_recommendations(self, pool_capacities: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Generate scaling recommendations based on pool utilization"""
        recommendations = []
        
        for pool_name, capacity in pool_capacities.items():
            utilization = capacity.get("utilization_percent", 0)
            
            if utilization > 90:
                recommendations.append({
                    "type": "scale_up",
                    "pool": pool_name,
                    "current_utilization": utilization,
                    "message": f"Pool {pool_name} is {utilization:.1f}% utilized. Consider adding more workers.",
                    "priority": "high"
                })
            elif utilization < 20 and capacity.get("active_runs", 0) == 0:
                recommendations.append({
                    "type": "scale_down",
                    "pool": pool_name,
                    "current_utilization": utilization,
                    "message": f"Pool {pool_name} is underutilized at {utilization:.1f}%. Consider reducing workers.",
                    "priority": "low"
                })
            elif 70 <= utilization <= 90:
                recommendations.append({
                    "type": "monitor",
                    "pool": pool_name,
                    "current_utilization": utilization,
                    "message": f"Pool {pool_name} utilization is healthy at {utilization:.1f}%.",
                    "priority": "info"
                })
        
        return recommendations


def create_multi_worker_router(multi_worker_api: MultiWorkerAPI) -> APIRouter:
    """Create FastAPI router for multi-worker management"""
    
    router = APIRouter(prefix="/api/v1/workers", tags=["multi-worker"])
    
    @router.get("/overview")
    async def get_worker_overview():
        """Get overview of all workers and pools"""
        return await multi_worker_api.get_worker_overview()
    
    @router.post("/execute")
    async def execute_workflow_smart(request: WorkflowExecuteRequest):
        """Execute workflow with intelligent pool selection"""
        return await multi_worker_api.execute_workflow_with_pool_selection(request)
    
    @router.get("/pools/{pool_name}/status")
    async def get_pool_status(pool_name: WorkPoolType):
        """Get detailed status for a specific work pool"""
        return await multi_worker_api.get_pool_status(pool_name)
    
    @router.post("/pools/{pool_name}/pause")
    async def pause_work_pool(pool_name: WorkPoolType):
        """Pause a work pool"""
        return await multi_worker_api.pause_work_pool(pool_name)
    
    @router.post("/pools/{pool_name}/resume")
    async def resume_work_pool(pool_name: WorkPoolType):
        """Resume a work pool"""
        return await multi_worker_api.resume_work_pool(pool_name)
    
    @router.get("/distribution")
    async def get_workflow_distribution():
        """Get workflow distribution across pools"""
        return await multi_worker_api.get_workflow_distribution()
    
    @router.get("/pools")
    async def list_work_pools():
        """List all available work pools with their configurations"""
        return {
            "work_pools": WorkPoolConfig.POOLS,
            "total_pools": len(WorkPoolConfig.POOLS)
        }
    
    return router


# Enhanced addWorkflow integration with multi-worker support
async def enhanced_add_workflow_multi_worker(
    self, 
    payload, 
    user_obj, 
    enable_prefect: bool = True, 
    schedule: Optional[str] = None,
    preferred_pool: Optional[str] = None,
    workflow_priority: str = "normal"
):
    """
    Enhanced addWorkflow with multi-worker pool selection
    """
    try:
        # Your existing addWorkflow logic...
        # ... (all the original validation and database operations)
        
        if enable_prefect:
            try:
                orchestrator = MultiWorkerPrefectOrchestrator()
                
                # Process workflow with intelligent pool selection
                prefect_result = await orchestrator.process_add_workflow(
                    workflowDict, 
                    schedule=schedule,
                    preferred_pool=preferred_pool
                )
                
                if prefect_result["status"] == "success":
                    # Update workflowDict with Prefect and pool information
                    workflowDict.update({
                        "prefect_deployment_id": prefect_result["deployment_id"],
                        "prefect_deployment_name": prefect_result["deployment_name"],
                        "prefect_flow_name": prefect_result["flow_name"],
                        "prefect_work_pool": prefect_result["work_pool"],
                        "prefect_status": "deployed",
                        "prefect_schedule": schedule,
                        "workflow_priority": workflow_priority,
                        "prefect_created_at": datetime.now().isoformat()
                    })
                    
                    # Update in MongoDB
                    await self.workflowCollection.update_one(
                        {"workflow_id": workflowDict["workflow_id"]},
                        {"$set": {
                            "prefect_deployment_id": prefect_result["deployment_id"],
                            "prefect_deployment_name": prefect_result["deployment_name"],
                            "prefect_flow_name": prefect_result["flow_name"],
                            "prefect_work_pool": prefect_result["work_pool"],
                            "prefect_status": "deployed",
                            "prefect_schedule": schedule,
                            "workflow_priority": workflow_priority,
                            "prefect_created_at": datetime.now().isoformat()
                        }}
                    )
                    
                    self.logger.info(f"Workflow deployed to pool {prefect_result['work_pool']}: {prefect_result}")
                
            except Exception as prefect_error:
                self.logger.error(f"Prefect multi-worker integration failed: {str(prefect_error)}")
                workflowDict["prefect_status"] = "failed"
                workflowDict["prefect_error"] = str(prefect_error)
        
        # Enhanced response with pool information
        response = {"id": workflowDict}
        if enable_prefect and workflowDict.get("prefect_deployment_id"):
            response["prefect"] = {
                "deployment_id": workflowDict["prefect_deployment_id"],
                "deployment_name": workflowDict["prefect_deployment_name"],
                "flow_name": workflowDict["prefect_flow_name"],
                "work_pool": workflowDict.get("prefect_work_pool"),
                "status": workflowDict["prefect_status"],
                "priority": workflow_priority
            }
        
        return response
        
    except Exception as ex:
        self.logger.error(f"Enhanced workflow creation failed: {str(ex)}")
        raise


# Usage example in main application
"""
# main.py integration example

from multi_worker_api import MultiWorkerAPI, create_multi_worker_router

# Initialize multi-worker API
multi_worker_api = MultiWorkerAPI(workflow_collection=your_workflow_collection, logger=your_logger)

# Add multi-worker management endpoints
multi_worker_router = create_multi_worker_router(multi_worker_api)
app.include_router(multi_worker_router)

# Available endpoints:
# GET /api/v1/workers/overview - Worker and pool overview
# POST /api/v1/workers/execute - Smart workflow execution
# GET /api/v1/workers/pools/{pool_name}/status - Pool status
# POST /api/v1/workers/pools/{pool_name}/pause - Pause pool
# POST /api/v1/workers/pools/{pool_name}/resume - Resume pool
# GET /api/v1/workers/distribution - Workflow distribution
# GET /api/v1/workers/pools - List all pools
"""

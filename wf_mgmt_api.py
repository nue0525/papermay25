# workflow_management_api.py
"""
API endpoints and management functions for Prefect workflows
"""

from fastapi import APIRouter, HTTPException, BackgroundTasks
from pydantic import BaseModel
from typing import Optional, Dict, Any, List
import asyncio
from datetime import datetime
from prefect import get_client
from prefect.client.schemas.filters import FlowFilter, FlowRunFilter
from prefect.deployments import run_deployment
import json

# Pydantic models for API requests
class WorkflowExecuteRequest(BaseModel):
    workflow_id: str
    parameters: Optional[Dict[str, Any]] = {}

class WorkflowScheduleRequest(BaseModel):
    workflow_id: str
    schedule: str  # Format: "cron:0 2 * * *" or "interval:60"

class WorkflowStatusRequest(BaseModel):
    workflow_id: str

class WorkflowDeleteRequest(BaseModel):
    workflow_id: str
    delete_runs: Optional[bool] = False


class WorkflowManager:
    """
    Manages Prefect workflows created from your addWorkflow process
    """
    
    def __init__(self, workflow_collection, logger=None):
        self.workflow_collection = workflow_collection
        self.logger = logger
    
    async def execute_workflow_adhoc(self, workflow_id: str, parameters: Optional[Dict[str, Any]] = None):
        """Execute a workflow on-demand"""
        try:
            # Get workflow from database
            workflow = await self.workflow_collection.find_one({"workflow_id": workflow_id})
            
            if not workflow:
                raise HTTPException(status_code=404, detail=f"Workflow {workflow_id} not found")
            
            deployment_name = workflow.get("prefect_deployment_name")
            if not deployment_name:
                raise HTTPException(
                    status_code=400, 
                    detail=f"Workflow {workflow_id} does not have Prefect deployment"
                )
            
            # Trigger Prefect flow run
            async with get_client() as client:
                deployment = await client.read_deployment_by_name(deployment_name)
                
                flow_run = await client.create_flow_run_from_deployment(
                    deployment.id,
                    parameters=parameters or {},
                    tags=["adhoc", f"workflow_id:{workflow_id}"]
                )
                
                if self.logger:
                    self.logger.info(f"Started adhoc execution for workflow {workflow_id}, flow_run_id: {flow_run.id}")
                
                return {
                    "status": "success",
                    "flow_run_id": str(flow_run.id),
                    "workflow_id": workflow_id,
                    "deployment_name": deployment_name,
                    "message": "Workflow execution started successfully"
                }
            
        except HTTPException:
            raise
        except Exception as e:
            if self.logger:
                self.logger.error(f"Workflow execution failed: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Workflow execution failed: {str(e)}")
    
    async def get_workflow_status(self, workflow_id: str):
        """Get current status and recent runs of a workflow"""
        try:
            # Get workflow from database
            workflow = await self.workflow_collection.find_one({"workflow_id": workflow_id})
            
            if not workflow:
                raise HTTPException(status_code=404, detail=f"Workflow {workflow_id} not found")
            
            deployment_name = workflow.get("prefect_deployment_name")
            flow_name = workflow.get("prefect_flow_name")
            
            if not deployment_name or not flow_name:
                return {
                    "workflow_id": workflow_id,
                    "workflow_name": workflow.get("workflow_name"),
                    "prefect_status": "not_deployed",
                    "recent_runs": [],
                    "deployment_info": None
                }
            
            async with get_client() as client:
                # Get deployment info
                try:
                    deployment = await client.read_deployment_by_name(deployment_name)
                    deployment_info = {
                        "deployment_id": str(deployment.id),
                        "deployment_name": deployment.name,
                        "is_schedule_active": deployment.is_schedule_active,
                        "schedule": str(deployment.schedule) if deployment.schedule else None,
                        "tags": deployment.tags
                    }
                except Exception:
                    deployment_info = None
                
                # Get recent flow runs
                flow_runs = await client.read_flow_runs(
                    flow_run_filter=FlowRunFilter(
                        tags={"all_": [f"workflow_id:{workflow_id}"]}
                    ),
                    limit=10,
                    sort="CREATED_DESC"
                )
                
                recent_runs = []
                for run in flow_runs:
                    recent_runs.append({
                        "flow_run_id": str(run.id),
                        "state": run.state.type if run.state else "Unknown",
                        "state_name": run.state.name if run.state else "Unknown",
                        "start_time": run.start_time.isoformat() if run.start_time else None,
                        "end_time": run.end_time.isoformat() if run.end_time else None,
                        "duration": str(run.total_run_time) if run.total_run_time else None,
                        "tags": run.tags
                    })
                
                return {
                    "workflow_id": workflow_id,
                    "workflow_name": workflow.get("workflow_name"),
                    "prefect_status": workflow.get("prefect_status", "unknown"),
                    "deployment_info": deployment_info,
                    "recent_runs": recent_runs,
                    "total_runs": len(recent_runs)
                }
                
        except HTTPException:
            raise
        except Exception as e:
            if self.logger:
                self.logger.error(f"Failed to get workflow status: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Failed to get workflow status: {str(e)}")
    
    async def pause_workflow_schedule(self, workflow_id: str):
        """Pause scheduled execution of a workflow"""
        try:
            workflow = await self.workflow_collection.find_one({"workflow_id": workflow_id})
            
            if not workflow:
                raise HTTPException(status_code=404, detail=f"Workflow {workflow_id} not found")
            
            deployment_name = workflow.get("prefect_deployment_name")
            if not deployment_name:
                raise HTTPException(
                    status_code=400, 
                    detail=f"Workflow {workflow_id} does not have Prefect deployment"
                )
            
            async with get_client() as client:
                deployment = await client.read_deployment_by_name(deployment_name)
                await client.set_deployment_schedule_active(deployment.id, False)
            
            # Update workflow status in database
            await self.workflow_collection.update_one(
                {"workflow_id": workflow_id},
                {"$set": {"schedule_active": False, "schedule_paused_at": datetime.now()}}
            )
            
            if self.logger:
                self.logger.info(f"Paused schedule for workflow {workflow_id}")
            
            return {
                "status": "success",
                "message": f"Workflow {workflow_id} schedule paused",
                "workflow_id": workflow_id
            }
            
        except HTTPException:
            raise
        except Exception as e:
            if self.logger:
                self.logger.error(f"Failed to pause workflow schedule: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Failed to pause workflow schedule: {str(e)}")
    
    async def resume_workflow_schedule(self, workflow_id: str):
        """Resume scheduled execution of a workflow"""
        try:
            workflow = await self.workflow_collection.find_one({"workflow_id": workflow_id})
            
            if not workflow:
                raise HTTPException(status_code=404, detail=f"Workflow {workflow_id} not found")
            
            deployment_name = workflow.get("prefect_deployment_name")
            if not deployment_name:
                raise HTTPException(
                    status_code=400, 
                    detail=f"Workflow {workflow_id} does not have Prefect deployment"
                )
            
            async with get_client() as client:
                deployment = await client.read_deployment_by_name(deployment_name)
                await client.set_deployment_schedule_active(deployment.id, True)
            
            # Update workflow status in database
            await self.workflow_collection.update_one(
                {"workflow_id": workflow_id},
                {"$set": {"schedule_active": True, "schedule_resumed_at": datetime.now()}}
            )
            
            if self.logger:
                self.logger.info(f"Resumed schedule for workflow {workflow_id}")
            
            return {
                "status": "success",
                "message": f"Workflow {workflow_id} schedule resumed",
                "workflow_id": workflow_id
            }
            
        except HTTPException:
            raise
        except Exception as e:
            if self.logger:
                self.logger.error(f"Failed to resume workflow schedule: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Failed to resume workflow schedule: {str(e)}")
    
    async def delete_workflow_deployment(self, workflow_id: str, delete_runs: bool = False):
        """Delete Prefect deployment for a workflow"""
        try:
            workflow = await self.workflow_collection.find_one({"workflow_id": workflow_id})
            
            if not workflow:
                raise HTTPException(status_code=404, detail=f"Workflow {workflow_id} not found")
            
            deployment_name = workflow.get("prefect_deployment_name")
            if not deployment_name:
                return {
                    "status": "success",
                    "message": f"Workflow {workflow_id} has no Prefect deployment to delete"
                }
            
            async with get_client() as client:
                deployment = await client.read_deployment_by_name(deployment_name)
                
                # Delete flow runs if requested
                if delete_runs:
                    flow_runs = await client.read_flow_runs(
                        flow_run_filter=FlowRunFilter(
                            tags={"all_": [f"workflow_id:{workflow_id}"]}
                        )
                    )
                    
                    for run in flow_runs:
                        await client.delete_flow_run(run.id)
                    
                    if self.logger:
                        self.logger.info(f"Deleted {len(flow_runs)} flow runs for workflow {workflow_id}")
                
                # Delete deployment
                await client.delete_deployment(deployment.id)
            
            # Update workflow in database
            await self.workflow_collection.update_one(
                {"workflow_id": workflow_id},
                {"$unset": {
                    "prefect_deployment_id": "",
                    "prefect_deployment_name": "",
                    "prefect_flow_name": "",
                    "prefect_status": "",
                    "prefect_schedule": ""
                }}
            )
            
            if self.logger:
                self.logger.info(f"Deleted Prefect deployment for workflow {workflow_id}")
            
            return {
                "status": "success",
                "message": f"Prefect deployment deleted for workflow {workflow_id}",
                "workflow_id": workflow_id,
                "runs_deleted": delete_runs
            }
            
        except HTTPException:
            raise
        except Exception as e:
            if self.logger:
                self.logger.error(f"Failed to delete workflow deployment: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Failed to delete workflow deployment: {str(e)}")
    
    async def get_workflow_run_logs(self, workflow_id: str, flow_run_id: str):
        """Get logs for a specific workflow run"""
        try:
            async with get_client() as client:
                # Verify the flow run belongs to this workflow
                flow_run = await client.read_flow_run(flow_run_id)
                
                if f"workflow_id:{workflow_id}" not in flow_run.tags:
                    raise HTTPException(
                        status_code=403, 
                        detail="Flow run does not belong to specified workflow"
                    )
                
                # Get logs
                logs = await client.read_logs(
                    log_filter={"flow_run_id": {"any_": [flow_run_id]}}
                )
                
                formatted_logs = []
                for log in logs:
                    formatted_logs.append({
                        "timestamp": log.timestamp.isoformat(),
                        "level": log.level,
                        "message": log.message,
                        "task_run_id": str(log.task_run_id) if log.task_run_id else None,
                        "task_run_name": log.task_run_name
                    })
                
                return {
                    "workflow_id": workflow_id,
                    "flow_run_id": flow_run_id,
                    "flow_run_state": flow_run.state.type if flow_run.state else "Unknown",
                    "logs": formatted_logs,
                    "log_count": len(formatted_logs)
                }
                
        except HTTPException:
            raise
        except Exception as e:
            if self.logger:
                self.logger.error(f"Failed to get workflow run logs: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Failed to get workflow run logs: {str(e)}")


# FastAPI Router
def create_workflow_router(workflow_manager: WorkflowManager) -> APIRouter:
    """Create FastAPI router with workflow management endpoints"""
    
    router = APIRouter(prefix="/api/v1/workflows", tags=["workflows"])
    
    @router.post("/execute")
    async def execute_workflow(request: WorkflowExecuteRequest):
        """Execute a workflow on-demand"""
        return await workflow_manager.execute_workflow_adhoc(
            request.workflow_id, 
            request.parameters
        )
    
    @router.get("/{workflow_id}/status")
    async def get_workflow_status(workflow_id: str):
        """Get workflow status and recent runs"""
        return await workflow_manager.get_workflow_status(workflow_id)
    
    @router.post("/{workflow_id}/pause")
    async def pause_workflow(workflow_id: str):
        """Pause workflow schedule"""
        return await workflow_manager.pause_workflow_schedule(workflow_id)
    
    @router.post("/{workflow_id}/resume")
    async def resume_workflow(workflow_id: str):
        """Resume workflow schedule"""
        return await workflow_manager.resume_workflow_schedule(workflow_id)
    
    @router.delete("/{workflow_id}/deployment")
    async def delete_workflow_deployment(workflow_id: str, delete_runs: bool = False):
        """Delete workflow Prefect deployment"""
        return await workflow_manager.delete_workflow_deployment(workflow_id, delete_runs)
    
    @router.get("/{workflow_id}/runs/{flow_run_id}/logs")
    async def get_workflow_run_logs(workflow_id: str, flow_run_id: str):
        """Get logs for a specific workflow run"""
        return await workflow_manager.get_workflow_run_logs(workflow_id, flow_run_id)
    
    @router.get("/{workflow_id}/runs")
    async def get_workflow_runs(workflow_id: str, limit: int = 10):
        """Get recent workflow runs"""
        status_data = await workflow_manager.get_workflow_status(workflow_id)
        return {
            "workflow_id": workflow_id,
            "runs": status_data["recent_runs"][:limit]
        }
    
    return router


# Modified addWorkflow integration
async def enhanced_add_workflow(self, payload, user_obj, enable_prefect: bool = True, schedule: Optional[str] = None):
    """
    Enhanced version of your addWorkflow method with Prefect integration
    """
    try:
        # Your existing addWorkflow logic here...
        # (All the original validation, database operations, etc.)
        
        if not (payload.workflow_name and payload.folder_id and payload.job_id):
            raise Exception("Please enter the fields WORKFLOW NAME, FOLDER ID and JOB ID")

        self.logger.info(f"/workflow/ Insert Workflow API Call  --history-- {user_obj['fullName']}, {str(payload)}")

        # Original workflow creation logic
        from uuid import uuid4
        import time
        import re
        from bson import Regex
        
        payload = updateModelObject(payload, ["created_at"])
        setattr(payload, "workflow_id", str(uuid4()))

        workflowDict = payload.__dict__
        workflowDict["created_by"] = user_obj["UserId"]
        workflowDict["workflow_name"] = workflowDict["workflow_name"].strip()
        workflowDict["check_in"] = True

        # ... [Include all your existing workflow creation logic] ...
        
        # After successful workflow creation in your database
        if enable_prefect:
            try:
                from prefect_workflow_orchestrator import PrefectWorkflowOrchestrator
                
                orchestrator = PrefectWorkflowOrchestrator()
                prefect_result = await orchestrator.process_add_workflow(workflowDict, schedule)
                
                if prefect_result["status"] == "success":
                    # Update workflowDict with Prefect information
                    workflowDict.update({
                        "prefect_deployment_id": prefect_result["deployment_id"],
                        "prefect_deployment_name": prefect_result["deployment_name"],
                        "prefect_flow_name": prefect_result["flow_name"],
                        "prefect_status": "deployed",
                        "prefect_schedule": schedule,
                        "prefect_created_at": datetime.now().isoformat()
                    })
                    
                    # Update in MongoDB
                    await self.workflowCollection.update_one(
                        {"workflow_id": workflowDict["workflow_id"]},
                        {"$set": {
                            "prefect_deployment_id": prefect_result["deployment_id"],
                            "prefect_deployment_name": prefect_result["deployment_name"],
                            "prefect_flow_name": prefect_result["flow_name"],
                            "prefect_status": "deployed",
                            "prefect_schedule": schedule,
                            "prefect_created_at": datetime.now().isoformat()
                        }}
                    )
                    
                    self.logger.info(f"Prefect integration successful: {prefect_result}")
                else:
                    self.logger.error(f"Prefect integration failed: {prefect_result}")
                    workflowDict["prefect_status"] = "failed"
                    workflowDict["prefect_error"] = prefect_result.get("error", "Unknown error")
                    
            except Exception as prefect_error:
                self.logger.error(f"Prefect integration failed: {str(prefect_error)}")
                workflowDict["prefect_status"] = "failed"
                workflowDict["prefect_error"] = str(prefect_error)
        
        # Your existing return logic
        response = {"id": workflowDict}
        if enable_prefect and workflowDict.get("prefect_deployment_id"):
            response["prefect"] = {
                "deployment_id": workflowDict["prefect_deployment_id"],
                "deployment_name": workflowDict["prefect_deployment_name"],
                "flow_name": workflowDict["prefect_flow_name"],
                "status": workflowDict["prefect_status"]
            }
        
        return json.loads(json_util.dumps(response))
        
    except Exception as ex:
        self.logger.error(f"insert_workflow  ::      {str(ex)}", exc_info=True)
        if str(ex) == "token expired":
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail=str(ex))
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(ex))


# Usage example in your FastAPI app
"""
from fastapi import FastAPI
from workflow_management_api import create_workflow_router, WorkflowManager

app = FastAPI()

# Initialize workflow manager with your MongoDB collection
workflow_manager = WorkflowManager(workflow_collection=your_workflow_collection, logger=your_logger)

# Add workflow management endpoints
workflow_router = create_workflow_router(workflow_manager)
app.include_router(workflow_router)

# Now you can:
# POST /api/v1/workflows/execute - Execute workflow adhoc
# GET /api/v1/workflows/{workflow_id}/status - Get workflow status
# POST /api/v1/workflows/{workflow_id}/pause - Pause workflow
# POST /api/v1/workflows/{workflow_id}/resume - Resume workflow
# DELETE /api/v1/workflows/{workflow_id}/deployment - Delete deployment
# GET /api/v1/workflows/{workflow_id}/runs/{flow_run_id}/logs - Get run logs
"""

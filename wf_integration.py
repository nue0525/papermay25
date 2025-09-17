# workflow_integration.py
"""
Integration module to connect the existing addWorkflow method with Prefect orchestration
"""

import asyncio
import json
from typing import Dict, Any, Optional
from prefect_workflow_orchestrator import PrefectWorkflowOrchestrator


class WorkflowService:
    """Enhanced workflow service with Prefect integration"""
    
    def __init__(self):
        self.prefect_orchestrator = PrefectWorkflowOrchestrator()
        # Your existing attributes from the original class
        self.logger = None
        self.workflowCollection = None
        self.workspaceCollection = None
    
    async def addWorkflow(self, payload, user_obj, enable_prefect: bool = True, schedule: Optional[str] = None):
        """
        Enhanced addWorkflow method with Prefect integration
        
        Args:
            payload: Original workflow payload
            user_obj: User object
            enable_prefect: Whether to create Prefect flow and deployment
            schedule: Optional schedule for the workflow (e.g., "cron:0 2 * * *" or "interval:60")
        """
        
        try:
            # Original validation
            if not (payload.workflow_name and payload.folder_id and payload.job_id):
                raise Exception(
                    "Please enter the fields WORKFLOW NAME, FOLDER ID and JOB ID"
                )

            self.logger.info(
                f"/workflow/ Insert Workflow API Call  --history-- {user_obj['fullName']}, {str(payload)}"
            )

            # Original workflow creation logic (keeping all existing code)
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

            pattern = re.compile(r"\s+")
            workflowDict["workflow_name"] = re.sub(
                pattern, "", workflowDict["workflow_name"]
            )

            # Check for duplicates
            duplicate_workflow = await self.workflowCollection.find_one({
                "job_id": payload.job_id,
                "workflow_name": {"$regex": Regex(payload.workflow_name, "i")}
            })

            if duplicate_workflow:
                raise Exception("Duplicate")

            startTime = time.time()
            
            # Extract workflow components
            workspace_id = workflowDict["workspace_id"]
            folder_id = workflowDict["folder_id"]
            job_id = workflowDict["job_id"]
            workflow_id = workflowDict["workflow_id"]
            workflow_name = workflowDict["workflow_name"]
            
            # Original database operations continue here...
            # [Include all your existing database insertion code]
            
            # After successful workflow creation, integrate with Prefect
            if enable_prefect:
                try:
                    prefect_result = await self._create_prefect_workflow(
                        workflowDict, schedule
                    )
                    
                    # Store Prefect deployment info in workflow document
                    workflowDict["prefect_deployment_id"] = prefect_result.get("deployment_id")
                    workflowDict["prefect_flow_name"] = prefect_result.get("flow_name")
                    workflowDict["prefect_status"] = prefect_result.get("status")
                    
                    self.logger.info(f"Prefect integration successful: {prefect_result}")
                    
                except Exception as prefect_error:
                    self.logger.error(f"Prefect integration failed: {str(prefect_error)}")
                    # Continue with workflow creation even if Prefect fails
                    workflowDict["prefect_status"] = "failed"
                    workflowDict["prefect_error"] = str(prefect_error)
            
            # Continue with original workflow operations...
            await mongo_utils.addDashCount(ObjectTypeConstants.WORKFLOW, workflowDict)
            await user_activity_object.add_user_activity(
                object_id=workflowDict["workflow_id"],
                object_name=workflowDict["workflow_name"],
                user_object=user_obj,
                activity_type="Added",
                category=UserActivityConstants.WORKFLOW,
                object_type=ObjectTypeConstants.WORKFLOW,
                sub_category=None,
                previous_values=None,
                current_values=workflowDict,
                status="Successful",
            )
            
            # Rest of your original database operations...
            # [Include all remaining original code]
            
            # Insert into MongoDB
            await self.workflowCollection.insert_one(workflowDict)
            
            self.logger.info(
                f"API: /workflow/insert_workflow  ::   {str(time.time()-startTime)[:6]} sec"
            )
            
            await self.create_new_version(workflowDict, user_obj)

            # Enhanced response with Prefect info
            response = {"id": workflowDict}
            if enable_prefect and workflowDict.get("prefect_deployment_id"):
                response["prefect_deployment_id"] = workflowDict["prefect_deployment_id"]
                response["prefect_flow_name"] = workflowDict["prefect_flow_name"]
            
            return json.loads(json_util.dumps(response))
            
        except Exception as ex:
            self.logger.error(f"insert_workflow  ::      {str(ex)}", exc_info=True)
            if str(ex) == "token expired":
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED, detail=str(ex)
                )
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(ex))
    
    async def _create_prefect_workflow(self, workflow_dict: Dict[str, Any], schedule: Optional[str] = None):
        """Create Prefect workflow from workflow dictionary"""
        
        return await self.prefect_orchestrator.process_add_workflow(
            workflow_dict, schedule
        )
    
    async def execute_workflow(self, workflow_id: str, parameters: Optional[Dict[str, Any]] = None):
        """Execute a workflow on-demand using Prefect"""
        
        try:
            # Get workflow from database
            workflow = await self.workflowCollection.find_one({"workflow_id": workflow_id})
            
            if not workflow:
                raise Exception(f"Workflow {workflow_id} not found")
            
            deployment_id = workflow.get("prefect_deployment_id")
            if not deployment_id:
                raise Exception(f"Workflow {workflow_id} does not have Prefect deployment")
            
            # Trigger Prefect flow run
            from prefect.deployments import run_deployment
            
            flow_run = await run_deployment(
                name=workflow.get("prefect_flow_name", f"workflow_{workflow_id}"),
                parameters=parameters or {}
            )
            
            return {
                "status": "success",
                "flow_run_id": str(flow_run.id),
                "workflow_id": workflow_id,
                "message": "Workflow execution started successfully"
            }
            
        except Exception as e:
            self.logger.error(f"Workflow execution failed: {str(e)}")
            return {
                "status": "error",
                "error": str(e),
                "workflow_id": workflow_id
            }
    
    async def get_workflow_runs(self, workflow_id: str, limit: int = 10):
        """Get recent workflow runs from Prefect"""
        
        try:
            from prefect import get_client
            
            workflow = await self.workflowCollection.find_one({"workflow_id": workflow_id})
            if not workflow:
                raise Exception(f"Workflow {workflow_id} not found")
            
            flow_name = workflow.get("prefect_flow_name")
            if not flow_name:
                raise Exception(f"Workflow {workflow_id} does not have Prefect flow")
            
            async with get_client() as client:
                flow_runs = await client.read_flow_runs(
                    flow_filter={"name": {"any_": [flow_name]}},
                    limit=limit,
                    sort="CREATED_DESC"
                )
                
                return [
                    {
                        "flow_run_id": str(run.id),
                        "state": run.state.type,
                        "state_name": run.state.name,
                        "start_time": run.start_time.isoformat() if run.start_time else None,
                        "end_time": run.end_time.isoformat() if run.end_time else None,
                        "duration": str(run.total_run_time) if run.total_run_time else None
                    }
                    for run in flow_runs
                ]
                
        except Exception as e:
            self.logger.error(f"Failed to get workflow runs: {str(e)}")
            return []
    
    async def pause_workflow_schedule(self, workflow_id: str):
        """Pause scheduled workflow execution"""
        
        try:
            from prefect import get_client
            
            workflow = await self.workflowCollection.find_one({"workflow_id": workflow_id})
            if not workflow:
                raise Exception(f"Workflow {workflow_id} not found")
            
            deployment_id = workflow.get("prefect_deployment_id")
            if not deployment_id:
                raise Exception(f"Workflow {workflow_id} does not have Prefect deployment")
            
            async with get_client() as client:
                await client.set_deployment_schedule_active(deployment_id, False)
            
            # Update workflow status in database
            await self.workflowCollection.update_one(
                {"workflow_id": workflow_id},
                {"$set": {"schedule_active": False}}
            )
            
            return {
                "status": "success",
                "message": f"Workflow {workflow_id} schedule paused"
            }
            
        except Exception as e:
            return {
                "status": "error",
                "error": str(e)
            }
    
    async def resume_workflow_schedule(self, workflow_id: str):
        """Resume scheduled workflow execution"""
        
        try:
            from prefect import get_client
            
            workflow = await self.workflowCollection.find_one({"workflow_id": workflow_id})
            if not workflow:
                raise Exception(f"Workflow {workflow_id} not found")
            
            deployment_id = workflow.get("prefect_deployment_id")
            if not deployment_id:
                raise Exception(f"Workflow {workflow_id} does not have Prefect deployment")
            
            async with get_client() as client:
                await client.set_deployment_schedule_active(deployment_id, True)
            
            # Update workflow status in database
            await self.workflowCollection.update_one(
                {"workflow_id": workflow_id},
                {"$set": {"schedule_active": True}}
            )
            
            return {
                "status": "success",
                "message": f"Workflow {workflow_id} schedule resumed"
            }
            
        except Exception as e:
            return {
                "status": "error",
                "error": str(e)
            }


# API endpoints integration example
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

class WorkflowExecuteRequest(BaseModel):
    workflow_id: str

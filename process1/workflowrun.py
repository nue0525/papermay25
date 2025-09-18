"""
Refactored WorkflowRun Service with Prefect Integration
This replaces the Redis RQ-based workflow execution with Prefect flows
"""

import json
import uuid
import asyncio
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional
from fastapi import status, HTTPException

# Prefect imports
from prefect.client.orchestration import PrefectClient
from prefect.client.schemas.filters import FlowRunFilter, FlowFilter
from prefect.client.schemas.sorting import FlowRunSort
from prefect.deployments import run_deployment

# Existing imports
from Workflow_Runs.models import WorkflowRunSchema
from Connection.pg import create_connection
from psycopg2 import sql
from Utility.utils import getCurrentUTCtime
from APIs.Common.logger import getWorkflowRunsLogger
from UserManagement.Roles.service import Roles
from Settings.service import GlobalSettingsService

# Import our Prefect flows
from .prefect_flows import (
    workflow_execution_flow,
    metadata_storage_flow,
    get_workflow_metadata
)


class WorkflowRunPrefect:
    """
    Refactored WorkflowRun service using Prefect for orchestration
    Maintains API compatibility while using Prefect underneath
    """
    
    def __init__(self):
        self.logger = getWorkflowRunsLogger
        # Initialize Prefect client for programmatic interaction
        self.prefect_client = None
    
    async def _get_prefect_client(self) -> PrefectClient:
        """Get or create Prefect client"""
        if self.prefect_client is None:
            self.prefect_client = PrefectClient.from_settings()
        return self.prefect_client
    
    async def addWorkflowRun(
        self,
        payload: WorkflowRunSchema,
        user_obj: Dict[str, Any],
        fromJob: bool = False,
        job_run_id: Optional[str] = None,
        module: str = "workflows"
    ) -> Dict[str, Any]:
        """
        Start a workflow execution using Prefect flows
        This replaces the Redis RQ-based execution
        """
        try:
            self.logger.info(f"Starting Prefect workflow run for: {payload.workflow_id}")
            
            # Validate required fields
            if not (payload.job_id and payload.folder_id and payload.workflow_id):
                raise Exception("Please enter the field FOLDER ID, JOB ID AND WORKFLOW ID")
            
            # Check user permissions
            user_id = user_obj.get("UserId")
            if not user_id:
                raise Exception("User ID not found")
            
            roles_instance = Roles()
            required_role = await roles_instance.checkRolebyUserId(user_id, "Workflow Run")
            if not required_role:
                raise Exception("Permission Denied for Workflow Run")
            
            # Get workflow metadata from PostgreSQL (instead of MongoDB)
            workflow_metadata = await get_workflow_metadata(payload.workflow_id)
            if not workflow_metadata:
                raise Exception(f"Workflow not found: {payload.workflow_id}")
            
            workspace_info = workflow_metadata['workspace_info']
            connection_info_v1 = workflow_metadata['connection_info_v1']
            dependencies = workflow_metadata['dependencies']
            
            # Check if workflow is active and not already running
            await self._validate_workflow_state(payload.workflow_id, workspace_info['workflow_name'])
            
            # Generate IDs
            if not fromJob:
                job_run_id = str(uuid.uuid4())
            workflow_run_id = str(uuid.uuid4())
            
            # Prepare storage configuration
            storage_config = self._get_storage_config()
            
            # Prepare user information
            user_info = {
                'user_id': user_obj["UserId"],
                'user_name': user_obj.get("fullName", ""),
                'workspace_id': workspace_info['workspace_id'],
                'workspace_name': workspace_info['workspace_name'],
                'folder_id': workspace_info['folder_id'],
                'folder_name': workspace_info['folder_name'],
                'job_id': workspace_info['job_id'],
                'job_name': workspace_info['job_name'],
                'job_run_id': job_run_id,
                'workflow_name': workspace_info['workflow_name']
            }
            
            # Prepare notification configuration
            notification_config = None
            # You can extract this from the workflow configuration if needed
            
            # Start Prefect flow execution
            self.logger.info(f"Starting Prefect flow execution for workflow: {workspace_info['workflow_name']}")
            
            # Use Prefect client to run the flow
            client = await self._get_prefect_client()
            
            flow_run = await client.create_flow_run_from_deployment(
                deployment_id=None,  # You can create deployments for scheduled workflows
                flow_id=None,  # Will be determined by the flow function
                name=f"workflow-{workflow_run_id}",
                parameters={
                    "workflow_run_id": workflow_run_id,
                    "workflow_id": payload.workflow_id,
                    "workflow_name": workspace_info['workflow_name'],
                    "job_run_id": job_run_id,
                    "workspace_info": workspace_info,
            flow_run = await client.create_flow_run_from_deployment(
                deployment_id=None,  # You can create deployments for scheduled workflows
                flow_id=None,  # Will be determined by the flow function
                name=f"workflow-{workflow_run_id}",
                parameters={
                    "workflow_run_id": workflow_run_id,
                    "workflow_id": payload.workflow_id,
                    "workflow_name": workspace_info['workflow_name'],
                    "job_run_id": job_run_id,
                    "workspace_info": workspace_info,
                    "connection_info_v1": connection_info_v1,
                    "dependencies": dependencies,
                    "user_info": user_info,
                    "storage_config": storage_config,
                    "notification_config": notification_config
                }
            )
            
            # Alternative: Run flow directly (for immediate execution)
            flow_result = await workflow_execution_flow.with_options(
                name=f"workflow-{workflow_run_id}"
            ).submit(
                workflow_run_id=workflow_run_id,
                workflow_id=payload.workflow_id,
                workflow_name=workspace_info['workflow_name'],
                job_run_id=job_run_id,
                workspace_info=workspace_info,
                connection_info_v1=connection_info_v1,
                dependencies=dependencies,
                user_info=user_info,
                storage_config=storage_config,
                notification_config=notification_config
            )
            
            self.logger.info(f"Prefect flow started successfully: {flow_result}")
            
            # Return response in the same format as the original service
            return {
                "id": {
                    "workflow_run_id": workflow_run_id,
                    "job_run_id": job_run_id,
                    "prefect_flow_run_id": str(flow_result.id) if flow_result else None,
                    "workflow_id": payload.workflow_id,
                    "workflow_name": workspace_info['workflow_name'],
                    "status": "Running"
                }
            }
            
        except Exception as ex:
            self.logger.error(f"Error starting Prefect workflow: {str(ex)}", exc_info=True)
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(ex))
    
    async def getWorkflowRunWithFilters(
        self, 
        user_obj: Dict[str, Any], 
        is_download: bool = False, 
        **payload
    ) -> Dict[str, Any]:
        """
        Get workflow runs with enhanced Prefect integration
        Uses both PostgreSQL and Prefect API for comprehensive data
        """
        try:
            self.logger.info(f"Getting workflow runs with filters: {payload}")
            
            # Get data from PostgreSQL (faster for filtering and pagination)
            pg_results = await self._get_workflow_runs_from_postgres(**payload)
            
            # Optionally enrich with real-time Prefect data
            if not is_download:
                enriched_results = await self._enrich_with_prefect_data(pg_results)
                return enriched_results
            
            return pg_results
            
        except Exception as ex:
            self.logger.error(f"Error getting workflow runs: {str(ex)}", exc_info=True)
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(ex))
    
    async def _get_workflow_runs_from_postgres(self, **payload) -> Dict[str, Any]:
        """Get workflow runs from PostgreSQL with enhanced querying"""
        try:
            connection = create_connection()
            cursor = connection.cursor()
            
            # Build query with filters
            base_query = """
                SELECT 
                    wrs.workflow_run_id,
                    wrs.workflow_id,
                    wrs.workflow_name,
                    wrs.job_run_id,
                    wrs.job_name,
                    wrs.run_status,
                    wrs.start_timestamp,
                    wrs.end_timestamp,
                    wrs.run_by,
                    wrs.prefect_flow_run_id,
                    wrs.workspace_name,
                    wrs.folder_name,
                    COUNT(nrd.node_id) as total_nodes,
                    COUNT(CASE WHEN nrd.node_status = 'Completed' THEN 1 END) as completed_nodes,
                    COUNT(CASE WHEN nrd.node_status = 'Failed' THEN 1 END) as failed_nodes,
                    COUNT(CASE WHEN nrd.node_status = 'Running' THEN 1 END) as running_nodes,
                    SUM(nrd.target_rows) as records_processed
                FROM workflow_run_stats wrs
                LEFT JOIN node_run_details nrd ON wrs.workflow_run_id = nrd.workflow_run_id
            """
            
            where_conditions = []
            params = []
            
            # Add filters
            if payload.get("workflow_id"):
                where_conditions.append("wrs.workflow_id = %s")
                params.append(payload["workflow_id"])
            
            if payload.get("run_status"):
                where_conditions.append("wrs.run_status = %s")
                params.append(payload["run_status"])
            
            if payload.get("search_val"):
                where_conditions.append(
                    "(wrs.workflow_name ILIKE %s OR wrs.job_name ILIKE %s)"
                )
                search_term = f"%{payload['search_val']}%"
                params.extend([search_term, search_term])
            
            # Add date filters if provided
            if payload.get("start_date"):
                where_conditions.append("wrs.start_timestamp >= %s")
                params.append(payload["start_date"])
            
            if payload.get("end_date"):
                where_conditions.append("wrs.start_timestamp <= %s")
                params.append(payload["end_date"])
            
            # Build final query
            if where_conditions:
                base_query += " WHERE " + " AND ".join(where_conditions)
            
            base_query += """
                GROUP BY wrs.workflow_run_id, wrs.workflow_id, wrs.workflow_name,
                         wrs.job_run_id, wrs.job_name, wrs.run_status, wrs.start_timestamp,
                         wrs.end_timestamp, wrs.run_by, wrs.prefect_flow_run_id,
                         wrs.workspace_name, wrs.folder_name
                ORDER BY wrs.start_timestamp DESC
            """
            
            # Add pagination
            limit = payload.get("rows", 20)
            offset = payload.get("num_of_rows_to_skip", 0)
            base_query += f" LIMIT {limit} OFFSET {offset}"
            
            cursor.execute(base_query, params)
            results = cursor.fetchall()
            
            # Get total count
            count_query = """
                SELECT COUNT(DISTINCT wrs.workflow_run_id)
                FROM workflow_run_stats wrs
            """
            if where_conditions:
                count_query += " WHERE " + " AND ".join(where_conditions)
            
            cursor.execute(count_query, params[:len(where_conditions)])
            total_count = cursor.fetchone()[0]
            
            cursor.close()
            connection.close()
            
            # Format results
            items = []
            for result in results:
                item = {
                    "_id": {
                        "workflow_run_id": result[0],
                        "workflow_id": result[1],
                        "workflow_name": result[2],
                        "job_run_id": result[3],
                        "job_name": result[4],
                        "runStatus": result[5],
                        "startTimestamp": result[6].isoformat() if result[6] else None,
                        "end_timestamp": result[7].isoformat() if result[7] else None,
                        "runBy": {"name": result[8]},
                        "prefect_flow_run_id": result[9],
                        "workspace_name": result[10],
                        "folder_name": result[11],
                        "total_nodes": result[12] or 0,
                        "completed_nodes": result[13] or 0,
                        "failed_nodes": result[14] or 0,
                        "running_nodes": result[15] or 0,
                        "recordsProcessed": result[16] or 0
                    }
                }
                items.append(item)
            
            return {
                "Items": items,
                "total_records": {"totalDocs": total_count}
            }
            
        except Exception as ex:
            self.logger.error(f"Error querying PostgreSQL: {str(ex)}", exc_info=True)
            raise
    
    async def _enrich_with_prefect_data(self, pg_results: Dict[str, Any]) -> Dict[str, Any]:
        """Enrich PostgreSQL results with real-time Prefect data"""
        try:
            client = await self._get_prefect_client()
            
            for item in pg_results["Items"]:
                prefect_flow_run_id = item["_id"].get("prefect_flow_run_id")
                if prefect_flow_run_id:
                    # Get real-time flow run status from Prefect
                    try:
                        flow_run = await client.read_flow_run(prefect_flow_run_id)
                        if flow_run:
                            item["_id"]["prefect_state"] = flow_run.state.name
                            item["_id"]["prefect_state_message"] = flow_run.state.message
                    except Exception as ex:
                        self.logger.warning(f"Could not get Prefect data for {prefect_flow_run_id}: {str(ex)}")
            
            return pg_results
            
        except Exception as ex:
            self.logger.warning(f"Error enriching with Prefect data: {str(ex)}")
            # Return original data if Prefect enrichment fails
            return pg_results
    
    async def killWorkRunById(
        self, 
        job_run_id: str, 
        user_obj: Dict[str, Any]
    ) -> int:
        """
        Kill a running workflow using Prefect cancellation
        """
        try:
            self.logger.info(f"Killing workflow run: {job_run_id}")
            
            if not job_run_id:
                raise Exception("Please enter field JOB RUN ID")
            
            # Get workflow run details from PostgreSQL
            workflow_runs = await self._get_workflow_runs_by_job_id(job_run_id)
            
            client = await self._get_prefect_client()
            
            for workflow_run in workflow_runs:
                workflow_run_id = workflow_run['workflow_run_id']
                prefect_flow_run_id = workflow_run.get('prefect_flow_run_id')
                
                # Cancel Prefect flow run
                if prefect_flow_run_id:
                    try:
                        await client.set_flow_run_state(
                            flow_run_id=prefect_flow_run_id,
                            state={"type": "CANCELLED", "message": "Cancelled by user"}
                        )
                        self.logger.info(f"Cancelled Prefect flow run: {prefect_flow_run_id}")
                    except Exception as ex:
                        self.logger.error(f"Error cancelling Prefect flow: {str(ex)}")
                
                # Update database status
                await self._update_workflow_status(workflow_run_id, "Stopped", user_obj["UserId"])
            
            return status.HTTP_200_OK
            
        except Exception as ex:
            self.logger.error(f"Error killing workflow run: {str(ex)}", exc_info=True)
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(ex))
    
    async def _get_workflow_runs_by_job_id(self, job_run_id: str) -> List[Dict[str, Any]]:
        """Get workflow runs by job run ID"""
        try:
            connection = create_connection()
            cursor = connection.cursor()
            
            query = sql.SQL("""
                SELECT workflow_run_id, workflow_id, prefect_flow_run_id, run_status
                FROM workflow_run_stats
                WHERE job_run_id = %s
            """)
            
            cursor.execute(query, (job_run_id,))
            results = cursor.fetchall()
            
            cursor.close()
            connection.close()
            
            return [
                {
                    'workflow_run_id': result[0],
                    'workflow_id': result[1],
                    'prefect_flow_run_id': result[2],
                    'run_status': result[3]
                }
                for result in results
            ]
            
        except Exception as ex:
            self.logger.error(f"Error getting workflow runs by job ID: {str(ex)}")
            return []
    
    async def _update_workflow_status(
        self, 
        workflow_run_id: str, 
        status: str, 
        updated_by: str
    ):
        """Update workflow status in database"""
        try:
            connection = create_connection()
            cursor = connection.cursor()
            
            query = sql.SQL("""
                UPDATE workflow_run_stats 
                SET run_status = %s, 
                    end_timestamp = %s,
                    updated_by = %s
                WHERE workflow_run_id = %s
            """)
            
            cursor.execute(query, (
                status, 
                getCurrentUTCtime(), 
                updated_by, 
                workflow_run_id
            ))
            
            # Also update node statuses
            node_query = sql.SQL("""
                UPDATE node_run_details 
                SET node_status = %s,
                    node_end_timestamp = %s
                WHERE workflow_run_id = %s AND node_status = 'Running'
            """)
            
            cursor.execute(node_query, (
                status, 
                getCurrentUTCtime(), 
                workflow_run_id
            ))
            
            connection.commit()
            cursor.close()
            connection.close()
            
        except Exception as ex:
            self.logger.error(f"Error updating workflow status: {str(ex)}")
    
    async def _validate_workflow_state(self, workflow_id: str, workflow_name: str):
        """Validate that workflow can be executed"""
        try:
            connection = create_connection()
            cursor = connection.cursor()
            
            # Check if workflow is already running
            query = sql.SQL("""
                SELECT COUNT(*) 
                FROM workflow_run_stats 
                WHERE workflow_id = %s AND run_status = 'Running'
            """)
            
            cursor.execute(query, (workflow_id,))
            running_count = cursor.fetchone()[0]
            
            if running_count > 0:
                raise Exception(f"Workflow {workflow_name} is already running")
            
            cursor.close()
            connection.close()
            
        except Exception as ex:
            if "already running" in str(ex):
                raise
            self.logger.warning(f"Could not validate workflow state: {str(ex)}")
    
    def _get_storage_config(self) -> Dict[str, Any]:
        """Get storage configuration for the workflow"""
        # Import your storage configuration
        from Utility.config import default_bucket, key_path, GCS_Integration
        
        storage_config = {
            "bucket_name": default_bucket,
            "gcs_integration": GCS_Integration
        }
        
        # Load GCS credentials
        try:
            with open(key_path) as key_json:
                storage_config["gcs_creds"] = json.load(key_json)
        except Exception as ex:
            self.logger.warning(f"Could not load GCS credentials: {str(ex)}")
        
        return storage_config
    
    # Enhanced reporting methods using PostgreSQL queries
    async def getWorkflowRunReport(
        self, 
        workspace_id: str, 
        folder_id: str, 
        job_id: str, 
        workflow_id: str
    ) -> Dict[str, Any]:
        """
        Get comprehensive workflow run report using PostgreSQL queries
        Enhanced with Prefect execution metrics
        """
        try:
            connection = create_connection()
            cursor = connection.cursor()
            
            # Get overview data with Prefect integration
            overview_query = """
                SELECT 
                    wh.workspace_id, wh.workspace_name, wh.folder_id, wh.folder_name,
                    wh.job_id, wh.job_name, wh.workflow_id, wh.workflow_name,
                    wh.created_at, wh.created_by,
                    wrs.workflow_run_id, wrs.start_timestamp, wrs.end_timestamp,
                    wrs.run_by, wrs.run_status, wrs.prefect_flow_run_id,
                    COUNT(wn.node_id) as total_nodes,
                    COUNT(CASE WHEN nrd.node_status = 'Completed' THEN 1 END) as completed_tasks,
                    COUNT(CASE WHEN nrd.node_status = 'Running' THEN 1 END) as running_tasks,
                    COUNT(CASE WHEN nrd.node_status = 'Failed' THEN 1 END) as failed_tasks,
                    SUM(nrd.target_rows) as total_rows_processed,
                    AVG(nrd.total_run_time_sec) as avg_execution_time,
                    SUM(nrd.memory_usage_mb) as total_memory_used
                FROM workflow_hierarchy wh
                LEFT JOIN workflow_nodes wn ON wh.workflow_id = wn.workflow_id
                LEFT JOIN workflow_run_stats wrs ON wh.workflow_id = wrs.workflow_id
                    AND wrs.start_timestamp = (
                        SELECT MAX(start_timestamp) 
                        FROM workflow_run_stats 
                        WHERE workflow_id = wh.workflow_id
                    )
                LEFT JOIN node_run_details nrd ON wrs.workflow_run_id = nrd.workflow_run_id
                WHERE wh.workspace_id = %s AND wh.folder_id = %s 
                    AND wh.job_id = %s AND wh.workflow_id = %s
                GROUP BY wh.workspace_id, wh.workspace_name, wh.folder_id, wh.folder_name,
                         wh.job_id, wh.job_name, wh.workflow_id, wh.workflow_name,
                         wh.created_at, wh.created_by, wrs.workflow_run_id,
                         wrs.start_timestamp, wrs.end_timestamp, wrs.run_by, 
                         wrs.run_status, wrs.prefect_flow_run_id
            """
            
            cursor.execute(overview_query, (workspace_id, folder_id, job_id, workflow_id))
            overview_result = cursor.fetchone()
            
            if not overview_result:
                return {"error": "Workflow not found"}
            
            # Get execution metrics
            metrics_query = """
                SELECT 
                    metric_name, metric_value, metric_unit, metric_type,
                    recorded_at
                FROM workflow_execution_metrics
                WHERE workflow_id = %s
                ORDER BY recorded_at DESC
                LIMIT 50
            """
            
            cursor.execute(metrics_query, (workflow_id,))
            metrics_results = cursor.fetchall()
            
            # Get historical runs
            history_query = """
                SELECT 
                    workflow_run_id, run_status, start_timestamp, end_timestamp,
                    run_by, prefect_flow_run_id,
                    EXTRACT(epoch FROM (end_timestamp - start_timestamp)) as runtime_seconds
                FROM workflow_run_stats
                WHERE workflow_id = %s
                ORDER BY start_timestamp DESC
                LIMIT 20
            """
            
            cursor.execute(history_query, (workflow_id,))
            history_results = cursor.fetchall()
            
            cursor.close()
            connection.close()
            
            # Format response
            response = {
                "overview_card": {
                    "workspace_id": overview_result[0],
                    "workspace_name": overview_result[1],
                    "folder_id": overview_result[2],
                    "folder_name": overview_result[3],
                    "job_id": overview_result[4],
                    "job_name": overview_result[5],
                    "workflow_id": overview_result[6],
                    "workflow_name": overview_result[7],
                    "created_at": overview_result[8].isoformat() if overview_result[8] else None,
                    "created_by": overview_result[9],
                    "last_run_id": overview_result[10],
                    "last_run_at": overview_result[11].isoformat() if overview_result[11] else None,
                    "last_run_status": overview_result[14],
                    "prefect_flow_run_id": overview_result[15]
                },
                "performance_metrics": {
                    "total_nodes": overview_result[16] or 0,
                    "completed_tasks": overview_result[17] or 0,
                    "running_tasks": overview_result[18] or 0,
                    "failed_tasks": overview_result[19] or 0,
                    "total_rows_processed": overview_result[20] or 0,
                    "avg_execution_time": float(overview_result[21] or 0),
                    "total_memory_used": float(overview_result[22] or 0)
                },
                "execution_metrics": [
                    {
                        "metric_name": result[0],
                        "metric_value": float(result[1]),
                        "metric_unit": result[2],
                        "metric_type": result[3],
                        "recorded_at": result[4].isoformat()
                    }
                    for result in metrics_results
                ],
                "historical_runs": [
                    {
                        "workflow_run_id": result[0],
                        "run_status": result[1],
                        "start_timestamp": result[2].isoformat() if result[2] else None,
                        "end_timestamp": result[3].isoformat() if result[3] else None,
                        "run_by": result[4],
                        "prefect_flow_run_id": result[5],
                        "runtime_seconds": float(result[6] or 0)
                    }
                    for result in history_results
                ]
            }
            
            return response
            
        except Exception as ex:
            self.logger.error(f"Error getting workflow report: {str(ex)}", exc_info=True)
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(ex))

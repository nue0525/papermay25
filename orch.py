# prefect workf orchestrator
import json
import uuid
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, timedelta
import asyncio
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine, text
from prefect import flow, task, get_run_logger
from prefect.deployments import Deployment
from prefect.server.schemas.schedules import CronSchedule, IntervalSchedule
from prefect.blocks.system import Secret
from prefect.task_runners import SequentialTaskRunner, ConcurrentTaskRunner
from prefect.futures import PrefectFuture
import psycopg2
from contextlib import contextmanager
import re


class PrefectWorkflowOrchestrator:
    """
    Converts workflow definitions into Prefect flows with proper task dependencies
    """
    
    def __init__(self):
        self.logger = None
        self.connection_configs = {}
        
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
    
    def build_execution_order(self, dependencies: Dict[str, List[str]]) -> List[List[str]]:
        """
        Build execution order based on dependencies using topological sort
        Returns list of lists where each inner list contains nodes that can run in parallel
        """
        # Create a mapping of node dependencies
        in_degree = {}
        all_nodes = set(dependencies.keys())
        
        # Initialize in-degree count
        for node in all_nodes:
            in_degree[node] = 0
        
        # Calculate in-degrees
        for node, parents in dependencies.items():
            in_degree[node] = len(parents)
        
        # Find execution order using Kahn's algorithm
        execution_levels = []
        queue = [node for node in all_nodes if in_degree[node] == 0]
        
        while queue:
            current_level = queue.copy()
            execution_levels.append(current_level)
            queue.clear()
            
            for node in current_level:
                # Remove this node and update in-degrees of dependent nodes
                for dependent_node, parents in dependencies.items():
                    if node in parents:
                        in_degree[dependent_node] -= 1
                        if in_degree[dependent_node] == 0 and dependent_node not in [item for sublist in execution_levels for item in sublist]:
                            queue.append(dependent_node)
        
        return execution_levels
    
    def get_database_connection_string(self, db_details: Dict[str, Any], connector_type: str) -> str:
        """Generate database connection string based on connector type and details"""
        if connector_type == "postgresql":
            # You'll need to configure these from your environment or secrets
            host = "localhost"  # Configure from environment
            port = "5432"
            username = "your_username"  # Configure from environment  
            password = "your_password"  # Configure from secrets
            database = db_details.get("db", "")
            
            return f"postgresql://{username}:{password}@{host}:{port}/{database}"
        
        # Add other database types as needed
        return ""
    
    def create_extract_task(self, node_id: str, node_info: Dict[str, Any]):
        """Create data extraction task for source nodes"""
        
        @task(name=f"extract_{node_info.get('node_name', node_id)}")
        async def extract_data() -> pd.DataFrame:
            logger = get_run_logger()
            try:
                db_details = node_info.get("dbDetails", {})
                connector_type = node_info.get("connector_type", "")
                
                logger.info(f"Extracting data from {node_info.get('node_name', node_id)}")
                
                if connector_type == "postgresql":
                    conn_string = self.get_database_connection_string(db_details, connector_type)
                    engine = create_engine(conn_string)
                    
                    # Build query based on node configuration
                    schema = db_details.get("schema", "public")
                    table = db_details.get("table", "")
                    filter_condition = db_details.get("filter", "")
                    sql_query = db_details.get("sql", "")
                    
                    if sql_query:
                        query = sql_query
                    else:
                        query = f"SELECT * FROM {schema}.{table}"
                        if filter_condition:
                            query += f" WHERE {filter_condition}"
                    
                    logger.info(f"Executing query: {query}")
                    df = pd.read_sql(query, engine)
                    engine.dispose()
                    
                    logger.info(f"Extracted {len(df)} rows, {len(df.columns)} columns from {table}")
                    return df
                
                return pd.DataFrame()
                
            except Exception as e:
                logger.error(f"Data extraction failed for node {node_id}: {str(e)}")
                raise
        
        return extract_data
    
    def create_transform_task(self, node_id: str, node_info: Dict[str, Any]):
        """Create data transformation task"""
        
        @task(name=f"transform_{node_info.get('node_name', node_id)}")
        async def transform_data(input_data: pd.DataFrame) -> pd.DataFrame:
            logger = get_run_logger()
            try:
                connector_type = node_info.get("connector_type", "")
                transformations = node_info.get("transformations", {})
                field_mapping = node_info.get("field_mapping", [])
                
                logger.info(f"Transforming data for {node_info.get('node_name', node_id)}")
                logger.info(f"Input data shape: {input_data.shape}")
                
                df = input_data.copy()
                
                if connector_type == "filter":
                    # Apply filter transformations
                    transform_configs = transformations.get("transformations", [])
                    
                    for config in transform_configs:
                        if config.get("selected") and config.get("expression"):
                            expression_info = config["expression"]
                            if isinstance(expression_info, dict):
                                expression = expression_info.get("value", "")
                            else:
                                expression = str(expression_info)
                            
                            if expression:
                                logger.info(f"Applying filter: {expression}")
                                # Parse and apply the expression
                                try:
                                    # Handle simple comparisons (>, <, =, etc.)
                                    if ">" in expression:
                                        parts = expression.split(">")
                                        column = parts[0].strip()
                                        value = float(parts[1].strip())
                                        df = df[df[column] > value]
                                    elif "<" in expression:
                                        parts = expression.split("<")
                                        column = parts[0].strip()
                                        value = float(parts[1].strip())
                                        df = df[df[column] < value]
                                    elif "=" in expression and "!=" not in expression:
                                        parts = expression.split("=")
                                        column = parts[0].strip()
                                        value = parts[1].strip().strip("'\"")
                                        df = df[df[column] == value]
                                    
                                    logger.info(f"Filter applied. Rows after filter: {len(df)}")
                                    
                                except Exception as filter_error:
                                    logger.warning(f"Failed to apply filter {expression}: {str(filter_error)}")
                
                # Apply field mapping and column selection
                if field_mapping:
                    selected_columns = []
                    column_renames = {}
                    
                    for mapping in field_mapping:
                        if mapping.get("selected", False):
                            source_col = mapping.get("value") or mapping.get("name")
                            target_col = mapping.get("target", source_col)
                            
                            if source_col in df.columns:
                                selected_columns.append(source_col)
                                if target_col != source_col:
                                    column_renames[source_col] = target_col
                                
                                # Apply expressions if any
                                expression = mapping.get("expression", "")
                                if expression and expression.strip():
                                    try:
                                        # Handle simple expressions
                                        df[source_col] = df.eval(expression)
                                    except Exception as expr_error:
                                        logger.warning(f"Failed to apply expression {expression}: {str(expr_error)}")
                    
                    # Select only mapped columns
                    if selected_columns:
                        df = df[selected_columns]
                    
                    # Rename columns
                    if column_renames:
                        df = df.rename(columns=column_renames)
                        logger.info(f"Renamed columns: {column_renames}")
                
                logger.info(f"Transformation completed. Output shape: {df.shape}")
                return df
                
            except Exception as e:
                logger.error(f"Data transformation failed for node {node_id}: {str(e)}")
                raise
        
        return transform_data
    
    def create_load_task(self, node_id: str, node_info: Dict[str, Any]):
        """Create data loading task for target nodes"""
        
        @task(name=f"load_{node_info.get('node_name', node_id)}")
        async def load_data(input_data: pd.DataFrame) -> Dict[str, Any]:
            logger = get_run_logger()
            try:
                db_details = node_info.get("dbDetails", {})
                connector_type = node_info.get("connector_type", "")
                field_mapping = node_info.get("field_mapping", [])
                
                logger.info(f"Loading data to {node_info.get('node_name', node_id)}")
                logger.info(f"Input data shape: {input_data.shape}")
                
                df = input_data.copy()
                
                # Apply field mapping for target
                if field_mapping:
                    selected_columns = []
                    column_renames = {}
                    
                    for mapping in field_mapping:
                        if mapping.get("selected", False):
                            source_col = mapping.get("value") or mapping.get("name")
                            target_col = mapping.get("target", source_col)
                            
                            if source_col in df.columns:
                                selected_columns.append(source_col)
                                if target_col != source_col:
                                    column_renames[source_col] = target_col
                    
                    if selected_columns:
                        df = df[selected_columns]
                    
                    if column_renames:
                        df = df.rename(columns=column_renames)
                
                if connector_type == "postgresql":
                    conn_string = self.get_database_connection_string(db_details, connector_type)
                    engine = create_engine(conn_string)
                    
                    schema = db_details.get("schema", "public")
                    table = db_details.get("table", "")
                    truncate = db_details.get("truncateTable", False)
                    
                    # Truncate table if specified
                    if truncate:
                        with engine.connect() as conn:
                            conn.execute(text(f"TRUNCATE TABLE {schema}.{table}"))
                            conn.commit()
                            logger.info(f"Truncated table {schema}.{table}")
                    
                    # Load data
                    df.to_sql(
                        table, 
                        engine, 
                        schema=schema, 
                        if_exists='append', 
                        index=False,
                        method='multi',
                        chunksize=1000
                    )
                    
                    engine.dispose()
                    logger.info(f"Successfully loaded {len(df)} rows to {schema}.{table}")
                    
                    return {
                        "status": "success",
                        "rows_loaded": len(df),
                        "target_table": f"{schema}.{table}",
                        "node_id": node_id
                    }
                
                return {
                    "status": "skipped",
                    "reason": f"Unsupported connector type: {connector_type}",
                    "node_id": node_id
                }
                
            except Exception as e:
                logger.error(f"Data loading failed for node {node_id}: {str(e)}")
                raise
        
        return load_data
    
    def create_workflow_flow(self, workflow_data: Dict[str, Any]):
        """Create the main Prefect flow for the workflow"""
        
        workflow_id = workflow_data["workflow_id"]
        workflow_name = workflow_data["workflow_name"]
        connection_info = workflow_data["connection_info"]
        dependencies = workflow_data["dependencies"]
        
        # Build execution order
        execution_levels = self.build_execution_order(dependencies)
        
        @flow(
            name=f"workflow_{workflow_name}_{workflow_id[:8]}",
            task_runner=ConcurrentTaskRunner(),
            retries=1,
            retry_delay_seconds=30
        )
        async def workflow_flow():
            logger = get_run_logger()
            logger.info(f"Starting workflow: {workflow_name} (ID: {workflow_id})")
            logger.info(f"Execution order: {execution_levels}")
            
            # Store results from each node
            node_results = {}
            execution_summary = {
                "workflow_id": workflow_id,
                "workflow_name": workflow_name,
                "started_at": datetime.now().isoformat(),
                "nodes_processed": 0,
                "nodes_failed": 0,
                "execution_details": []
            }
            
            try:
                # Execute nodes level by level based on dependencies
                for level_index, level_nodes in enumerate(execution_levels):
                    logger.info(f"Executing level {level_index + 1}/{len(execution_levels)}: {level_nodes}")
                    
                    # Process all nodes in current level
                    for node_id in level_nodes:
                        if node_id not in connection_info:
                            logger.warning(f"Node {node_id} not found in connection info")
                            continue
                        
                        node_info = connection_info[node_id]
                        connector_type = node_info.get("connector_type", "")
                        node_name = node_info.get("node_name", node_id)
                        sequence = node_info.get("sequence", 0)
                        
                        logger.info(f"Processing node: {node_name} (Type: {connector_type}, Sequence: {sequence})")
                        
                        node_start_time = datetime.now()
                        
                        try:
                            if connector_type in ["postgresql", "mysql", "oracle"] and sequence == 1:
                                # Source node - extract data
                                extract_task = self.create_extract_task(node_id, node_info)
                                data = await extract_task()
                                node_results[node_id] = data
                                
                                execution_summary["execution_details"].append({
                                    "node_id": node_id,
                                    "node_name": node_name,
                                    "operation": "extract",
                                    "status": "success",
                                    "rows": len(data),
                                    "columns": len(data.columns) if not data.empty else 0,
                                    "duration": str(datetime.now() - node_start_time)
                                })
                                
                            elif connector_type in ["filter", "join", "aggregate"]:
                                # Transformation node
                                transform_task = self.create_transform_task(node_id, node_info)
                                
                                # Get input data from parent nodes
                                parent_nodes = dependencies.get(node_id, [])
                                if parent_nodes and parent_nodes[0] in node_results:
                                    input_data = node_results[parent_nodes[0]]
                                    transformed_data = await transform_task(input_data)
                                    node_results[node_id] = transformed_data
                                    
                                    execution_summary["execution_details"].append({
                                        "node_id": node_id,
                                        "node_name": node_name,
                                        "operation": "transform",
                                        "status": "success",
                                        "input_rows": len(input_data),
                                        "output_rows": len(transformed_data),
                                        "duration": str(datetime.now() - node_start_time)
                                    })
                                else:
                                    logger.warning(f"No input data available for transformation node {node_id}")
                                    
                            elif connector_type in ["postgresql", "mysql", "oracle"] and sequence > 1:
                                # Target node - load data
                                load_task = self.create_load_task(node_id, node_info)
                                
                                # Get input data from parent nodes
                                parent_nodes = dependencies.get(node_id, [])
                                if parent_nodes and parent_nodes[0] in node_results:
                                    input_data = node_results[parent_nodes[0]]
                                    load_result = await load_task(input_data)
                                    node_results[node_id] = load_result
                                    
                                    execution_summary["execution_details"].append({
                                        "node_id": node_id,
                                        "node_name": node_name,
                                        "operation": "load",
                                        "status": load_result.get("status", "unknown"),
                                        "rows_loaded": load_result.get("rows_loaded", 0),
                                        "target_table": load_result.get("target_table", ""),
                                        "duration": str(datetime.now() - node_start_time)
                                    })
                                else:
                                    logger.warning(f"No input data available for load node {node_id}")
                            
                            execution_summary["nodes_processed"] += 1
                            
                        except Exception as node_error:
                            logger.error(f"Node {node_id} failed: {str(node_error)}")
                            execution_summary["nodes_failed"] += 1
                            execution_summary["execution_details"].append({
                                "node_id": node_id,
                                "node_name": node_name,
                                "operation": "failed",
                                "status": "error",
                                "error": str(node_error),
                                "duration": str(datetime.now() - node_start_time)
                            })
                            # Continue processing other nodes
                
                execution_summary["completed_at"] = datetime.now().isoformat()
                execution_summary["status"] = "completed" if execution_summary["nodes_failed"] == 0 else "completed_with_errors"
                
                logger.info(f"Workflow {workflow_name} completed. Processed: {execution_summary['nodes_processed']}, Failed: {execution_summary['nodes_failed']}")
                return execution_summary
                
            except Exception as e:
                execution_summary["status"] = "failed"
                execution_summary["error"] = str(e)
                execution_summary["completed_at"] = datetime.now().isoformat()
                logger.error(f"Workflow {workflow_name} failed: {str(e)}")
                raise
        
        return workflow_flow
    
    def create_deployment(self, workflow_flow, workflow_data: Dict[str, Any], schedule: Optional[str] = None):
        """Create Prefect deployment for the workflow"""
        
        workflow_name = workflow_data["workflow_name"]
        workflow_id = workflow_data["workflow_id"]
        
        deployment_name = f"{workflow_name}_{workflow_id[:8]}"
        
        # Configure schedule if provided
        schedule_config = None
        if schedule:
            if schedule.startswith("cron:"):
                cron_expression = schedule.replace("cron:", "")
                schedule_config = CronSchedule(cron=cron_expression)
            elif schedule.startswith("interval:"):
                interval_minutes = int(schedule.replace("interval:", ""))
                schedule_config = IntervalSchedule(interval=timedelta(minutes=interval_minutes))
        
        deployment = Deployment.build_from_flow(
            flow=workflow_flow,
            name=deployment_name,
            schedule=schedule_config,
            work_pool_name="default-agent-pool",
            tags=[
                f"workflow_id:{workflow_id}",
                f"workspace_id:{workflow_data['workspace_id']}",
                f"folder_id:{workflow_data['folder_id']}",
                f"job_id:{workflow_data['job_id']}",
                "auto-generated"
            ],
            parameters={
                "workflow_metadata": {
                    "workflow_id": workflow_id,
                    "workflow_name": workflow_name,
                    "workspace_id": workflow_data["workspace_id"],
                    "folder_id": workflow_data["folder_id"],
                    "job_id": workflow_data["job_id"],
                    "created_at": datetime.now().isoformat()
                }
            },
            description=f"Auto-generated Prefect deployment for workflow: {workflow_name}"
        )
        
        return deployment
    
    async def process_add_workflow(self, workflow_dict: Dict[str, Any], schedule: Optional[str] = None):
        """
        Main method to process addWorkflow and create Prefect flow and deployment
        """
        try:
            # Parse workflow data
            workflow_data = self.parse_workflow_data(workflow_dict)
            
            # Create the workflow flow
            workflow_flow = self.create_workflow_flow(workflow_data)
            
            # Create deployment
            deployment = self.create_deployment(workflow_flow, workflow_data, schedule)
            
            # Apply deployment
            deployment_id = await deployment.apply()
            
            return {
                "status": "success",
                "workflow_id": workflow_data["workflow_id"],
                "deployment_id": str(deployment_id),
                "deployment_name": deployment.name,
                "flow_name": workflow_flow.name,
                "schedule": schedule,
                "node_count": len(workflow_data["connection_info"]),
                "execution_levels": len(self.build_execution_order(workflow_data["dependencies"])),
                "message": f"Workflow '{workflow_data['workflow_name']}' successfully converted to Prefect flow and deployed"
            }
            
        except Exception as e:
            return {
                "status": "error",
                "workflow_id": workflow_dict.get("workflow_id", "unknown"),
                "error": str(e),
                "message": f"Failed to process workflow: {str(e)}"
            }


# Integration with your existing addWorkflow method
async def integrate_with_add_workflow(self, payload, user_obj, enable_prefect: bool = True, schedule: Optional[str] = None):
    """
    Call this function at the end of your existing addWorkflow method
    """
    if enable_prefect:
        try:
            orchestrator = PrefectWorkflowOrchestrator()
            
            # Create workflowDict as you do in your existing method
            workflowDict = payload.__dict__
            # ... your existing workflow creation logic ...
            
            # After successful workflow creation, integrate with Prefect
            prefect_result = await orchestrator.process_add_workflow(workflowDict, schedule)
            
            # Store Prefect deployment info in your workflow document
            if prefect_result["status"] == "success":
                # Update workflowDict with Prefect information
                workflowDict["prefect_deployment_id"] = prefect_result["deployment_id"]
                workflowDict["prefect_deployment_name"] = prefect_result["deployment_name"]
                workflowDict["prefect_flow_name"] = prefect_result["flow_name"]
                workflowDict["prefect_status"] = "deployed"
                workflowDict["prefect_schedule"] = schedule
                
                # Update in MongoDB
                await self.workflowCollection.update_one(
                    {"workflow_id": workflowDict["workflow_id"]},
                    {"$set": {
                        "prefect_deployment_id": prefect_result["deployment_id"],
                        "prefect_deployment_name": prefect_result["deployment_name"],
                        "prefect_flow_name": prefect_result["flow_name"],
                        "prefect_status": "deployed",
                        "prefect_schedule": schedule
                    }}
                )
                
                self.logger.info(f"Prefect integration successful for workflow {workflowDict['workflow_id']}")
                return prefect_result
            else:
                self.logger.error(f"Prefect integration failed: {prefect_result['error']}")
                return prefect_result
                
        except Exception as e:
            self.logger.error(f"Prefect integration failed: {str(e)}")
            return {"status": "error", "error": str(e)}
    
    return {"status": "skipped", "reason": "Prefect integration disabled"}


# Example usage with your specific workflow structure
async def main():
    """Example usage with your workflow structure"""
    
    # Sample workflow data based on your structure
    sample_workflow = {
        "workflow_id": str(uuid.uuid4()),
        "workflow_name": "Employee_Data_Pipeline",
        "workspace_id": "workspace-123",
        "folder_id": "folder-456", 
        "job_id": "job-789",
        "connectionInformation_v1": json.dumps({
            "c1c446d1-c8a2-484e-babf-6d371fe8cdf9": {
                "connector_id": "fc0333a6-4b8d-4186-82cb-b82db7656dc0",
                "connector_type": "postgresql",
                "node_name": "PostgreSQL_Source",
                "connector_category": "Databases",
                "dbDetails": {
                    "db": "neondb",
                    "filter": "",
                    "schema": "public",
                    "sql": "",
                    "table": "girish_groupby_src",
                    "type": "sql",
                    "onErrors": "fail"
                },
                "transformations": {},
                "field_mapping": [],
                "sequence": 1
            },
            "5b60ac6b-d2a9-42eb-80ba-9ef33a79d5f7": {
                "connector_type": "filter",
                "node_name": "Filter_Transform",
                "connector_category": "Transformations",
                "dbDetails": {},
                "transformations": {
                    "transformations": [{
                        "value": "employee_id",
                        "name": "employee_id",
                        "type": "integer",
                        "selected": True,
                        "expression": {"value": "employee_id>1", "html": "employee_id&gt;1"}
                    }]
                },
                "field_mapping": [
                    {"value": "employee_id", "selected": True, "target": "employee_id"},
                    {"value": "first_name", "selected": True, "target": "first_name"},
                    {"value": "last_name", "selected": True, "target": "last_name"},
                    {"value": "place", "selected": True, "target": "place"}
                ],
                "sequence": 2
            },
            "eb514ced-7ef8-4281-8e78-1ed301295a68": {
                "connector_id": "fc0333a6-4b8d-4186-82cb-b82db7656dc0",
                "connector_type": "postgresql",
                "node_name": "PostgreSQL_Target",
                "connector_category": "Databases",
                "dbDetails": {
                    "db": "neondb",
                    "schema": "public",
                    "table": "girish_groupby_tgt",
                    "truncateTable": True,
                    "onErrors": "fail"
                },
                "field_mapping": [
                    {"value": "employee_id", "selected": True, "target": "employee_id"},
                    {"value": "first_name", "selected": True, "target": "first_name"},
                    {"value": "last_name", "selected": True, "target": "last_name"},
                    {"value": "place", "selected": True, "target": "place"}
                ],
                "sequence": 3
            }
        }),
        "dependencies": json.dumps({
            "c1c446d1-c8a2-484e-babf-6d371fe8cdf9": [],
            "5b60ac6b-d2a9-42eb-80ba-9ef33a79d5f7": ["c1c446d1-c8a2-484e-babf-6d371fe8cdf9"],
            "eb514ced-7ef8-4281-8e78-1ed301295a68": ["5b60ac6b-d2a9-42eb-80ba-9ef33a79d5f7"]
        })
    }
    
    # Initialize orchestrator
    orchestrator = PrefectWorkflowOrchestrator()
    
    # Process workflow with schedule
    result = await orchestrator.process_add_workflow(
        sample_workflow, 
        schedule="cron:0 2 * * *"  # Daily at 2 AM
    )
    
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    asyncio.run(main())

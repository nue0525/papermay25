# Prefect workflow orchestrator
import json
import uuid
from typing import Dict, List, Any, Optional
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
import psycopg2
from contextlib import contextmanager


class PrefectWorkflowOrchestrator:
    """
    Converts workflow definitions into Prefect flows with proper task dependencies
    """
    
    def __init__(self):
        self.logger = None
        
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
        # Find nodes with no dependencies (root nodes)
        all_nodes = set(dependencies.keys())
        nodes_with_dependencies = set()
        
        for node, deps in dependencies.items():
            nodes_with_dependencies.update(deps)
        
        # Topological sort to determine execution order
        execution_levels = []
        remaining_nodes = set(all_nodes)
        processed_nodes = set()
        
        while remaining_nodes:
            # Find nodes that can be executed (all dependencies satisfied)
            current_level = []
            for node in remaining_nodes:
                node_deps = set(dependencies.get(node, []))
                if node_deps.issubset(processed_nodes):
                    current_level.append(node)
            
            if not current_level:
                # Handle circular dependencies or orphaned nodes
                current_level = list(remaining_nodes)
            
            execution_levels.append(current_level)
            remaining_nodes -= set(current_level)
            processed_nodes.update(current_level)
        
        return execution_levels
    
    def create_database_connection_task(self, node_id: str, node_info: Dict[str, Any]):
        """Create a database connection task"""
        
        @task(name=f"connect_db_{node_id}")
        async def connect_database():
            logger = get_run_logger()
            try:
                db_details = node_info.get("dbDetails", {})
                connector_type = node_info.get("connector_type", "")
                
                if connector_type == "postgresql":
                    # Get connection details from secrets or environment
                    conn_string = f"postgresql://user:password@host:port/{db_details['db']}"
                    engine = create_engine(conn_string)
                    
                    logger.info(f"Connected to PostgreSQL database: {db_details['db']}")
                    return {"engine": engine, "connection_type": "postgresql"}
                
                # Add other database types as needed
                return {"engine": None, "connection_type": connector_type}
                
            except Exception as e:
                logger.error(f"Database connection failed for node {node_id}: {str(e)}")
                raise
        
        return connect_database
    
    def create_extract_task(self, node_id: str, node_info: Dict[str, Any]):
        """Create data extraction task"""
        
        @task(name=f"extract_{node_id}")
        async def extract_data(connection_info: Dict[str, Any]) -> pd.DataFrame:
            logger = get_run_logger()
            try:
                db_details = node_info.get("dbDetails", {})
                
                if connection_info["connection_type"] == "postgresql":
                    engine = connection_info["engine"]
                    
                    # Build query based on node configuration
                    if db_details.get("sql"):
                        query = db_details["sql"]
                    else:
                        table = db_details.get("table", "")
                        schema = db_details.get("schema", "public")
                        filter_condition = db_details.get("filter", "")
                        
                        query = f"SELECT * FROM {schema}.{table}"
                        if filter_condition:
                            query += f" WHERE {filter_condition}"
                    
                    df = pd.read_sql(query, engine)
                    logger.info(f"Extracted {len(df)} rows from {node_id}")
                    return df
                
                return pd.DataFrame()
                
            except Exception as e:
                logger.error(f"Data extraction failed for node {node_id}: {str(e)}")
                raise
        
        return extract_data
    
    def create_transform_task(self, node_id: str, node_info: Dict[str, Any]):
        """Create data transformation task"""
        
        @task(name=f"transform_{node_id}")
        async def transform_data(input_data: pd.DataFrame, field_mapping: List[Dict[str, Any]] = None) -> pd.DataFrame:
            logger = get_run_logger()
            try:
                transformations = node_info.get("transformations", {})
                connector_type = node_info.get("connector_type", "")
                
                df = input_data.copy()
                
                if connector_type == "filter":
                    # Apply filter transformations
                    transform_configs = transformations.get("transformations", [])
                    for config in transform_configs:
                        if config.get("selected") and config.get("expression"):
                            expression = config["expression"]["value"]
                            # Apply filter (simplified - would need proper expression parsing)
                            if ">" in expression:
                                parts = expression.split(">")
                                if len(parts) == 2:
                                    column = parts[0].strip()
                                    value = float(parts[1].strip())
                                    df = df[df[column] > value]
                
                # Apply field mapping if provided
                if field_mapping:
                    selected_columns = [mapping["value"] for mapping in field_mapping if mapping.get("selected")]
                    if selected_columns:
                        df = df[selected_columns]
                    
                    # Apply column transformations/expressions
                    for mapping in field_mapping:
                        if mapping.get("expression") and mapping["expression"].strip():
                            # Apply expression transformations
                            pass  # Implement expression evaluation
                
                logger.info(f"Transformed data for node {node_id}: {len(df)} rows, {len(df.columns)} columns")
                return df
                
            except Exception as e:
                logger.error(f"Data transformation failed for node {node_id}: {str(e)}")
                raise
        
        return transform_data
    
    def create_load_task(self, node_id: str, node_info: Dict[str, Any]):
        """Create data loading task"""
        
        @task(name=f"load_{node_id}")
        async def load_data(data: pd.DataFrame, connection_info: Dict[str, Any]) -> bool:
            logger = get_run_logger()
            try:
                db_details = node_info.get("dbDetails", {})
                
                if connection_info["connection_type"] == "postgresql":
                    engine = connection_info["engine"]
                    table = db_details.get("table", "")
                    schema = db_details.get("schema", "public")
                    truncate = db_details.get("truncateTable", False)
                    
                    if truncate:
                        with engine.connect() as conn:
                            conn.execute(text(f"TRUNCATE TABLE {schema}.{table}"))
                            conn.commit()
                    
                    # Load data
                    data.to_sql(
                        table, 
                        engine, 
                        schema=schema, 
                        if_exists='append', 
                        index=False
                    )
                    
                    logger.info(f"Loaded {len(data)} rows to {schema}.{table}")
                    return True
                
                return False
                
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
            name=f"workflow_{workflow_name}_{workflow_id}",
            task_runner=ConcurrentTaskRunner(),
            retries=1,
            retry_delay_seconds=30
        )
        async def workflow_flow():
            logger = get_run_logger()
            logger.info(f"Starting workflow: {workflow_name}")
            
            # Store results from each node
            node_results = {}
            node_connections = {}
            
            try:
                # Execute nodes level by level based on dependencies
                for level_index, level_nodes in enumerate(execution_levels):
                    logger.info(f"Executing level {level_index + 1}: {level_nodes}")
                    
                    # Execute all nodes in current level concurrently
                    level_tasks = []
                    
                    for node_id in level_nodes:
                        if node_id not in connection_info:
                            logger.warning(f"Node {node_id} not found in connection info")
                            continue
                        
                        node_info = connection_info[node_id]
                        connector_type = node_info.get("connector_type", "")
                        node_name = node_info.get("node_name", node_id)
                        
                        logger.info(f"Processing node: {node_name} ({connector_type})")
                        
                        if connector_type in ["postgresql", "mysql", "oracle"]:
                            # Database source node
                            connect_task = self.create_database_connection_task(node_id, node_info)
                            extract_task = self.create_extract_task(node_id, node_info)
                            
                            # Execute connection and extraction
                            connection = await connect_task()
                            data = await extract_task(connection)
                            
                            node_results[node_id] = data
                            node_connections[node_id] = connection
                            
                        elif connector_type == "filter":
                            # Transformation node
                            transform_task = self.create_transform_task(node_id, node_info)
                            field_mapping = node_info.get("field_mapping", [])
                            
                            # Get input data from parent nodes
                            parent_nodes = dependencies.get(node_id, [])
                            if parent_nodes:
                                input_data = node_results.get(parent_nodes[0], pd.DataFrame())
                                transformed_data = await transform_task(input_data, field_mapping)
                                node_results[node_id] = transformed_data
                            
                        elif connector_type in ["postgresql"] and node_info.get("dbDetails", {}).get("table"):
                            # Target/Load node
                            load_task = self.create_load_task(node_id, node_info)
                            connect_task = self.create_database_connection_task(node_id, node_info)
                            
                            # Get input data from parent nodes
                            parent_nodes = dependencies.get(node_id, [])
                            if parent_nodes and parent_nodes[0] in node_results:
                                input_data = node_results[parent_nodes[0]]
                                connection = await connect_task()
                                
                                # Apply field mapping if exists
                                field_mapping = node_info.get("field_mapping", [])
                                if field_mapping:
                                    transform_task = self.create_transform_task(node_id, node_info)
                                    input_data = await transform_task(input_data, field_mapping)
                                
                                success = await load_task(input_data, connection)
                                node_results[node_id] = success
                    
                    # Wait for current level to complete before proceeding to next level
                    await asyncio.gather(*level_tasks, return_exceptions=True)
                
                logger.info(f"Workflow {workflow_name} completed successfully")
                return {"status": "success", "nodes_processed": len(node_results)}
                
            except Exception as e:
                logger.error(f"Workflow {workflow_name} failed: {str(e)}")
                raise
            
            finally:
                # Clean up connections
                for connection in node_connections.values():
                    if hasattr(connection.get("engine"), "dispose"):
                        connection["engine"].dispose()
        
        return workflow_flow
    
    def create_deployment(self, workflow_flow, workflow_data: Dict[str, Any], schedule: Optional[str] = None):
        """Create Prefect deployment for the workflow"""
        
        workflow_name = workflow_data["workflow_name"]
        workflow_id = workflow_data["workflow_id"]
        
        deployment_name = f"deployment_{workflow_name}_{workflow_id}"
        
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
            work_pool_name="default-agent-pool",  # Configure as needed
            tags=[
                f"workflow_id:{workflow_id}",
                f"workspace_id:{workflow_data['workspace_id']}",
                f"folder_id:{workflow_data['folder_id']}",
                f"job_id:{workflow_data['job_id']}"
            ],
            parameters={
                "workflow_metadata": {
                    "workflow_id": workflow_id,
                    "workflow_name": workflow_name,
                    "created_at": datetime.now().isoformat()
                }
            }
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
                "deployment_id": deployment_id,
                "flow_name": workflow_flow.name,
                "deployment_name": deployment.name,
                "message": f"Workflow '{workflow_data['workflow_name']}' successfully converted to Prefect flow and deployed"
            }
            
        except Exception as e:
            return {
                "status": "error",
                "error": str(e),
                "message": f"Failed to process workflow: {str(e)}"
            }


# Usage example
async def main():
    """Example usage of the workflow orchestrator"""
    
    # Sample workflow data (from your addWorkflow payload)
    sample_workflow = {
        "workflow_id": "sample-workflow-id",
        "workflow_name": "SampleDataPipeline",
        "workspace_id": "workspace-123",
        "folder_id": "folder-456",
        "job_id": "job-789",
        "connectionInformation_v1": json.dumps({
            "node1": {
                "connector_type": "postgresql",
                "node_name": "SourceDB",
                "connector_category": "Databases",
                "dbDetails": {
                    "db": "source_db",
                    "schema": "public",
                    "table": "employees",
                    "filter": "active = true"
                },
                "sequence": 1
            },
            "node2": {
                "connector_type": "filter",
                "node_name": "FilterTransform",
                "connector_category": "Transformations",
                "transformations": {
                    "transformations": [
                        {
                            "value": "salary",
                            "selected": True,
                            "expression": {"value": "salary > 50000"}
                        }
                    ]
                },
                "field_mapping": [
                    {"value": "employee_id", "selected": True, "target": "emp_id"},
                    {"value": "name", "selected": True, "target": "full_name"}
                ],
                "sequence": 2
            },
            "node3": {
                "connector_type": "postgresql",
                "node_name": "TargetDB",
                "connector_category": "Databases",
                "dbDetails": {
                    "db": "target_db",
                    "schema": "public",
                    "table": "high_salary_employees",
                    "truncateTable": True
                },
                "field_mapping": [
                    {"value": "emp_id", "selected": True, "target": "employee_id"},
                    {"value": "full_name", "selected": True, "target": "name"}
                ],
                "sequence": 3
            }
        }),
        "dependencies": json.dumps({
            "node1": [],
            "node2": ["node1"],
            "node3": ["node2"]
        })
    }
    
    # Initialize orchestrator
    orchestrator = PrefectWorkflowOrchestrator()
    
    # Process workflow (with optional schedule)
    result = await orchestrator.process_add_workflow(
        sample_workflow, 
        schedule="cron:0 2 * * *"  # Daily at 2 AM
    )
    
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    asyncio.run(main())

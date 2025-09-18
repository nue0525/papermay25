"""
Migration Script and Deployment Guide for Prefect Integration
This script helps migrate from Redis RQ to Prefect orchestration
"""

import json
import asyncio
from datetime import datetime
from typing import Dict, List, Any, Optional
import uuid

from Connection.pg import create_connection
from Connection.mongoConn import getConnection
from psycopg2 import sql

# Prefect setup
from prefect.deployments import Deployment
from prefect.server.schemas.schedules import CronSchedule

class PrefectMigration:
    """Migration utility for moving from Redis RQ to Prefect"""
    
    def __init__(self):
        self.pg_connection = create_connection()
        self.mongo_db, self.mongo_client = getConnection()
        self.workflow_collection = self.mongo_db.get_collection("workflow")
        self.workspace_collection = self.mongo_db.get_collection("workspace")
    
    async def migrate_workflow_metadata(self) -> Dict[str, int]:
        """
        Migrate workflow metadata from MongoDB to PostgreSQL
        This is the first step in the migration process
        """
        print("Starting workflow metadata migration...")
        
        stats = {
            "workflows_migrated": 0,
            "nodes_migrated": 0,
            "dependencies_migrated": 0,
            "errors": 0
        }
        
        try:
            # Get all active workflows from MongoDB
            workflows = self.workflow_collection.find({"status": "Active"})
            
            cursor = self.pg_connection.cursor()
            
            for workflow_doc in workflows:
                try:
                    await self._migrate_single_workflow(workflow_doc, cursor, stats)
                except Exception as ex:
                    print(f"Error migrating workflow {workflow_doc.get('workflow_id', 'unknown')}: {str(ex)}")
                    stats["errors"] += 1
            
            self.pg_connection.commit()
            cursor.close()
            
        except Exception as ex:
            print(f"Migration failed: {str(ex)}")
            self.pg_connection.rollback()
            raise
        
        print(f"Migration completed: {stats}")
        return stats
    
    async def _migrate_single_workflow(
        self, 
        workflow_doc: Dict[str, Any], 
        cursor, 
        stats: Dict[str, int]
    ):
        """Migrate a single workflow from MongoDB to PostgreSQL"""
        
        # Extract workflow information
        workspace_id = workflow_doc.get("workspace_id", "")
        workspace_name = workflow_doc.get("workspace_name", "")
        folder_id = workflow_doc.get("folder_id", "")
        folder_name = workflow_doc.get("folder_name", "")
        job_id = workflow_doc.get("job_id", "")
        job_name = workflow_doc.get("job_name", "")
        workflow_id = workflow_doc.get("workflow_id", "")
        workflow_name = workflow_doc.get("workflow_name", "")
        created_by = workflow_doc.get("created_by", "")
        created_at = workflow_doc.get("created_at", datetime.utcnow())
        
        # 1. Insert workflow hierarchy
        hierarchy_query = sql.SQL("""
            INSERT INTO workflow_hierarchy 
            (workspace_id, workspace_name, folder_id, folder_name, job_id, job_name,
             workflow_id, workflow_name, created_by, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (workflow_id) DO NOTHING
        """)
        
        cursor.execute(hierarchy_query, (
            workspace_id, workspace_name, folder_id, folder_name,
            job_id, job_name, workflow_id, workflow_name,
            created_by, created_at, created_at
        ))
        
        # 2. Parse and migrate nodes
        connection_info_v1 = workflow_doc.get("connectionInformation_v1", "{}")
        if isinstance(connection_info_v1, str):
            connection_info_v1 = json.loads(connection_info_v1)
        
        for node_id, node_config in connection_info_v1.items():
            node_query = sql.SQL("""
                INSERT INTO workflow_nodes 
                (workflow_id, node_id, node_name, node_type, connector_type,
                 connector_category, sequence_number, connection_info, field_mappings,
                 transformations, db_details, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (workflow_id, node_id) DO NOTHING
            """)
            
            node_type = self._determine_node_type(node_config)
            
            cursor.execute(node_query, (
                workflow_id,
                node_id,
                node_config.get("node_name", f"Node_{node_id}"),
                node_type,
                node_config.get("connector_type", ""),
                node_config.get("connector_category", ""),
                node_config.get("sequence", 0),
                json.dumps(node_config),
                json.dumps(node_config.get("field_mapping", [])),
                json.dumps(node_config.get("transformations", {})),
                json.dumps(node_config.get("dbDetails", {})),
                created_at,
                created_at
            ))
            
            stats["nodes_migrated"] += 1
        
        # 3. Parse and migrate dependencies
        dependencies = workflow_doc.get("dependencies", "{}")
        if isinstance(dependencies, str):
            dependencies = json.loads(dependencies)
        
        for child_node_id, parent_node_ids in dependencies.items():
            for parent_node_id in parent_node_ids:
                dep_query = sql.SQL("""
                    INSERT INTO workflow_dependencies 
                    (workflow_id, child_node_id, parent_node_id, created_at)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (workflow_id, child_node_id, parent_node_id) DO NOTHING
                """)
                
                cursor.execute(dep_query, (
                    workflow_id, child_node_id, parent_node_id, created_at
                ))
                
                stats["dependencies_migrated"] += 1
        
        stats["workflows_migrated"] += 1
        print(f"Migrated workflow: {workflow_name}")
    
    def _determine_node_type(self, node_config: Dict[str, Any]) -> str:
        """Determine node type based on configuration"""
        connector_type = node_config.get('connector_type', '')
        sequence = node_config.get('sequence', 0)
        
        if sequence == 1:
            return 'source'
        elif connector_type == 'expression' or node_config.get('connector_category') == 'Transformations':
            return 'transformation'
        else:
            return 'target'
    
    async def create_prefect_deployments(self) -> List[str]:
        """
        Create Prefect deployments for migrated workflows
        This enables scheduled execution and better management
        """
        print("Creating Prefect deployments...")
        
        from .prefect_flows import workflow_execution_flow
        
        deployments_created = []
        
        try:
            cursor = self.pg_connection.cursor()
            
            # Get all workflows
            query = sql.SQL("""
                SELECT workflow_id, workflow_name, workspace_name, folder_name, job_name
                FROM workflow_hierarchy

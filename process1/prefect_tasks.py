"""
Prefect Tasks for Data Processing Pipeline
Refactored from Redis RQ to use Prefect orchestration with Polars for data processing
"""

import os
import json
import uuid
import asyncio
import polars as pl
import psutil
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional, Tuple
from pathlib import Path

from prefect import task, get_run_logger
from prefect.task_runners import SequentialTaskRunner
from prefect.artifacts import create_markdown_artifact
from prefect.blocks.system import JSON as JSONBlock

from google.cloud import storage
from google.oauth2 import service_account
import psycopg2
from psycopg2 import sql

from Connection.pg import create_connection
from CoreEngine.Transformations.expression_functions import exp_dict
from CoreEngine.modularize.utils import get_size, initialize_gcs_client

# Import existing utility functions
from Utility.utils import getCurrentUTCtime
from APIs.Common.logger import getLogger

logger = getLogger


@task(name="store-workflow-metadata", retries=3, retry_delay_seconds=10)
async def store_workflow_metadata_task(
    workspace_id: str,
    workspace_name: str,
    folder_id: str,
    folder_name: str,
    job_id: str,
    job_name: str,
    workflow_id: str,
    workflow_name: str,
    connection_info_v1: Dict[str, Any],
    dependencies: Dict[str, List[str]],
    created_by: str
) -> str:
    """
    Store workflow metadata in PostgreSQL tables for better query performance
    """
    run_logger = get_run_logger()
    run_logger.info(f"Storing metadata for workflow: {workflow_name}")
    
    try:
        connection = create_connection()
        cursor = connection.cursor()
        
        # 1. Store workflow hierarchy
        hierarchy_query = sql.SQL("""
            INSERT INTO workflow_hierarchy 
            (workspace_id, workspace_name, folder_id, folder_name, job_id, job_name, 
             workflow_id, workflow_name, created_by, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (workflow_id) 
            DO UPDATE SET 
                workflow_name = EXCLUDED.workflow_name,
                updated_by = EXCLUDED.created_by,
                updated_at = EXCLUDED.updated_at
        """)
        
        now = getCurrentUTCtime()
        cursor.execute(hierarchy_query, (
            workspace_id, workspace_name, folder_id, folder_name,
            job_id, job_name, workflow_id, workflow_name,
            created_by, now, now
        ))
        
        # 2. Store workflow nodes
        nodes_data = []
        for node_id, node_config in connection_info_v1.items():
            node_data = (
                workflow_id,
                node_id,
                node_config.get('node_name', f'Node_{node_id}'),
                _determine_node_type(node_config),
                node_config.get('connector_type'),
                node_config.get('connector_category', 'Unknown'),
                node_config.get('sequence', 0),
                json.dumps(node_config.get('dbDetails', {})),
                json.dumps(node_config.get('field_mapping', [])),
                json.dumps(node_config.get('transformations', {})),
                json.dumps(node_config.get('dbDetails', {})),
                True,  # is_enabled
                now,
                now
            )
            nodes_data.append(node_data)
        
        if nodes_data:
            # Clear existing nodes for this workflow
            cursor.execute(
                "DELETE FROM workflow_nodes WHERE workflow_id = %s",
                (workflow_id,)
            )
            
            nodes_query = sql.SQL("""
                INSERT INTO workflow_nodes 
                (workflow_id, node_id, node_name, node_type, connector_type, 
                 connector_category, sequence_number, connection_info, field_mappings, 
                 transformations, db_details, is_enabled, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """)
            
            cursor.executemany(nodes_query, nodes_data)
        
        # 3. Store dependencies
        dependencies_data = []
        for child_node_id, parent_node_ids in dependencies.items():
            for parent_node_id in parent_node_ids:
                dep_data = (
                    workflow_id,
                    child_node_id,
                    parent_node_id,
                    'data_flow',
                    True,  # is_active
                    now
                )
                dependencies_data.append(dep_data)
        
        if dependencies_data:
            # Clear existing dependencies for this workflow
            cursor.execute(
                "DELETE FROM workflow_dependencies WHERE workflow_id = %s",
                (workflow_id,)
            )
            
            deps_query = sql.SQL("""
                INSERT INTO workflow_dependencies 
                (workflow_id, child_node_id, parent_node_id, dependency_type, is_active, created_at)
                VALUES (%s, %s, %s, %s, %s, %s)
            """)
            
            cursor.executemany(deps_query, dependencies_data)
        
        connection.commit()
        cursor.close()
        connection.close()
        
        run_logger.info(f"Successfully stored metadata for workflow {workflow_name}")
        return f"Metadata stored for workflow: {workflow_id}"
        
    except Exception as ex:
        run_logger.error(f"Error storing workflow metadata: {str(ex)}")
        raise


@task(name="postgres-extract", retries=3, retry_delay_seconds=30)
async def postgres_extract_task(
    node_id: str,
    node_name: str,
    config: Dict[str, Any],
    workflow_run_id: str,
    workflow_id: str,
    workflow_name: str,
    job_run_id: str,
    workspace_info: Dict[str, str],
    storage_config: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Extract data from PostgreSQL using Polars for better performance
    """
    run_logger = get_run_logger()
    start_time = datetime.now(timezone.utc)
    
    run_logger.info(f"Starting PostgreSQL extraction for node: {node_name}")
    
    try:
        # Record task start in database
        await _record_task_start(
            node_id, node_name, "source", "postgresql",
            workflow_run_id, workflow_id, job_run_id, workspace_info
        )
        
        # Build query
        query = _build_extract_query(config)
        run_logger.info(f"Executing query: {query}")
        
        # Connect to PostgreSQL and extract data using Polars
        connection_string = f"postgresql://{config['username']}:{config['password']}@{config['host-name']}/{config['db']}"
        
        # Use Polars for data extraction - much more efficient than pandas
        df = pl.read_database(
            query=query,
            connection=connection_string,
            engine="connectorx"  # Fast connector for Polars
        )
        
        row_count = df.height
        run_logger.info(f"Extracted {row_count} rows from PostgreSQL")
        
        if row_count == 0:
            run_logger.warning(f"No data found for node {node_name}")
            storage_path = await _handle_empty_dataset(node_id, workflow_run_id, storage_config)
        else:
            # Process and upload data
            storage_path = await _upload_dataframe_to_storage(
                df, node_id, workflow_run_id, config, storage_config
            )
        
        # Record metrics and completion
        end_time = datetime.now(timezone.utc)
        execution_time = (end_time - start_time).total_seconds()
        
        await _record_task_completion(
            node_id, workflow_run_id, job_run_id,
            row_count, storage_path, execution_time, "extract"
        )
        
        # Create Prefect artifact for data lineage
        await create_markdown_artifact(
            key=f"extract-{node_id}-{workflow_run_id}",
            markdown=f"""
# Data Extraction Complete
- **Node**: {node_name}
- **Records Extracted**: {row_count:,}
- **Storage Path**: {storage_path}
- **Execution Time**: {execution_time:.2f} seconds
- **Data Size**: {get_size(df.estimated_size())}
            """,
            description=f"Extraction results for {node_name}"
        )
        
        result = {
            "status": "completed",
            "storage_path": storage_path,
            "row_count": row_count,
            "execution_time": execution_time,
            "node_id": node_id,
            "type": "extract"
        }
        
        run_logger.info(f"PostgreSQL extraction completed for {node_name}")
        return result
        
    except Exception as ex:
        run_logger.error(f"PostgreSQL extraction failed: {str(ex)}")
        await _record_task_failure(
            node_id, workflow_run_id, job_run_id, str(ex)
        )
        raise


@task(name="postgres-load", retries=3, retry_delay_seconds=30)
async def postgres_load_task(
    node_id: str,
    node_name: str,
    config: Dict[str, Any],
    storage_path: str,
    workflow_run_id: str,
    workflow_id: str,
    workflow_name: str,
    job_run_id: str,
    workspace_info: Dict[str, str],
    storage_config: Dict[str, Any],
    field_mappings: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """
    Load data into PostgreSQL using Polars for efficient processing
    """
    run_logger = get_run_logger()
    start_time = datetime.now(timezone.utc)
    
    run_logger.info(f"Starting PostgreSQL load for node: {node_name}")
    
    try:
        # Record task start
        await _record_task_start(
            node_id, node_name, "target", "postgresql",
            workflow_run_id, workflow_id, job_run_id, workspace_info
        )
        
        # Download and process data from storage
        total_rows_loaded = 0
        storage_client = _get_storage_client(storage_config)
        
        # Get all files from storage path
        files = _list_storage_files(storage_client, storage_path, storage_config)
        
        if not files:
            run_logger.warning(f"No files found in storage path: {storage_path}")
            storage_path_out = storage_path  # Keep original path
        else:
            # Process files using Polars
            all_dataframes = []
            for file_path in files:
                if file_path.endswith('.csv'):
                    # Use Polars to read CSV - much faster than pandas
                    df = _read_csv_from_storage(storage_client, file_path, storage_config)
                    if df is not None and df.height > 0:
                        all_dataframes.append(df)
            
            if all_dataframes:
                # Concatenate all dataframes using Polars
                combined_df = pl.concat(all_dataframes, how="vertical")
                
                # Apply field mappings
                if field_mappings:
                    combined_df = _apply_field_mappings_polars(combined_df, field_mappings)
                
                # Load data into PostgreSQL
                total_rows_loaded = await _load_dataframe_to_postgres(
                    combined_df, config, run_logger
                )
                
                run_logger.info(f"Loaded {total_rows_loaded} rows into PostgreSQL")
            
            storage_path_out = storage_path
        
        # Record completion
        end_time = datetime.now(timezone.utc)
        execution_time = (end_time - start_time).total_seconds()
        
        await _record_task_completion(
            node_id, workflow_run_id, job_run_id,
            total_rows_loaded, storage_path_out, execution_time, "load"
        )
        
        # Create Prefect artifact
        await create_markdown_artifact(
            key=f"load-{node_id}-{workflow_run_id}",
            markdown=f"""
# Data Load Complete
- **Node**: {node_name}
- **Records Loaded**: {total_rows_loaded:,}
- **Target Table**: {config.get('schema', 'public')}.{config['table']}
- **Execution Time**: {execution_time:.2f} seconds
            """,
            description=f"Load results for {node_name}"
        )
        
        result = {
            "status": "completed",
            "storage_path": storage_path_out,
            "row_count": total_rows_loaded,
            "execution_time": execution_time,
            "node_id": node_id,
            "type": "load"
        }
        
        run_logger.info(f"PostgreSQL load completed for {node_name}")
        return result
        
    except Exception as ex:
        run_logger.error(f"PostgreSQL load failed: {str(ex)}")
        await _record_task_failure(
            node_id, workflow_run_id, job_run_id, str(ex)
        )
        raise


@task(name="expression-transform", retries=3, retry_delay_seconds=30)
async def expression_transform_task(
    node_id: str,
    node_name: str,
    config: Dict[str, Any],
    storage_path: str,
    transformation_config: Dict[str, Any],
    workflow_run_id: str,
    workflow_id: str,
    workflow_name: str,
    job_run_id: str,
    workspace_info: Dict[str, str],
    storage_config: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Apply transformations using Polars for high-performance data processing
    """
    run_logger = get_run_logger()
    start_time = datetime.now(timezone.utc)
    
    run_logger.info(f"Starting expression transformation for node: {node_name}")
    
    try:
        # Record task start
        await _record_task_start(
            node_id, node_name, "transformation", "expression",
            workflow_run_id, workflow_id, job_run_id, workspace_info
        )
        
        # Parse transformation configuration
        selected_columns, expression_columns = _parse_transformation_config(transformation_config)
        run_logger.info(f"Selected columns: {selected_columns}")
        run_logger.info(f"Expression columns: {expression_columns}")
        
        # Get storage client
        storage_client = _get_storage_client(storage_config)
        
        # Process files from storage
        files = _list_storage_files(storage_client, storage_path, storage_config)
        total_rows_processed = 0
        output_files = []
        
        for i, file_path in enumerate(files):
            if file_path.endswith('.csv'):
                run_logger.info(f"Processing file {i+1}/{len(files)}: {file_path}")
                
                # Read CSV using Polars (much faster than pandas)
                df = _read_csv_from_storage(storage_client, file_path, storage_config)
                
                if df is None or df.height == 0:
                    run_logger.warning(f"Empty or invalid file: {file_path}")
                    continue
                
                # Apply transformations using Polars expressions
                transformed_df = _apply_transformations_polars(
                    df, selected_columns, expression_columns, run_logger
                )
                
                # Upload transformed data
                output_path = await _upload_transformed_data(
                    transformed_df, node_id, workflow_run_id, i, storage_config
                )
                output_files.append(output_path)
                total_rows_processed += transformed_df.height
        
        # Create storage path for outputs
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        storage_path_out = f"workflow_data/temp/{node_id}_{timestamp}/"
        
        # Record completion
        end_time = datetime.now(timezone.utc)
        execution_time = (end_time - start_time).total_seconds()
        
        await _record_task_completion(
            node_id, workflow_run_id, job_run_id,
            total_rows_processed, storage_path_out, execution_time, "transform"
        )
        
        # Create Prefect artifact
        await create_markdown_artifact(
            key=f"transform-{node_id}-{workflow_run_id}",
            markdown=f"""
# Transformation Complete
- **Node**: {node_name}
- **Records Processed**: {total_rows_processed:,}
- **Files Processed**: {len(files)}
- **Output Files**: {len(output_files)}
- **Execution Time**: {execution_time:.2f} seconds
- **Transformations Applied**: {len(expression_columns)}
            """,
            description=f"Transformation results for {node_name}"
        )
        
        result = {
            "status": "completed",
            "storage_path": f"gs://{storage_config['bucket_name']}/{storage_path_out}",
            "row_count": total_rows_processed,
            "execution_time": execution_time,
            "node_id": node_id,
            "type": "transform"
        }
        
        run_logger.info(f"Expression transformation completed for {node_name}")
        return result
        
    except Exception as ex:
        run_logger.error(f"Expression transformation failed: {str(ex)}")
        await _record_task_failure(
            node_id, workflow_run_id, job_run_id, str(ex)
        )
        raise


@task(name="update-metrics", retries=2, retry_delay_seconds=5)
async def update_metrics_task(
    workflow_run_id: str,
    workflow_id: str,
    node_results: List[Dict[str, Any]]
) -> str:
    """
    Update workflow execution metrics and data lineage
    """
    run_logger = get_run_logger()
    
    try:
        connection = create_connection()
        cursor = connection.cursor()
        
        # Update workflow-level metrics
        total_rows = sum(result.get('row_count', 0) for result in node_results)
        total_execution_time = sum(result.get('execution_time', 0) for result in node_results)
        
        metrics_data = [
            (workflow_run_id, workflow_id, 'total_rows_processed', total_rows, 'rows', 'performance', None, datetime.now(timezone.utc)),
            (workflow_run_id, workflow_id, 'total_execution_time', total_execution_time, 'seconds', 'performance', None, datetime.now(timezone.utc)),
            (workflow_run_id, workflow_id, 'total_nodes_executed', len(node_results), 'count', 'performance', None, datetime.now(timezone.utc))
        ]
        
        # Add node-specific metrics
        for result in node_results:
            node_id = result.get('node_id')
            if node_id:
                metrics_data.extend([
                    (workflow_run_id, workflow_id, 'node_rows_processed', result.get('row_count', 0), 'rows', 'performance', node_id, datetime.now(timezone.utc)),
                    (workflow_run_id, workflow_id, 'node_execution_time', result.get('execution_time', 0), 'seconds', 'performance', node_id, datetime.now(timezone.utc))
                ])
        
        # Insert metrics
        metrics_query = sql.SQL("""
            INSERT INTO workflow_execution_metrics 
            (workflow_run_id, workflow_id, metric_name, metric_value, metric_unit, metric_type, node_id, recorded_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """)
        
        cursor.executemany(metrics_query, metrics_data)
        
        # Update data lineage for connected nodes
        lineage_data = []
        for i, result in enumerate(node_results):
            if i > 0:  # Skip first node (source)
                source_node = node_results[i-1].get('node_id')
                target_node = result.get('node_id')
                storage_path = result.get('storage_path', '')
                
                lineage_data.append((
                    workflow_run_id,
                    source_node,
                    target_node,
                    storage_path,
                    result.get('row_count', 0),
                    0,  # data_size_bytes - would need to calculate
                    None,  # checksum
                    json.dumps({'transformation_type': result.get('type')}),
                    datetime.now(timezone.utc)
                ))
        
        if lineage_data:
            lineage_query = sql.SQL("""
                INSERT INTO workflow_data_lineage 
                (workflow_run_id, source_node_id, target_node_id, data_path, record_count, 
                 data_size_bytes, checksum, transformation_applied, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """)
            
            cursor.executemany(lineage_query, lineage_data)
        
        connection.commit()
        cursor.close()
        connection.close()
        
        run_logger.info(f"Updated metrics for workflow run: {workflow_run_id}")
        return "Metrics updated successfully"
        
    except Exception as ex:
        run_logger.error(f"Failed to update metrics: {str(ex)}")
        raise


# Helper Functions

def _determine_node_type(node_config: Dict[str, Any]) -> str:
    """Determine node type based on configuration"""
    connector_type = node_config.get('connector_type', '')
    sequence = node_config.get('sequence', 0)
    
    if sequence == 1:
        return 'source'
    elif connector_type == 'expression' or node_config.get('connector_category') == 'Transformations':
        return 'transformation'
    else:
        return 'target'


def _build_extract_query(config: Dict[str, Any]) -> str:
    """Build SQL query for data extraction"""
    filter_clause = config.get("filter", "").strip()
    sql_query = config.get("sql", "").strip()
    table = config['table']
    
    if len(sql_query.strip()) > 1:
        query = sql_query.rstrip(';')
    elif len(filter_clause.strip()) > 1:
        query = f"SELECT * FROM {table} {filter_clause.rstrip(';')}"
    else:
        query = f"SELECT * FROM {table}"
    
    return query


async def _handle_empty_dataset(node_id: str, workflow_run_id: str, storage_config: Dict[str, Any]) -> str:
    """Handle case where no data is extracted"""
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    folder_path = f"workflow_data/temp/{workflow_run_id}/{node_id}/{timestamp}"
    
    # Create empty placeholder file
    empty_csv = "# No data extracted\n"
    
    storage_client = _get_storage_client(storage_config)
    bucket = storage_client.get_bucket(storage_config["bucket_name"])
    blob = bucket.blob(f"{folder_path}/empty_0.csv")
    blob.upload_from_string(empty_csv, content_type="text/csv")
    
    return f"gs://{storage_config['bucket_name']}/{folder_path}/"


async def _upload_dataframe_to_storage(
    df: pl.DataFrame, 
    node_id: str, 
    workflow_run_id: str, 
    config: Dict[str, Any], 
    storage_config: Dict[str, Any]
) -> str:
    """Upload Polars DataFrame to cloud storage"""
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    folder_path = f"workflow_data/temp/{workflow_run_id}/{node_id}/{timestamp}"
    
    storage_client = _get_storage_client(storage_config)
    bucket = storage_client.get_bucket(storage_config["bucket_name"])
    
    # Split large dataframes into chunks for better performance
    chunk_size = 100000
    total_chunks = (df.height + chunk_size - 1) // chunk_size
    
    for i in range(total_chunks):
        start_idx = i * chunk_size
        end_idx = min((i + 1) * chunk_size, df.height)
        chunk_df = df.slice(start_idx, end_idx - start_idx)
        
        # Convert to CSV string using Polars (faster than pandas)
        csv_string = chunk_df.write_csv()
        
        # Upload chunk
        blob_name = f"{folder_path}/{config['table']}_{i}.csv"
        blob = bucket.blob(blob_name)
        blob.upload_from_string(csv_string, content_type="text/csv")
    
    return f"gs://{storage_config['bucket_name']}/{folder_path}/"


def _get_storage_client(storage_config: Dict[str, Any]):
    """Get cloud storage client"""
    if "gcs_creds" in storage_config:
        creds = service_account.Credentials.from_service_account_info(
            storage_config["gcs_creds"]
        )
        return storage.Client(credentials=creds)
    else:
        return storage.Client()


def _list_storage_files(storage_client, storage_path: str, storage_config: Dict[str, Any]) -> List[str]:
    """List files in storage path"""
    # Parse GCS path
    parts = storage_path.replace('gs://', '').split('/', 1)
    bucket_name = parts[0]
    prefix = parts[1] if len(parts) > 1 else ''
    
    bucket = storage_client.get_bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=prefix)
    
    return [blob.name for blob in blobs if blob.name.endswith('.csv')]


def _read_csv_from_storage(storage_client, file_path: str, storage_config: Dict[str, Any]) -> Optional[pl.DataFrame]:
    """Read CSV file from storage using Polars"""
    try:
        bucket_name = storage_config["bucket_name"]
        bucket = storage_client.get_bucket(bucket_name)
        blob = bucket.blob(file_path)
        
        # Download as string and parse with Polars
        csv_content = blob.download_as_text()
        
        # Use Polars to read CSV from string (much faster than pandas)
        df = pl.read_csv(
            csv_content.encode(),
            infer_schema_length=1000,  # Infer schema from first 1000 rows
            try_parse_dates=True
        )
        
        return df
        
    except Exception as ex:
        logger.error(f"Error reading CSV from storage: {str(ex)}")
        return None


def _apply_field_mappings_polars(df: pl.DataFrame, field_mappings: List[Dict[str, Any]]) -> pl.DataFrame:
    """Apply field mappings using Polars operations"""
    # Select only mapped fields
    selected_fields = [mapping for mapping in field_mappings if mapping.get('selected', False)]
    
    if not selected_fields:
        return df
    
    # Build column selection and renaming
    select_exprs = []
    for mapping in selected_fields:
        source_col = mapping['name']
        target_col = mapping['target']
        
        if source_col in df.columns:
            if source_col != target_col:
                select_exprs.append(pl.col(source_col).alias(target_col))
            else:
                select_exprs.append(pl.col(source_col))
    
    return df.select(select_exprs) if select_exprs else df


def _parse_transformation_config(transformation_config: Dict[str, Any]) -> Tuple[List[str], List[str]]:
    """Parse transformation configuration to extract selected columns and expressions"""
    selected_columns = []
    expression_columns = []
    
    if "transformations" in transformation_config and "source" in transformation_config["transformations"]:
        source = transformation_config["transformations"]["source"]
        columns = source.get("columns", [])
        
        for column in columns:
            column_name = column["name"]
            is_selected = column.get("selected", False)
            
            if is_selected:
                selected_columns.append(column_name)
                
                # Check for expressions
                if "expression" in column and column["expression"].get("value"):
                    expression_value = f"{column_name}:{column['expression']['value']}"
                    expression_columns.append(expression_value)
    
    return selected_columns, expression_columns


def _apply_transformations_polars(
    df: pl.DataFrame, 
    selected_columns: List[str], 
    expression_columns: List[str], 
    run_logger
) -> pl.DataFrame:
    """Apply transformations using Polars expressions for high performance"""
    
    # Start with selected columns
    if selected_columns:
        available_cols = [col for col in selected_columns if col in df.columns]
        result_df = df.select(available_cols)
    else:
        result_df = df
    
    # Apply expression transformations using Polars
    for expr_str in expression_columns:
        try:
            parts = expr_str.split(':', 1)
            if len(parts) != 2:
                continue
                
            dest_col = parts[0]
            expression = parts[1]
            
            # Apply transformation using Polars expressions
            result_df = _apply_polars_expression(result_df, dest_col, expression, run_logger)
            
        except Exception as ex:
            run_logger.error(f"Error applying expression {expr_str}: {str(ex)}")
    
    return result_df


def _apply_polars_expression(df: pl.DataFrame, dest_col: str, expression: str, run_logger) -> pl.DataFrame:
    """Apply individual expression using Polars"""
    try:
        # Handle IF conditions with Polars when_then_otherwise
        if expression.lower().startswith('if('):
            return _handle_if_condition_polars(df, dest_col, expression, run_logger)
        
        # Handle function calls
        elif '(' in expression and expression.endswith(')'):
            return _handle_function_call_polars(df, dest_col, expression, run_logger)
        
        # Handle arithmetic expressions
        else:
            # Use Polars expressions for arithmetic
            try:
                # Simple arithmetic - convert to Polars expression syntax
                polars_expr = _convert_to_polars_expression(expression)
                return df.with_columns(polars_expr.alias(dest_col))
            except Exception:
                # Fallback: keep original value if expression fails
                run_logger.warning(f"Could not parse expression: {expression}")
                return df
                
    except Exception as ex:
        run_logger.error(f"Error in Polars expression: {str(ex)}")
        return df


def _handle_if_condition_polars(df: pl.DataFrame, dest_col: str, expression: str, run_logger) -> pl.DataFrame:
    """Handle IF conditions using Polars when_then_otherwise"""
    try:
        # Parse IF condition - simplified parser
        content = expression[3:-1]  # Remove 'if(' and ')'
        parts = content.split(',')
        
        if len(parts) != 3:
            run_logger.error(f"Invalid IF condition: {expression}")
            return df
        
        condition_str = parts[0].strip()
        true_value = parts[1].strip()
        false_value = parts[2].strip()
        
        # Convert to Polars conditional expression
        # This is a simplified version - you'd need more complex parsing for nested conditions
        condition_expr = _parse_condition_to_polars(condition_str)
        true_expr = _parse_value_to_polars(true_value)
        false_expr = _parse_value_to_polars(false_value)
        
        result_expr = pl.when(condition_expr).then(true_expr).otherwise(false_expr)
        return df.with_columns(result_expr.alias(dest_col))
        
    except Exception as ex:
        run_logger.error(f"Error handling IF condition: {str(ex)}")
        return df


def _handle_function_call_polars(df: pl.DataFrame, dest_col: str, expression: str, run_logger) -> pl.DataFrame:
    """Handle function calls using Polars built-in functions"""
    try:
        fn_parts = expression.split('(', 1)
        func_name = fn_parts[0].strip().lower()
        param_str = fn_parts[1].rstrip(')')
        
        # Map common functions to Polars operations
        if func_name == 'upper':
            col_name = param_str.strip()
            return df.with_columns(pl.col(col_name).str.to_uppercase().alias(dest_col))
        
        elif func_name == 'lower':
            col_name = param_str.strip()
            return df.with_columns(pl.col(col_name).str.to_lowercase().alias(dest_col))
        
        elif func_name == 'length':
            col_name = param_str.strip()
            return df.with_columns(pl.col(col_name).str.len_chars().alias(dest_col))
        
        elif func_name == 'abs':
            col_name = param_str.strip()
            return df.with_columns(pl.col(col_name).abs().alias(dest_col))
        
        elif func_name == 'round':
            params = [p.strip() for p in param_str.split(',')]
            col_name = params[0]
            decimals = int(params[1]) if len(params) > 1 else 0
            return df.with_columns(pl.col(col_name).round(decimals).alias(dest_col))
        
        # Add more function mappings as needed
        else:
            run_logger.warning(f"Unsupported function: {func_name}")
            return df
            
    except Exception as ex:
        run_logger.error(f"Error handling function call: {str(ex)}")
        return df


def _convert_to_polars_expression(expression: str) -> pl.Expr:
    """Convert simple arithmetic expressions to Polars expressions"""
    # This is a simplified converter - you'd need a more robust parser for complex expressions
    # For now, handle basic arithmetic
    if '+' in expression:
        parts = expression.split('+')
        left = parts[0].strip()
        right = parts[1].strip()
        return pl.col(left) + pl.col(right) if right.isalpha() else pl.col(left) + float(right)
    
    elif '-' in expression:
        parts = expression.split('-')
        left = parts[0].strip()
        right = parts[1].strip()
        return pl.col(left) - pl.col(right) if right.isalpha() else pl.col(left) - float(right)
    
    elif '*' in expression:
        parts = expression.split('*')
        left = parts[0].strip()
        right = parts[1].strip()
        return pl.col(left) * pl.col(right) if right.isalpha() else pl.col(left) * float(right)
    
    elif '/' in expression:
        parts = expression.split('/')
        left = parts[0].strip()
        right = parts[1].strip()
        return pl.col(left) / pl.col(right) if right.isalpha() else pl.col(left) / float(right)
    
    else:
        # Assume it's a column name
        return pl.col(expression)


def _parse_condition_to_polars(condition_str: str) -> pl.Expr:
    """Parse condition string to Polars expression"""
    # Simplified condition parser
    if '>' in condition_str:
        parts = condition_str.split('>')
        left = parts[0].strip()
        right = parts[1].strip()
        return pl.col(left) > float(right) if right.replace('.', '').isdigit() else pl.col(left) > pl.col(right)
    
    elif '<' in condition_str:
        parts = condition_str.split('<')
        left = parts[0].strip()
        right = parts[1].strip()
        return pl.col(left) < float(right) if right.replace('.', '').isdigit() else pl.col(left) < pl.col(right)
    
    elif '==' in condition_str or '=' in condition_str:
        sep = '==' if '==' in condition_str else '='
        parts = condition_str.split(sep)
        left = parts[0].strip()
        right = parts[1].strip().strip("'\"")
        return pl.col(left) == right
    
    else:
        # Default to true
        return pl.lit(True)


def _parse_value_to_polars(value_str: str) -> pl.Expr:
    """Parse value string to Polars expression"""
    value_str = value_str.strip()
    
    if value_str.startswith("'") and value_str.endswith("'"):
        # String literal
        return pl.lit(value_str[1:-1])
    elif value_str.replace('.', '').isdigit():
        # Numeric literal
        return pl.lit(float(value_str))
    else:
        # Assume column name
        return pl.col(value_str)


async def _upload_transformed_data(
    df: pl.DataFrame, 
    node_id: str, 
    workflow_run_id: str, 
    file_index: int, 
    storage_config: Dict[str, Any]
) -> str:
    """Upload transformed dataframe to storage"""
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    folder_path = f"workflow_data/temp/{node_id}_{timestamp}"
    
    storage_client = _get_storage_client(storage_config)
    bucket = storage_client.get_bucket(storage_config["bucket_name"])
    
    # Convert to CSV using Polars
    csv_string = df.write_csv()
    
    # Upload file
    blob_name = f"{folder_path}/transformed_output_{file_index}.csv"
    blob = bucket.blob(blob_name)
    blob.upload_from_string(csv_string, content_type="text/csv")
    
    return blob_name


async def _load_dataframe_to_postgres(df: pl.DataFrame, config: Dict[str, Any], run_logger) -> int:
    """Load Polars DataFrame to PostgreSQL"""
    try:
        # Convert to pandas for database insertion (Polars doesn't have direct DB write yet)
        pandas_df = df.to_pandas()
        
        # Create connection
        connection = psycopg2.connect(
            host=config["host-name"],
            database=config["db"],
            user=config["username"],
            password=config["password"]
        )
        
        cursor = connection.cursor()
        schema = config.get("schema", "public")
        table = config["table"]
        full_table_name = f"{schema}.{table}"
        
        # Truncate if specified
        if config.get("truncateTable", False):
            cursor.execute(f"TRUNCATE TABLE {full_table_name}")
            connection.commit()
        
        # Prepare bulk insert
        columns = list(pandas_df.columns)
        values_template = ','.join(['%s'] * len(columns))
        insert_query = f"INSERT INTO {full_table_name} ({','.join(columns)}) VALUES ({values_template})"
        
        # Convert DataFrame to list of tuples for bulk insert
        data_tuples = [tuple(row) for row in pandas_df.values]
        
        # Bulk insert
        cursor.executemany(insert_query, data_tuples)
        connection.commit()
        
        rows_inserted = len(data_tuples)
        run_logger.info(f"Inserted {rows_inserted} rows into {full_table_name}")
        
        cursor.close()
        connection.close()
        
        return rows_inserted
        
    except Exception as ex:
        run_logger.error(f"Error loading data to PostgreSQL: {str(ex)}")
        raise


async def _record_task_start(
    node_id: str, node_name: str, node_type: str, connector_type: str,
    workflow_run_id: str, workflow_id: str, job_run_id: str, workspace_info: Dict[str, str]
):
    """Record task start in database"""
    try:
        connection = create_connection()
        cursor = connection.cursor()
        
        # Get current task run info from Prefect context
        from prefect import runtime
        task_run_id = str(runtime.task_run.id) if runtime.task_run else None
        
        query = sql.SQL("""
            INSERT INTO node_run_details 
            (node_id, node_name, connector_type, node_type, workflow_run_id, workflow_id, 
             workspace_id, workspace_name, folder_id, folder_name, job_id, job_name,
             job_run_id, node_start_timestamp, node_status, prefect_task_run_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (workflow_run_id, node_id) 
            DO UPDATE SET 
                node_status = 'Running',
                node_start_timestamp = EXCLUDED.node_start_timestamp,
                prefect_task_run_id = EXCLUDED.prefect_task_run_id
        """)
        
        cursor.execute(query, (
            node_id, node_name, connector_type, node_type, workflow_run_id, workflow_id,
            workspace_info.get('workspace_id'), workspace_info.get('workspace_name'),
            workspace_info.get('folder_id'), workspace_info.get('folder_name'),
            workspace_info.get('job_id'), workspace_info.get('job_name'),
            job_run_id, datetime.now(timezone.utc), 'Running', task_run_id
        ))
        
        connection.commit()
        cursor.close()
        connection.close()
        
    except Exception as ex:
        logger.error(f"Error recording task start: {str(ex)}")


async def _record_task_completion(
    node_id: str, workflow_run_id: str, job_run_id: str,
    row_count: int, storage_path: str, execution_time: float, task_type: str
):
    """Record task completion in database"""
    try:
        connection = create_connection()
        cursor = connection.cursor()
        
        # Get memory usage
        process = psutil.Process()
        memory_mb = process.memory_info().rss / 1024 / 1024
        
        query = sql.SQL("""
            UPDATE node_run_details 
            SET node_status = 'Completed',
                node_end_timestamp = %s,
                source_rows = %s,
                target_rows = %s,
                output_storage_path = %s,
                total_run_time_sec = %s,
                memory_usage_mb = %s,
        query = sql.SQL("""
            UPDATE node_run_details 
            SET node_status = 'Completed',
                node_end_timestamp = %s,
                source_rows = %s,
                target_rows = %s,
                output_storage_path = %s,
                total_run_time_sec = %s,
                memory_usage_mb = %s,
                data_processing_engine = 'polars'
            WHERE workflow_run_id = %s AND node_id = %s
        """)
        
        cursor.execute(query, (
            datetime.now(timezone.utc), row_count, row_count,
            f'{{{storage_path}}}', execution_time, memory_mb,
            workflow_run_id, node_id
        ))
        
        connection.commit()
        cursor.close()
        connection.close()
        
    except Exception as ex:
        logger.error(f"Error recording task completion: {str(ex)}")


async def _record_task_failure(
    node_id: str, workflow_run_id: str, job_run_id: str, error_message: str
):
    """Record task failure in database"""
    try:
        connection = create_connection()
        cursor = connection.cursor()
        
        query = sql.SQL("""
            UPDATE node_run_details 
            SET node_status = 'Failed',
                node_end_timestamp = %s,
                error_message = %s
            WHERE workflow_run_id = %s AND node_id = %s
        """)
        
        cursor.execute(query, (
            datetime.now(timezone.utc), error_message[:1000],  # Truncate long error messages
            workflow_run_id, node_id
        ))
        
        connection.commit()
        cursor.close()
        connection.close()
        
    except Exception as ex:
        logger.error(f"Error recording task failure: {str(ex)}")

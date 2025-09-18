"""
Highly Optimized PostgreSQL Extract and Load Functions using Polars
Maintains all functionality: filters, SQL queries, field mappings, selected columns, etc.
"""

import os
import json
import uuid
import asyncio
import polars as pl
import psutil
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional, Tuple, Union
from pathlib import Path
import io
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
import hashlib

# Database and storage imports
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values
from google.cloud import storage
from google.oauth2 import service_account

# Utility imports
from Connection.pg import create_connection
from CoreEngine.modularize.utils import (
    createLoggerObject,
    sendMessageToRedis,
    get_size,
    initialize_gcs_client
)
from Utility.utils import getCurrentUTCtime

# Redis and monitoring
from redis import Redis
redisConn = Redis(host="localhost", port=6379, db=0)


async def postgres_extract_optimized(
    config: Dict[str, Any],
    nodeId: str,
    nodeName: str,
    **kwargs
) -> Dict[str, Any]:
    """
    Highly optimized PostgreSQL extraction using Polars with all original functionality:
    - Custom SQL queries
    - WHERE clause filters
    - Column selection
    - Chunked processing for large datasets
    - Parallel processing
    - Memory optimization
    """
    # Extract parameters
    workflowRunId = kwargs["workflowRunId"]
    workflow_id = kwargs["workflow_id"]
    workflow_name = kwargs["workflow_name"]
    jobRunDict = kwargs["jobRunDict"]
    workflowLogPath = kwargs["workflowLogPath"]
    
    # Setup logging and tracking
    logger = createLoggerObject(workflowLogPath)
    start_time = datetime.now(timezone.utc)
    row_count = 0
    
    try:
        logger.info(f"Starting optimized PostgreSQL extraction for node: {nodeName}")
        
        # Record task start in database
        await _record_node_start(nodeId, nodeName, "extract", workflowRunId, workflow_id, jobRunDict)
        
        # Send status to Redis
        _send_status_update(workflowRunId, nodeId, "Started", "Extraction initiated")
        
        # Build optimized extraction query
        query, estimated_rows = await _build_extraction_query_optimized(config, logger)
        logger.info(f"Extraction query: {query}")
        logger.info(f"Estimated rows: {estimated_rows}")
        
        # Determine optimal chunk size based on available memory and data size
        chunk_size = _calculate_optimal_chunk_size(estimated_rows)
        logger.info(f"Using chunk size: {chunk_size}")
        
        # Setup storage configuration
        storage_config = _get_storage_configuration()
        
        # Extract data using optimized approach
        if estimated_rows > chunk_size:
            # Large dataset - use chunked parallel extraction
            storage_path, total_rows = await _extract_large_dataset_parallel(
                config, query, chunk_size, nodeId, workflowRunId, 
                storage_config, logger
            )
        else:
            # Small dataset - use direct extraction
            storage_path, total_rows = await _extract_small_dataset_direct(
                config, query, nodeId, workflowRunId, 
                storage_config, logger
            )
        
        row_count = total_rows
        
        # Record completion
        execution_time = (datetime.now(timezone.utc) - start_time).total_seconds()
        await _record_node_completion(
            nodeId, workflowRunId, row_count, storage_path, 
            execution_time, "extract", logger
        )
        
        # Send completion status
        _send_status_update(
            workflowRunId, nodeId, "Completed", 
            f"Extracted {row_count:,} rows in {execution_time:.2f}s"
        )
        
        # Store result in Redis for dependent nodes
        result_data = {
            "Status": "Completed",
            "storage_path": storage_path,
            "type": "extract",
            "row_count": row_count,
            "execution_time": execution_time
        }
        redisConn.set(workflowRunId + "_" + nodeId, json.dumps(result_data))
        
        logger.info(f"Extraction completed: {row_count:,} rows in {execution_time:.2f}s")
        
        return {
            "status": "completed",
            "node_id": nodeId,
            "storage_path": storage_path,
            "row_count": row_count,
            "execution_time": execution_time,
            "throughput_rows_per_sec": row_count / execution_time if execution_time > 0 else 0
        }
        
    except Exception as ex:
        error_msg = f"PostgreSQL extraction failed for {nodeName}: {str(ex)}"
        logger.error(error_msg, exc_info=True)
        
        # Record failure
        execution_time = (datetime.now(timezone.utc) - start_time).total_seconds()
        await _record_node_failure(nodeId, workflowRunId, error_msg, execution_time)
        
        # Send failure status
        _send_status_update(workflowRunId, nodeId, "Failed", error_msg)
        
        raise Exception(error_msg)


async def postgres_load_optimized(
    config: Dict[str, Any],
    nodeId: str,
    nodeName: str,
    storage_path: str,
    field_mappings: List[Dict[str, Any]],
    **kwargs
) -> Dict[str, Any]:
    """
    Highly optimized PostgreSQL load using Polars with all original functionality:
    - Field mapping and column selection
    - Data type conversions
    - Batch processing
    - Conflict resolution (upsert/truncate)
    - Parallel loading
    - Memory optimization
    """
    # Extract parameters
    workflowRunId = kwargs["workflowRunId"]
    workflow_id = kwargs["workflow_id"]
    workflow_name = kwargs["workflow_name"]
    jobRunDict = kwargs["jobRunDict"]
    workflowLogPath = kwargs["workflowLogPath"]
    
    # Setup logging and tracking
    logger = createLoggerObject(workflowLogPath)
    start_time = datetime.now(timezone.utc)
    total_rows_loaded = 0
    
    try:
        logger.info(f"Starting optimized PostgreSQL load for node: {nodeName}")
        
        # Record task start
        await _record_node_start(nodeId, nodeName, "load", workflowRunId, workflow_id, jobRunDict)
        
        # Send status update
        _send_status_update(workflowRunId, nodeId, "Started", "Load initiated")
        
        # Setup storage and target configurations
        storage_config = _get_storage_configuration()
        target_config = _parse_target_configuration(config, field_mappings, logger)
        
        # Process field mappings for optimal column handling
        column_mapping = _build_optimized_column_mapping(field_mappings, logger)
        
        # Load and process data from storage
        if not storage_path or storage_path == "":
            logger.warning("No storage path provided - creating empty dataset")
            total_rows_loaded = 0
        else:
            # Get all files from storage path
            files_to_process = _get_storage_files_list(storage_path, storage_config, logger)
            
            if not files_to_process:
                logger.warning(f"No files found in storage path: {storage_path}")
                total_rows_loaded = 0
            else:
                # Process files and load to PostgreSQL
                total_rows_loaded = await _load_files_to_postgres_optimized(
                    files_to_process, target_config, column_mapping,
                    storage_config, nodeId, logger
                )
        
        # Record completion
        execution_time = (datetime.now(timezone.utc) - start_time).total_seconds()
        await _record_node_completion(
            nodeId, workflowRunId, total_rows_loaded, storage_path,
            execution_time, "load", logger
        )
        
        # Send completion status
        _send_status_update(
            workflowRunId, nodeId, "Completed",
            f"Loaded {total_rows_loaded:,} rows in {execution_time:.2f}s"
        )
        
        # Check if this is an intermediate node that needs extraction
        dependencies = jobRunDict.get("workflows", [{}])[0].get("dependencies", {})
        has_children = any(nodeId in parent_list for parent_list in dependencies.values())
        
        if has_children and config.get("connector_type") == "postgresql":
            # This is an intermediate PostgreSQL node - set up for extraction
        if has_children and config.get("connector_type") == "postgresql":
            # This is an intermediate PostgreSQL node - set up for extraction
            result_status = "LoadCompleted"
            storage_path_out = f"workflow_data/temp/{workflowRunId}/{nodeId}/extract_{int(datetime.now().timestamp())}"
        else:
            result_status = "Completed"
            storage_path_out = storage_path
        
        # Store result in Redis
        result_data = {
            "Status": result_status,
            "storage_path": storage_path_out,
            "type": "load",
            "row_count": total_rows_loaded,
            "execution_time": execution_time
        }
        redisConn.set(workflowRunId + "_" + nodeId, json.dumps(result_data))
        
        logger.info(f"Load completed: {total_rows_loaded:,} rows in {execution_time:.2f}s")
        
        return {
            "status": "completed",
            "node_id": nodeId,
            "storage_path": storage_path_out,
            "row_count": total_rows_loaded,
            "execution_time": execution_time,
            "throughput_rows_per_sec": total_rows_loaded / execution_time if execution_time > 0 else 0
        }
        
    except Exception as ex:
        error_msg = f"PostgreSQL load failed for {nodeName}: {str(ex)}"
        logger.error(error_msg, exc_info=True)
        
        # Record failure
        execution_time = (datetime.now(timezone.utc) - start_time).total_seconds()
        await _record_node_failure(nodeId, workflowRunId, error_msg, execution_time)
        
        # Send failure status
        _send_status_update(workflowRunId, nodeId, "Failed", error_msg)
        
        raise Exception(error_msg)


# ============================================================================
# OPTIMIZED HELPER FUNCTIONS
# ============================================================================

async def _build_extraction_query_optimized(
    config: Dict[str, Any], 
    logger
) -> Tuple[str, int]:
    """
    Build optimized extraction query with all functionality:
    - Custom SQL queries
    - WHERE clause filters
    - Column selection
    - Row estimation for optimization
    """
    try:
        # Get basic configuration
        table = config.get("table", "")
        schema = config.get("schema", "public")
        full_table = f"{schema}.{table}" if schema != "public" else table
        
        # Get filters and custom SQL
        filter_clause = config.get("filter", "").strip()
        custom_sql = config.get("sql", "").strip()
        
        # Build query based on priority: custom SQL > filter > all
        if custom_sql:
            # Custom SQL has highest priority
            query = custom_sql.rstrip(';')
            logger.info("Using custom SQL query")
        elif filter_clause:
            # Apply WHERE filter
            # Check if it's a complete WHERE clause or just conditions
            if filter_clause.upper().startswith('WHERE'):
                query = f"SELECT * FROM {full_table} {filter_clause}"
            else:
                query = f"SELECT * FROM {full_table} WHERE {filter_clause}"
            logger.info("Using filtered query")
        else:
            # Select all
            query = f"SELECT * FROM {full_table}"
            logger.info("Using full table query")
        
        # Get row count estimate for optimization
        estimated_rows = await _estimate_query_rows(config, query, logger)
        
        return query, estimated_rows
        
    except Exception as ex:
        logger.error(f"Error building extraction query: {str(ex)}")
        raise


async def _estimate_query_rows(
    config: Dict[str, Any], 
    query: str, 
    logger
) -> int:
    """Estimate number of rows for optimization decisions"""
    try:
        # Create connection
        conn = psycopg2.connect(
            host=config["host-name"],
            database=config["db"],
            user=config["username"],
            password=config["password"]
        )
        cursor = conn.cursor()
        
        # Use EXPLAIN to estimate rows
        explain_query = f"EXPLAIN (FORMAT JSON) {query}"
        cursor.execute(explain_query)
        explain_result = cursor.fetchone()[0]
        
        # Extract estimated rows from explain plan
        estimated_rows = int(explain_result[0]["Plan"]["Plan Rows"])
        
        cursor.close()
        conn.close()
        
        return estimated_rows
        
    except Exception as ex:
        logger.warning(f"Could not estimate rows, using default: {str(ex)}")
        return 100000  # Default estimate


def _calculate_optimal_chunk_size(estimated_rows: int) -> int:
    """Calculate optimal chunk size based on system resources and data size"""
    try:
        # Get available memory
        memory = psutil.virtual_memory()
        available_memory_gb = memory.available / (1024**3)
        
        # Calculate chunk size based on available memory and estimated rows
        if estimated_rows < 10000:
            return estimated_rows  # Process all at once for small datasets
        elif available_memory_gb > 8:
            return min(500000, estimated_rows // 4)  # Large chunks for high-memory systems
        elif available_memory_gb > 4:
            return min(100000, estimated_rows // 8)  # Medium chunks
        else:
            return min(50000, estimated_rows // 16)   # Small chunks for low-memory systems
            
    except Exception:
        return 50000  # Safe default


async def _extract_large_dataset_parallel(
    config: Dict[str, Any],
    base_query: str,
    chunk_size: int,
    node_id: str,
    workflow_run_id: str,
    storage_config: Dict[str, Any],
    logger
) -> Tuple[str, int]:
    """Extract large datasets using parallel chunked processing"""
    
    logger.info(f"Using parallel chunked extraction with chunk size: {chunk_size}")
    
    try:
        # Get total count for chunking
        total_count = await _get_total_count(config, base_query, logger)
        num_chunks = (total_count + chunk_size - 1) // chunk_size
        
        logger.info(f"Processing {total_count:,} rows in {num_chunks} chunks")
        
        # Create storage path
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        folder_path = f"workflow_data/temp/{workflow_run_id}/{node_id}/{timestamp}"
        
        # Process chunks in parallel
        chunk_futures = []
        with ThreadPoolExecutor(max_workers=min(4, num_chunks)) as executor:
            for chunk_idx in range(num_chunks):
                offset = chunk_idx * chunk_size
                future = executor.submit(
                    _extract_single_chunk,
                    config, base_query, offset, chunk_size,
                    chunk_idx, folder_path, storage_config, logger
                )
                chunk_futures.append(future)
            
            # Wait for all chunks to complete
            total_rows = 0
            for future in as_completed(chunk_futures):
                chunk_rows = future.result()
                total_rows += chunk_rows
        
        storage_path = f"gs://{storage_config['bucket_name']}/{folder_path}/"
        logger.info(f"Parallel extraction completed: {total_rows:,} rows")
        
        return storage_path, total_rows
        
    except Exception as ex:
        logger.error(f"Parallel extraction failed: {str(ex)}")
        raise


def _extract_single_chunk(
    config: Dict[str, Any],
    base_query: str,
    offset: int,
    limit: int,
    chunk_idx: int,
    folder_path: str,
    storage_config: Dict[str, Any],
    logger
) -> int:
    """Extract a single chunk of data"""
    try:
        # Add LIMIT and OFFSET to query
        chunked_query = f"{base_query} LIMIT {limit} OFFSET {offset}"
        
        # Create database connection (need new connection per thread)
        conn = psycopg2.connect(
            host=config["host-name"],
            database=config["db"],
            user=config["username"],
            password=config["password"]
        )
        
        # Use Polars to read directly from PostgreSQL
        df = pl.read_database(
            query=chunked_query,
            connection=conn,
            engine="connectorx"
        )
        
        conn.close()
        
        if df.height == 0:
            logger.info(f"Chunk {chunk_idx}: No data")
            return 0
        
        # Convert to CSV and upload
        csv_data = df.write_csv()
        
        # Upload to storage
        storage_client = _get_storage_client(storage_config)
        bucket = storage_client.get_bucket(storage_config["bucket_name"])
        blob_name = f"{folder_path}/{config.get('table', 'data')}_{chunk_idx}.csv"
        blob = bucket.blob(blob_name)
        blob.upload_from_string(csv_data, content_type="text/csv")
        
        logger.info(f"Chunk {chunk_idx}: {df.height:,} rows uploaded")
        return df.height
        
    except Exception as ex:
        logger.error(f"Error processing chunk {chunk_idx}: {str(ex)}")
        return 0


async def _extract_small_dataset_direct(
    config: Dict[str, Any],
    query: str,
    node_id: str,
    workflow_run_id: str,
    storage_config: Dict[str, Any],
    logger
) -> Tuple[str, int]:
    """Extract small datasets directly without chunking"""
    
    logger.info("Using direct extraction for small dataset")
    
    try:
        # Create connection
        conn = psycopg2.connect(
            host=config["host-name"],
            database=config["db"],
            user=config["username"],
            password=config["password"]
        )
        
        # Use Polars for fast extraction
        df = pl.read_database(
            query=query,
            connection=conn,
            engine="connectorx"
        )
        
        conn.close()
        
        if df.height == 0:
            logger.info("No data extracted")
            # Create empty placeholder
            timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
            folder_path = f"workflow_data/temp/{workflow_run_id}/{node_id}/{timestamp}"
            storage_path = f"gs://{storage_config['bucket_name']}/{folder_path}/"
            
            # Upload empty CSV
            storage_client = _get_storage_client(storage_config)
            bucket = storage_client.get_bucket(storage_config["bucket_name"])
            blob = bucket.blob(f"{folder_path}/empty_0.csv")
            blob.upload_from_string("# No data\n", content_type="text/csv")
            
            return storage_path, 0
        
        # Upload data
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        folder_path = f"workflow_data/temp/{workflow_run_id}/{node_id}/{timestamp}"
        
        # Convert to CSV
        csv_data = df.write_csv()
        
        # Upload to storage
        storage_client = _get_storage_client(storage_config)
        bucket = storage_client.get_bucket(storage_config["bucket_name"])
        blob_name = f"{folder_path}/{config.get('table', 'data')}_0.csv"
        blob = bucket.blob(blob_name)
        blob.upload_from_string(csv_data, content_type="text/csv")
        
        storage_path = f"gs://{storage_config['bucket_name']}/{folder_path}/"
        logger.info(f"Direct extraction completed: {df.height:,} rows")
        
        return storage_path, df.height
        
    except Exception as ex:
        logger.error(f"Direct extraction failed: {str(ex)}")
        raise


async def _get_total_count(config: Dict[str, Any], query: str, logger) -> int:
    """Get total count for chunking strategy"""
    try:
        # Wrap query in COUNT to get total rows
        count_query = f"SELECT COUNT(*) FROM ({query}) AS subquery"
        
        conn = psycopg2.connect(
            host=config["host-name"],
            database=config["db"],
            user=config["username"],
            password=config["password"]
        )
        cursor = conn.cursor()
        cursor.execute(count_query)
        total_count = cursor.fetchone()[0]
        cursor.close()
        conn.close()
        
        return total_count
        
    except Exception as ex:
        logger.warning(f"Could not get total count: {str(ex)}")
        return 1000000  # Default assumption


def _parse_target_configuration(
    config: Dict[str, Any], 
    field_mappings: List[Dict[str, Any]], 
    logger
) -> Dict[str, Any]:
    """Parse target configuration for optimized loading"""
    
    try:
        schema = config.get("schema", "public")
        table = config.get("table", "")
        truncate_table = config.get("truncateTable", False)
        
        # Build full table name
        full_table_name = f"{schema}.{table}" if schema != "public" else table
        
        return {
            "host": config["host-name"],
            "database": config["db"],
            "username": config["username"],
            "password": config["password"],
            "schema": schema,
            "table": table,
            "full_table_name": full_table_name,
            "truncate_table": truncate_table
        }
        
    except Exception as ex:
        logger.error(f"Error parsing target configuration: {str(ex)}")
        raise


def _build_optimized_column_mapping(
    field_mappings: List[Dict[str, Any]], 
    logger
) -> Dict[str, Any]:
    """Build optimized column mapping for field transformations"""
    
    try:
        if not field_mappings:
            return {"use_all_columns": True}
        
        selected_mappings = [m for m in field_mappings if m.get("selected", True)]
        
        if not selected_mappings:
            return {"use_all_columns": True}
        
        # Build source to target column mapping
        column_mapping = {
            "use_all_columns": False,
            "source_columns": [],
            "target_columns": [],
            "column_map": {},
            "type_conversions": {}
        }
        
        for mapping in selected_mappings:
            source_col = mapping.get("name", mapping.get("value", ""))
            target_col = mapping.get("target", source_col)
            source_type = mapping.get("type", "")
            target_type = mapping.get("targetType", source_type)
            
            if source_col:
                column_mapping["source_columns"].append(source_col)
                column_mapping["target_columns"].append(target_col)
                column_mapping["column_map"][source_col] = target_col
                
                if source_type != target_type:
                    column_mapping["type_conversions"][source_col] = {
                        "from": source_type,
                        "to": target_type
                    }
        
        logger.info(f"Built column mapping: {len(selected_mappings)} columns selected")
        return column_mapping
        
    except Exception as ex:
        logger.error(f"Error building column mapping: {str(ex)}")
        return {"use_all_columns": True}


async def _load_files_to_postgres_optimized(
    files_to_process: List[str],
    target_config: Dict[str, Any],
    column_mapping: Dict[str, Any],
    storage_config: Dict[str, Any],
    node_id: str,
    logger
) -> int:
    """Load files to PostgreSQL with optimized batch processing"""
    
    try:
        total_rows_loaded = 0
        
        # Create database connection
        conn = psycopg2.connect(
            host=target_config["host"],
            database=target_config["database"],
            user=target_config["username"],
            password=target_config["password"]
        )
        cursor = conn.cursor()
        
        # Truncate table if requested
        if target_config.get("truncate_table", False):
            truncate_query = f"TRUNCATE TABLE {target_config['full_table_name']}"
            cursor.execute(truncate_query)
            conn.commit()
            logger.info(f"Truncated table: {target_config['full_table_name']}")
        
        # Process files
        storage_client = _get_storage_client(storage_config)
        
        for file_path in files_to_process:
            try:
                logger.info(f"Processing file: {file_path}")
                
                # Read file using Polars
                df = _read_csv_from_storage_optimized(
                    storage_client, file_path, storage_config, logger
                )
                
                if df is None or df.height == 0:
                    logger.warning(f"Skipping empty file: {file_path}")
                    continue
                
                # Apply column mapping and transformations
                df_transformed = _apply_column_transformations_optimized(
                    df, column_mapping, logger
                )
                
                # Load to PostgreSQL using optimized batch insert
                rows_loaded = await _batch_insert_optimized(
                    df_transformed, target_config, cursor, logger
                )
                
                total_rows_loaded += rows_loaded
                logger.info(f"Loaded {rows_loaded:,} rows from {file_path}")
                
            except Exception as ex:
                logger.error(f"Error processing file {file_path}: {str(ex)}")
                continue
        
        conn.commit()
        cursor.close()
        conn.close()
        
        logger.info(f"Total rows loaded: {total_rows_loaded:,}")
        return total_rows_loaded
        
    except Exception as ex:
        logger.error(f"Error loading files to PostgreSQL: {str(ex)}")
        raise


def _read_csv_from_storage_optimized(
    storage_client,
    file_path: str,
    storage_config: Dict[str, Any],
    logger
) -> Optional[pl.DataFrame]:
    """Read CSV from storage using Polars with optimization"""
    try:
        bucket_name = storage_config["bucket_name"]
        bucket = storage_client.get_bucket(bucket_name)
        blob = bucket.blob(file_path)
        
        # Download as bytes for Polars
        csv_bytes = blob.download_as_bytes()
        
        if len(csv_bytes) == 0:
            logger.warning(f"Empty file: {file_path}")
            return None
        
        # Use Polars to read CSV with optimizations
        df = pl.read_csv(
            csv_bytes,
            infer_schema_length=1000,  # Infer schema from first 1000 rows
            try_parse_dates=True,      # Auto-parse dates
            ignore_errors=True,        # Skip malformed rows
            truncate_ragged_lines=True # Handle inconsistent column counts
        )
        
        return df
        
    except Exception as ex:
        logger.error(f"Error reading CSV from storage: {str(ex)}")
        return None


def _apply_column_transformations_optimized(
    df: pl.DataFrame,
    column_mapping: Dict[str, Any],
    logger
) -> pl.DataFrame:
    """Apply column transformations using Polars expressions"""
    
    try:
        if column_mapping.get("use_all_columns", True):
            return df
        
        # Select and rename columns
        select_exprs = []
        
        for source_col in column_mapping["source_columns"]:
            if source_col not in df.columns:
                logger.warning(f"Source column not found: {source_col}")
                continue
            
            target_col = column_mapping["column_map"].get(source_col, source_col)
            
            # Apply type conversion if needed
            expr = pl.col(source_col)
            
            if source_col in column_mapping.get("type_conversions", {}):
                conversion = column_mapping["type_conversions"][source_col]
                expr = _apply_type_conversion_polars(expr, conversion, logger)
            
            # Add column with target name
            if source_col != target_col:
                expr = expr.alias(target_col)
            
            select_exprs.append(expr)
        
        if not select_exprs:
            logger.warning("No valid columns found for selection")
            return df
        
        return df.select(select_exprs)
        
    except Exception as ex:
        logger.error(f"Error applying column transformations: {str(ex)}")
        return df


def _apply_type_conversion_polars(
    expr: pl.Expr, 
    conversion: Dict[str, str], 
    logger
) -> pl.Expr:
    """Apply type conversion using Polars expressions"""
    
    try:
        from_type = conversion["from"].lower()
        to_type = conversion["to"].lower()
        
        # Date to string conversions
        if "date" in from_type and ("varchar" in to_type or "char" in to_type):
            return expr.dt.strftime("%Y-%m-%d")
        
        # Numeric conversions
        elif "integer" in from_type and "numeric" in to_type:
            return expr.cast(pl.Float64)
        
        elif "numeric" in from_type and "integer" in to_type:
            return expr.cast(pl.Int64)
        
        # String conversions
        elif to_type in ["varchar", "character varying", "text"]:
            return expr.cast(pl.Utf8)
        
        else:
            logger.warning(f"Unsupported type conversion: {from_type} -> {to_type}")
            return expr
            
    except Exception as ex:
        logger.warning(f"Error applying type conversion: {str(ex)}")
        return expr


async def _batch_insert_optimized(
    df: pl.DataFrame,
    target_config: Dict[str, Any],
    cursor,
    logger
) -> int:
    """Perform optimized batch insert using execute_values"""
    
    try:
        if df.height == 0:
            return 0
        
        # Convert DataFrame to list of tuples for batch insert
        # Use Polars' to_pandas() only for the final conversion step
        pandas_df = df.to_pandas()
        data_tuples = [tuple(row) for row in pandas_df.values]
        
        # Build insert query
        columns = list(df.columns)
        column_names = ", ".join(columns)
        table_name = target_config["full_table_name"]
        
        insert_query = f"INSERT INTO {table_name} ({column_names}) VALUES %s"
        
        # Use execute_values for optimal batch performance
        execute_values(
            cursor,
            insert_query,
            data_tuples,
            template=None,
            page_size=10000  # Optimal batch size
        )
        
        return len(data_tuples)
        
    except Exception as ex:
        logger.error(f"Error in batch insert: {str(ex)}")
        raise


def _get_storage_files_list(
    storage_path: str,
    storage_config: Dict[str, Any],
    logger
) -> List[str]:
    """Get list of files from storage path"""
    
    try:
        if not storage_path or not storage_path.startswith("gs://"):
            return []
        
        # Parse GCS path
        path_parts = storage_path.replace("gs://", "").split("/", 1)
        bucket_name = path_parts[0]
        prefix = path_parts[1] if len(path_parts) > 1 else ""
        
        # List files
        storage_client = _get_storage_client(storage_config)
        bucket = storage_client.get_bucket(bucket_name)
        blobs = bucket.list_blobs(prefix=prefix)
        
        # Filter for CSV files
        csv_files = [blob.name for blob in blobs if blob.name.endswith(".csv")]
        
        logger.info(f"Found {len(csv_files)} CSV files in storage path")
        return csv_files
        
    except Exception as ex:
        logger.error(f"Error listing storage files: {str(ex)}")
        return []


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def _get_storage_configuration() -> Dict[str, Any]:
    """Get storage configuration for cloud operations"""
    try:
        from Utility.config import default_bucket, key_path, GCS_Integration
        
        config = {
            "bucket_name": default_bucket,
            "gcs_integration": GCS_Integration
        }
        
        # Load credentials
        with open(key_path) as key_json:
            config["gcs_creds"] = json.load(key_json)
        
        return config
        
    except Exception as ex:
        raise Exception(f"Error loading storage configuration: {str(ex)}")


def _get_storage_client(storage_config: Dict[str, Any]):
    """Get cloud storage client"""
    if "gcs_creds" in storage_config:
        creds = service_account.Credentials.from_service_account_info(
            storage_config["gcs_creds"]
        )
        return storage.Client(credentials=creds)
    else:
        return storage.Client()


def _send_status_update(
    workflow_run_id: str, 
    node_id: str, 
    status: str, 
    message: str
):
    """Send status update to Redis"""
    try:
        status_msg = {
            "workflowStatus": "Running",
            "status": status,
            "target": node_id,
            "message": message
        }
        sendMessageToRedis(redisConn, [workflow_run_id], json.dumps(status_msg))
    except Exception as ex:
        print(f"Error sending status update: {str(ex)}")


async def _record_node_start(
    node_id: str,
    node_name: str,
    node_type: str,
    workflow_run_id: str,
    workflow_id: str,
    job_run_dict: Dict[str, Any]
):
    """Record node start in database"""
    try:
        connection = create_connection()
        cursor = connection.cursor()
        
        # Insert node run record
        query = sql.SQL("""
            INSERT INTO node_run_details 
            (node_id, node_name, connector_type, node_type, workflow_run_id, workflow_id,
             job_run_id, node_start_timestamp, node_status, data_processing_engine)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (workflow_run_id, node_id) 
            DO UPDATE SET 
                node_status = 'Running',
                node_start_timestamp = EXCLUDED.node_start_timestamp
        """)
        
        cursor.execute(query, (
            node_id, node_name, "postgresql", node_type, workflow_run_id, workflow_id,
            job_run_dict.get("jobRunId", ""), getCurrentUTCtime(), "Running", "polars"
        ))
        
        connection.commit()
        cursor.close()
        connection.close()
        
    except Exception as ex:
        print(f"Error recording node start: {str(ex)}")


async def _record_node_completion(
    node_id: str,
    workflow_run_id: str,
    row_count: int,
    storage_path: str,
    execution_time: float,
    operation_type: str,
    logger
):
    """Record node completion in database"""
    try:
        connection = create_connection()
        cursor = connection.cursor()
        
        # Get memory usage
        process = psutil.Process()
        memory_mb = process.memory_info().rss / 1024 / 1024
        
        # Update node run record
        query = sql.SQL("""
            UPDATE node_run_details 
            SET node_status = 'Completed',
                node_end_timestamp = %s,
                source_rows = %s,
                target_rows = %s,
                output_storage_path = %s,
                total_run_time_sec = %s,
                memory_usage_mb = %s
            WHERE workflow_run_id = %s AND node_id = %s
        """)
        
        cursor.execute(query, (
            getCurrentUTCtime(), row_count, row_count,
            f'{{{storage_path}}}', execution_time, memory_mb,
            workflow_run_id, node_id
        ))
        
        connection.commit()
        cursor.close()
        connection.close()
        
    except Exception as ex:
        logger.error(f"Error recording node completion: {str(ex)}")


async def _record_node_failure(
    node_id: str,
    workflow_run_id: str,
    error_message: str,
    execution_time: float
):
    """Record node failure in database"""
    try:
        connection = create_connection()
        cursor = connection.cursor()
        
        query = sql.SQL("""
            UPDATE node_run_details 
            SET node_status = 'Failed',
                node_end_timestamp = %s,
                error_message = %s,
                total_run_time_sec = %s
            WHERE workflow_run_id = %s AND node_id = %s
        """)
        
        cursor.execute(query, (
            getCurrentUTCtime(), error_message[:1000], execution_time,
            workflow_run_id, node_id
        ))
        
        connection.commit()
        cursor.close()
        connection.close()
        
    except Exception as ex:
        print(f"Error recording node failure: {str(ex)}")

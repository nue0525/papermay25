"""
Connector Base Classes and Interfaces

This module provides the foundational abstract classes and interfaces for all connector 
implementations in the NUE data platform. It establishes standardized patterns for:

- Data extraction and loading operations
- Status tracking and progress monitoring
- Error handling and retry mechanisms
- Resource management and connection pooling
- Polars DataFrame integration for optimal performance
- WebSocket-based real-time status updates
- Database transaction management

The architecture follows the Strategy pattern combined with dependency injection to ensure
scalability, testability, and maintainability across all connector types.

Author: NUE Data Platform Team
Version: 2.0.0
Created: 2025-11-24
"""

import asyncio
import logging
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional, Union, AsyncGenerator, Callable
from uuid import uuid4

import polars as pl
from pydantic import BaseModel, Field, validator
from sqlalchemy.ext.asyncio import AsyncSession

from backend.core.config import get_settings
from backend.core.logging import get_logger
from backend.db.session import get_db


class ConnectorType(str, Enum):
    """Enumeration of all supported connector types for categorization"""
    
    # Database Connectors
    POSTGRESQL = "postgresql"
    MYSQL = "mysql"
    MARIADB = "mariadb"
    ORACLE = "oracle"
    SQL_SERVER = "sql_server"
    AURORA_POSTGRESQL = "aurora_postgresql"
    COCKROACHDB = "cockroachdb"
    YUGABYTEDB = "yugabytedb"
    SNOWFLAKE = "snowflake"
    BIGQUERY = "bigquery"
    SPANNER = "spanner"
    TIMESCALEDB = "timescaledb"
    CLICKHOUSE = "clickhouse"
    SINGLESTORE = "singlestore"
    YELLOWBRICK = "yellowbrick"
    TERADATA = "teradata"
    MONGODB = "mongodb"
    SUPABASE = "supabase"
    
    # Object Storage Connectors
    AWS_S3 = "aws_s3"
    GOOGLE_CLOUD_STORAGE = "google_cloud_storage"
    AZURE_BLOB = "azure_blob"
    AZURE_DATA_LAKE = "azure_data_lake"
    
    # File Storage Connectors
    CSV = "csv"
    JSON = "json"
    PARQUET = "parquet"
    TSV = "tsv"
    EXCEL = "excel"
    
    # SaaS Connectors
    GOOGLE_SHEETS = "google_sheets"
    SALESFORCE = "salesforce"
    HUBSPOT = "hubspot"
    STRIPE = "stripe"
    
    # Apps/API Connectors
    REST_API = "rest_api"
    GRAPHQL = "graphql"
    WEBHOOK = "webhook"


class OperationStatus(str, Enum):
    """Status enumeration for connector operations"""
    PENDING = "pending"
    INITIALIZING = "initializing"
    CONNECTING = "connecting"
    CONNECTED = "connected"
    EXTRACTING = "extracting"
    LOADING = "loading"
    TRANSFORMING = "transforming"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    RETRYING = "retrying"


class ConnectorConfig(BaseModel):
    """Base configuration model for all connectors"""
    
    connector_id: str = Field(..., description="Unique identifier for the connector")
    connector_type: ConnectorType = Field(..., description="Type of connector")
    name: str = Field(..., description="Human-readable name for the connector")
    description: Optional[str] = Field(None, description="Detailed description")
    
    # Connection settings
    connection_config: Dict[str, Any] = Field(..., description="Connection configuration")
    timeout_seconds: int = Field(30, description="Connection timeout in seconds")
    retry_attempts: int = Field(3, description="Number of retry attempts")
    retry_delay_seconds: int = Field(5, description="Delay between retries")
    
    # Performance settings
    batch_size: int = Field(10000, description="Default batch size for operations")
    max_concurrent_operations: int = Field(5, description="Maximum concurrent operations")
    connection_pool_size: int = Field(10, description="Connection pool size")
    
    # Security settings
    enable_ssl: bool = Field(True, description="Enable SSL/TLS encryption")
    verify_ssl_cert: bool = Field(True, description="Verify SSL certificates")
    
    # Monitoring settings
    enable_metrics: bool = Field(True, description="Enable performance metrics")
    log_level: str = Field("INFO", description="Logging level")
    
    class Config:
        use_enum_values = True


class OperationProgress(BaseModel):
    """Progress tracking model for connector operations"""
    
    operation_id: str = Field(default_factory=lambda: str(uuid4()))
    connector_id: str
    operation_type: str  # 'extract', 'load', 'validate', etc.
    status: OperationStatus
    progress_percentage: float = Field(0.0, ge=0.0, le=100.0)
    
    # Data metrics
    rows_processed: int = 0
    rows_total: Optional[int] = None
    bytes_processed: int = 0
    bytes_total: Optional[int] = None
    
    # Timing information
    started_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    completed_at: Optional[datetime] = None
    
    # Error information
    error_message: Optional[str] = None
    error_details: Optional[Dict[str, Any]] = None
    
    # Additional metadata
    metadata: Dict[str, Any] = Field(default_factory=dict)
    
    def update_progress(
        self, 
        status: Optional[OperationStatus] = None,
        progress_percentage: Optional[float] = None,
        rows_processed: Optional[int] = None,
        bytes_processed: Optional[int] = None,
        error_message: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ):
        """Update operation progress with new values"""
        if status is not None:
            self.status = status
        if progress_percentage is not None:
            self.progress_percentage = progress_percentage
        if rows_processed is not None:
            self.rows_processed = rows_processed
        if bytes_processed is not None:
            self.bytes_processed = bytes_processed
        if error_message is not None:
            self.error_message = error_message
        if metadata is not None:
            self.metadata.update(metadata)
        
        self.updated_at = datetime.now(timezone.utc)
        
        if status in [OperationStatus.COMPLETED, OperationStatus.FAILED, OperationStatus.CANCELLED]:
            self.completed_at = datetime.now(timezone.utc)


class BaseConnector(ABC):
    """
    Abstract base class for all connector implementations.
    
    This class provides the foundational structure and common functionality
    for all data connectors in the platform. It enforces a consistent interface
    while allowing for connector-specific implementations.
    
    Key Features:
    - Async/await support for non-blocking operations
    - Built-in retry mechanisms with exponential backoff
    - Progress tracking and status updates
    - Resource management and cleanup
    - Error handling and logging
    - WebSocket integration for real-time updates
    - Polars DataFrame integration for performance
    """
    
    def __init__(
        self, 
        config: ConnectorConfig,
        db_session: Optional[AsyncSession] = None,
        websocket_callback: Optional[Callable] = None
    ):
        """
        Initialize the base connector with configuration and dependencies.
        
        Args:
            config: Connector configuration object
            db_session: Database session for status updates
            websocket_callback: Callback function for WebSocket updates
        """
        self.config = config
        self.logger = get_logger(f"connector.{config.connector_type}")
        self.db_session = db_session
        self.websocket_callback = websocket_callback
        self.settings = get_settings()
        
        # Internal state
        self._is_connected = False
        self._connection = None
        self._operation_progress: Dict[str, OperationProgress] = {}
        
        # Performance metrics
        self._metrics = {
            "total_operations": 0,
            "successful_operations": 0,
            "failed_operations": 0,
            "total_rows_processed": 0,
            "total_bytes_processed": 0,
            "average_operation_time": 0.0
        }
    
    @property
    def is_connected(self) -> bool:
        """Check if the connector is currently connected"""
        return self._is_connected
    
    @property
    def metrics(self) -> Dict[str, Any]:
        """Get connector performance metrics"""
        return self._metrics.copy()
    
    async def _emit_progress_update(self, progress: OperationProgress):
        """
        Emit progress update to both database and WebSocket.
        
        Args:
            progress: Progress object to emit
        """
        try:
            # Update database
            if self.db_session:
                await self._update_progress_in_db(progress)
            
            # Send WebSocket update
            if self.websocket_callback:
                await self.websocket_callback({
                    "type": "progress_update",
                    "data": progress.dict()
                })
            
            self.logger.info(
                f"Progress update - Operation: {progress.operation_id}, "
                f"Status: {progress.status}, Progress: {progress.progress_percentage}%"
            )
            
        except Exception as e:
            self.logger.error(f"Failed to emit progress update: {str(e)}")
    
    async def _update_progress_in_db(self, progress: OperationProgress):
        """
        Update operation progress in the database.
        
        Args:
            progress: Progress object to store
        """
        if not self.db_session:
            return
        
        try:
            # This would typically use a proper SQL query
            # For now, we'll log the update
            self.logger.debug(f"Updating progress in DB: {progress.operation_id}")
            
            # TODO: Implement actual database update
            # await self.db_session.execute(
            #     text("INSERT OR UPDATE operation_progress ...")
            # )
            # await self.db_session.commit()
            
        except Exception as e:
            self.logger.error(f"Failed to update progress in database: {str(e)}")
    
    async def _retry_operation(
        self, 
        operation_func: Callable, 
        *args, 
        **kwargs
    ) -> Any:
        """
        Retry an operation with exponential backoff.
        
        Args:
            operation_func: Function to retry
            *args: Positional arguments for the function
            **kwargs: Keyword arguments for the function
            
        Returns:
            Result of the operation
            
        Raises:
            Last exception if all retries fail
        """
        last_exception = None
        
        for attempt in range(self.config.retry_attempts):
            try:
                return await operation_func(*args, **kwargs)
            except Exception as e:
                last_exception = e
                self.logger.warning(
                    f"Operation failed (attempt {attempt + 1}/{self.config.retry_attempts}): {str(e)}"
                )
                
                if attempt < self.config.retry_attempts - 1:
                    delay = self.config.retry_delay_seconds * (2 ** attempt)
                    await asyncio.sleep(delay)
        
        raise last_exception
    
    @abstractmethod
    async def connect(self) -> bool:
        """
        Establish connection to the data source.
        
        Returns:
            True if connection successful, False otherwise
        """
        pass
    
    @abstractmethod
    async def disconnect(self):
        """
        Close connection and cleanup resources.
        """
        pass
    
    @abstractmethod
    async def validate_connection(self) -> bool:
        """
        Validate that the connection is working and credentials are correct.
        
        Returns:
            True if validation successful, False otherwise
        """
        pass
    
    @abstractmethod
    async def get_schema(self) -> Dict[str, Any]:
        """
        Retrieve schema information from the data source.
        
        Returns:
            Schema information as a dictionary
        """
        pass
    
    @abstractmethod
    async def extract_data(
        self, 
        query: Optional[str] = None,
        table: Optional[str] = None,
        filters: Optional[Dict[str, Any]] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None
    ) -> AsyncGenerator[pl.DataFrame, None]:
        """
        Extract data from the source as Polars DataFrames.
        
        Args:
            query: Custom query to execute
            table: Table name to extract from
            filters: Filters to apply
            limit: Maximum number of rows
            offset: Number of rows to skip
            
        Yields:
            Polars DataFrames containing the extracted data
        """
        pass
    
    @abstractmethod
    async def load_data(
        self, 
        data: pl.DataFrame,
        table: str,
        mode: str = "append"
    ) -> bool:
        """
        Load data to the destination.
        
        Args:
            data: Polars DataFrame to load
            table: Target table name
            mode: Load mode ('append', 'overwrite', 'upsert')
            
        Returns:
            True if load successful, False otherwise
        """
        pass
    
    async def extract_and_load(
        self,
        source_config: Dict[str, Any],
        target_config: Dict[str, Any],
        transformation_func: Optional[Callable[[pl.DataFrame], pl.DataFrame]] = None
    ) -> OperationProgress:
        """
        High-level method to extract, optionally transform, and load data.
        
        Args:
            source_config: Source extraction configuration
            target_config: Target load configuration  
            transformation_func: Optional transformation function
            
        Returns:
            Operation progress object
        """
        operation_id = str(uuid4())
        progress = OperationProgress(
            operation_id=operation_id,
            connector_id=self.config.connector_id,
            operation_type="extract_and_load",
            status=OperationStatus.INITIALIZING
        )
        
        self._operation_progress[operation_id] = progress
        await self._emit_progress_update(progress)
        
        try:
            # Connect if not already connected
            if not self._is_connected:
                progress.update_progress(status=OperationStatus.CONNECTING)
                await self._emit_progress_update(progress)
                await self.connect()
            
            progress.update_progress(status=OperationStatus.EXTRACTING)
            await self._emit_progress_update(progress)
            
            # Extract data in batches
            total_rows = 0
            async for batch in self.extract_data(**source_config):
                # Apply transformation if provided
                if transformation_func:
                    progress.update_progress(status=OperationStatus.TRANSFORMING)
                    await self._emit_progress_update(progress)
                    batch = transformation_func(batch)
                
                # Load batch
                progress.update_progress(status=OperationStatus.LOADING)
                await self._emit_progress_update(progress)
                await self.load_data(batch, **target_config)
                
                total_rows += len(batch)
                progress.update_progress(rows_processed=total_rows)
                await self._emit_progress_update(progress)
            
            progress.update_progress(
                status=OperationStatus.COMPLETED,
                progress_percentage=100.0
            )
            await self._emit_progress_update(progress)
            
            self._metrics["successful_operations"] += 1
            self._metrics["total_rows_processed"] += total_rows
            
            return progress
            
        except Exception as e:
            self.logger.error(f"Extract and load operation failed: {str(e)}")
            progress.update_progress(
                status=OperationStatus.FAILED,
                error_message=str(e)
            )
            await self._emit_progress_update(progress)
            
            self._metrics["failed_operations"] += 1
            raise
        
        finally:
            self._metrics["total_operations"] += 1
    
    async def __aenter__(self):
        """Async context manager entry"""
        await self.connect()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        await self.disconnect()


class DatabaseConnector(BaseConnector):
    """
    Base class for all database connectors.
    
    Provides common functionality for database connections including:
    - Connection pooling
    - Query execution
    - Transaction management
    - Schema introspection
    """
    
    def __init__(self, config: ConnectorConfig, **kwargs):
        super().__init__(config, **kwargs)
        self._connection_pool = None
    
    @abstractmethod
    async def execute_query(
        self, 
        query: str, 
        params: Optional[Dict[str, Any]] = None
    ) -> pl.DataFrame:
        """
        Execute a query and return results as Polars DataFrame.
        
        Args:
            query: SQL query to execute
            params: Query parameters
            
        Returns:
            Query results as Polars DataFrame
        """
        pass
    
    @abstractmethod
    async def get_tables(self) -> List[str]:
        """
        Get list of available tables.
        
        Returns:
            List of table names
        """
        pass
    
    @abstractmethod
    async def get_table_schema(self, table: str) -> Dict[str, Any]:
        """
        Get schema for a specific table.
        
        Args:
            table: Table name
            
        Returns:
            Table schema information
        """
        pass


class ObjectStorageConnector(BaseConnector):
    """
    Base class for object storage connectors (S3, GCS, Azure Blob, etc.).
    
    Provides common functionality for object storage operations:
    - File listing and metadata
    - Upload/download operations
    - Batch processing
    - Compression handling
    """
    
    @abstractmethod
    async def list_objects(
        self, 
        prefix: Optional[str] = None,
        limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        List objects in the storage.
        
        Args:
            prefix: Object prefix filter
            limit: Maximum number of objects
            
        Returns:
            List of object metadata
        """
        pass
    
    @abstractmethod
    async def upload_object(
        self, 
        data: Union[bytes, pl.DataFrame],
        key: str,
        metadata: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Upload object to storage.
        
        Args:
            data: Data to upload
            key: Object key/path
            metadata: Object metadata
            
        Returns:
            True if upload successful
        """
        pass
    
    @abstractmethod
    async def download_object(self, key: str) -> bytes:
        """
        Download object from storage.
        
        Args:
            key: Object key/path
            
        Returns:
            Object data as bytes
        """
        pass


class ConnectorFactory:
    """
    Factory class for creating connector instances based on type.
    
    This factory implements the Factory pattern to provide a centralized
    way to instantiate connectors while maintaining loose coupling and
    enabling easy extension for new connector types.
    """
    
    _connector_registry: Dict[ConnectorType, type] = {}
    
    @classmethod
    def register_connector(cls, connector_type: ConnectorType, connector_class: type):
        """
        Register a connector class for a specific type.
        
        Args:
            connector_type: Type of connector
            connector_class: Connector implementation class
        """
        cls._connector_registry[connector_type] = connector_class
    
    @classmethod
    def create_connector(
        cls, 
        connector_type: ConnectorType,
        config: ConnectorConfig,
        **kwargs
    ) -> BaseConnector:
        """
        Create a connector instance of the specified type.
        
        Args:
            connector_type: Type of connector to create
            config: Connector configuration
            **kwargs: Additional arguments for connector
            
        Returns:
            Connector instance
            
        Raises:
            ValueError: If connector type is not registered
        """
        if connector_type not in cls._connector_registry:
            raise ValueError(f"Connector type '{connector_type}' not registered")
        
        connector_class = cls._connector_registry[connector_type]
        return connector_class(config, **kwargs)
    
    @classmethod
    def get_supported_types(cls) -> List[ConnectorType]:
        """
        Get list of supported connector types.
        
        Returns:
            List of supported connector types
        """
        return list(cls._connector_registry.keys())


# Decorator for registering connectors
def register_connector(connector_type: ConnectorType):
    """
    Decorator to automatically register connector classes.
    
    Args:
        connector_type: Type of connector
        
    Returns:
        Decorated class
    """
    def decorator(cls):
        ConnectorFactory.register_connector(connector_type, cls)
        return cls
    return decorator

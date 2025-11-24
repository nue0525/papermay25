"""Pydantic Schemas for Workflow and Node Run Tracking.

Defines request and response models for execution tracking endpoints.
Tracks workflow runs and individual node executions for monitoring and debugging.

Run Lifecycle:
    1. Workflow triggered → workflow_runs record created (status: pending)
    2. Nodes execute → node_run_details records created per node
    3. Node completes → status updated (success/failed), metrics recorded
    4. Workflow completes → workflow_runs status updated, totals computed

Run Status Values:
    - pending: Queued for execution
    - running: Currently executing
    - success: Completed successfully
    - failed: Failed with error
    - cancelled: Manually stopped
    - timeout: Exceeded time limit

Use Cases:
    - Monitor workflow execution progress
    - Debug failed workflows
    - Track performance metrics (duration, row counts)
    - Audit trail for compliance
    - Cost tracking per execution
"""
from pydantic import BaseModel, Field
from typing import Optional, Any
from datetime import datetime


class WorkflowRunCreate(BaseModel):
    """Schema for creating a workflow run record.
    
    Created when workflow execution is triggered.
    
    Attributes:
        workflow_run_id (str): Unique run identifier (UUID)
        tenant_id (str): Tenant identifier
        workspace_id (Optional[str]): Parent workspace
        folder_id (Optional[str]): Parent folder
        job_id (Optional[str]): Parent job
        workflow_id (str): Workflow being executed
        trigger_type (str): How run was initiated (manual, scheduled, api, event)
        triggered_by (Optional[str]): User or system that triggered run
        config (Optional[dict]): Runtime configuration overrides
    
    Example:
        {
            "workflow_run_id": "run_123",
            "tenant_id": "tenant_abc",
            "workflow_id": "wf_456",
            "trigger_type": "manual",
            "triggered_by": "user_789",
            "config": {"env": "prod", "batch_size": 1000}
        }
    """
    workflow_run_id: str
    tenant_id: str
    workspace_id: Optional[str] = None
    folder_id: Optional[str] = None
    job_id: Optional[str] = None
    workflow_id: str
    trigger_type: str = "manual"  # manual, scheduled, api, event
    triggered_by: Optional[str] = None
    config: Optional[dict] = None


class WorkflowRunUpdate(BaseModel):
    """Schema for updating workflow run status and metrics.
    
    Updated as workflow executes and completes.
    
    Attributes:
        status (Optional[str]): Current status (pending, running, success, failed)
        error_message (Optional[str]): Error details if failed
        rows_processed (Optional[int]): Total rows processed across all nodes
        duration_ms (Optional[int]): Execution time in milliseconds
    """
    status: Optional[str] = None
    error_message: Optional[str] = None
    rows_processed: Optional[int] = None
    duration_ms: Optional[int] = None


class WorkflowRunOut(BaseModel):
    """Schema for workflow run response data.
    
    Returned by run tracking endpoints.
    
    Attributes:
        workflow_run_id (str): Unique run identifier
        tenant_id (str): Tenant identifier
        workflow_id (str): Workflow being executed
        status (str): Current status
        trigger_type (str): How run was initiated
        triggered_by (Optional[str]): User/system that triggered
        started_at (Optional[datetime]): Execution start time
        completed_at (Optional[datetime]): Execution end time
        duration_ms (Optional[int]): Total execution time
        rows_processed (Optional[int]): Total rows processed
        error_message (Optional[str]): Error if failed
        config (Optional[dict]): Runtime configuration
    
    Notes:
        - duration_ms computed from started_at and completed_at
        - rows_processed summed from all node_run_details
    """
    workflow_run_id: str
    tenant_id: str
    workflow_id: str
    status: str
    trigger_type: str
    triggered_by: Optional[str] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    duration_ms: Optional[int] = None
    rows_processed: Optional[int] = None
    error_message: Optional[str] = None
    config: Optional[dict] = None
    
    class Config:
        from_attributes = True


class NodeRunDetailCreate(BaseModel):
    """Schema for creating node execution record.
    
    Created when individual node starts execution.
    
    Attributes:
        node_run_detail_id (str): Unique node run identifier (UUID)
        workflow_run_id (str): Parent workflow run
        node_id (str): Node being executed
        tenant_id (str): Tenant identifier
        sequence (int): Execution order
    
    Example:
        {
            "node_run_detail_id": "noderun_123",
            "workflow_run_id": "run_456",
            "node_id": "node_789",
            "tenant_id": "tenant_abc",
            "sequence": 0
        }
    """
    node_run_detail_id: str
    workflow_run_id: str
    node_id: str
    tenant_id: str
    sequence: int = 0


class NodeRunDetailUpdate(BaseModel):
    """Schema for updating node execution status and metrics.
    
    Updated as node executes and completes.
    
    Attributes:
        status (Optional[str]): Node status (pending, running, success, failed)
        rows_processed (Optional[int]): Number of rows processed by this node
        duration_ms (Optional[int]): Node execution time in milliseconds
        error_message (Optional[str]): Error details if failed
        output_preview (Optional[dict]): Sample output data for debugging
    """
    status: Optional[str] = None
    rows_processed: Optional[int] = None
    duration_ms: Optional[int] = None
    error_message: Optional[str] = None
    output_preview: Optional[dict] = None


class NodeRunDetailOut(BaseModel):
    """Schema for node run detail response.
    
    Returned by node run tracking endpoints.
    
    Attributes:
        node_run_detail_id (str): Unique node run identifier
        workflow_run_id (str): Parent workflow run
        node_id (str): Node that was executed
        tenant_id (str): Tenant identifier
        status (str): Node execution status
        sequence (int): Execution order
        started_at (Optional[datetime]): Node start time
        completed_at (Optional[datetime]): Node completion time
        duration_ms (Optional[int]): Execution duration
        rows_processed (Optional[int]): Rows processed
        error_message (Optional[str]): Error if failed
        output_preview (Optional[dict]): Sample output
    """
    node_run_detail_id: str
    workflow_run_id: str
    node_id: str
    tenant_id: str
    status: str
    sequence: int
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    duration_ms: Optional[int] = None
    rows_processed: Optional[int] = None
    error_message: Optional[str] = None
    output_preview: Optional[dict] = None
    
    class Config:
        from_attributes = True

"""Pydantic Schemas for Workflow Resources.

Defines request and response models for workflow endpoints.
Workflows represent data pipelines/ETL jobs as directed acyclic graphs (DAGs).

Workflow Structure:
    - A workflow contains nodes (individual steps) and edges (dependencies)
    - Nodes are ordered by sequence and dependencies
    - Edges define data flow and execution order
    - Workflows belong to a job within a folder within a workspace

Schema Types:
    - Create: Request payload for creating workflows
    - Update: Request payload for partial updates
    - Out: Response model for workflow data

Validation:
    - Pydantic validates all field types automatically
    - Optional fields have sensible defaults
    - from_attributes=True for SQLAlchemy compatibility
"""
from pydantic import BaseModel, Field
from typing import Optional, Any

class WorkflowCreate(BaseModel):
    """Schema for creating a new workflow.
    
    Workflows are the core execution unit in the orchestrator.
    Each workflow is a DAG of nodes connected by edges.
    
    Attributes:
        tenant_id (str): Tenant identifier for isolation
        workflow_id (str): Unique workflow UUID (client-generated)
        workspace_id (Optional[str]): Parent workspace
        folder_id (Optional[str]): Parent folder
        job_id (Optional[str]): Parent job grouping
        workflow_name (str): Display name
        description (Optional[str]): Documentation
    
    Example:
        {
            "tenant_id": "tenant_abc",
            "workflow_id": "wf_789",
            "job_id": "job_123",
            "workflow_name": "User Data ETL",
            "description": "Extract users from DB, transform, load to warehouse"
        }
    """
    tenant_id: str
    workflow_id: str
    workspace_id: Optional[str] = None
    folder_id: Optional[str] = None
    job_id: Optional[str] = None
    workflow_name: str
    description: Optional[str] = None

class WorkflowUpdate(BaseModel):
    """Schema for partially updating a workflow.
    
    All fields optional for partial updates.
    Used in PATCH /workflows/{id} endpoint.
    
    Attributes:
        workflow_name (Optional[str]): New display name
        description (Optional[str]): New description
        status (Optional[str]): New status (Active, Inactive, Draft)
    """
    workflow_name: Optional[str] = None
    description: Optional[str] = None
    status: Optional[str] = None

class WorkflowOut(BaseModel):
    """Schema for workflow response data.
    
    Returned by workflow endpoints.
    
    Attributes:
        workflow_id (str): Unique workflow identifier
        tenant_id (str): Tenant identifier
        workflow_name (str): Display name
        status (Optional[str]): Current status
        workspace_id (Optional[str]): Parent workspace
        folder_id (Optional[str]): Parent folder
        job_id (Optional[str]): Parent job
    
    Notes:
        - Use GET /workflows/{id}/graph to retrieve nodes and edges
        - Use POST /workflows/{id}/run to execute the workflow (TODO)
    """
    workflow_id: str
    tenant_id: str
    workflow_name: str
    status: Optional[str] = None
    workspace_id: Optional[str] = None
    folder_id: Optional[str] = None
    job_id: Optional[str] = None

    class Config:
        from_attributes = True

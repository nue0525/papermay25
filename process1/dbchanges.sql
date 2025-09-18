-- Enhanced Database Schema for Prefect Integration
-- This extends the existing schema with new tables for workflow metadata storage

-- 1. Workspace hierarchy table to store the complete organizational structure
CREATE TABLE IF NOT EXISTS workflow_hierarchy (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id VARCHAR(255) NOT NULL,
    workspace_name VARCHAR(255) NOT NULL,
    folder_id VARCHAR(255) NOT NULL,
    folder_name VARCHAR(255) NOT NULL,
    job_id VARCHAR(255) NOT NULL,
    job_name VARCHAR(255) NOT NULL,
    workflow_id VARCHAR(255) NOT NULL UNIQUE,
    workflow_name VARCHAR(255) NOT NULL,
    workflow_status VARCHAR(50) DEFAULT 'Active',
    workflow_description TEXT,
    created_by VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_by VARCHAR(255),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    version INTEGER DEFAULT 1,
    is_active BOOLEAN DEFAULT TRUE
);

-- 2. Node definitions table to store individual workflow nodes
CREATE TABLE IF NOT EXISTS workflow_nodes (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workflow_id VARCHAR(255) NOT NULL,
    node_id VARCHAR(255) NOT NULL,
    node_name VARCHAR(255) NOT NULL,
    node_type VARCHAR(100) NOT NULL, -- 'source', 'transformation', 'target'
    connector_type VARCHAR(100) NOT NULL, -- 'postgresql', 'expression', etc.
    connector_category VARCHAR(100) NOT NULL, -- 'Databases', 'Transformations', etc.
    sequence_number INTEGER NOT NULL,
    connection_info JSONB NOT NULL, -- Stores the entire connector configuration
    field_mappings JSONB, -- Stores field mapping configuration
    transformations JSONB, -- Stores transformation configuration
    db_details JSONB, -- Database-specific details (table, schema, etc.)
    is_enabled BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(workflow_id, node_id),
    FOREIGN KEY (workflow_id) REFERENCES workflow_hierarchy(workflow_id) ON DELETE CASCADE
);

-- 3. Node dependencies table to store parent-child relationships
CREATE TABLE IF NOT EXISTS workflow_dependencies (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workflow_id VARCHAR(255) NOT NULL,
    child_node_id VARCHAR(255) NOT NULL,
    parent_node_id VARCHAR(255) NOT NULL,
    dependency_type VARCHAR(50) DEFAULT 'data_flow', -- 'data_flow', 'conditional', etc.
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(workflow_id, child_node_id, parent_node_id),
    FOREIGN KEY (workflow_id) REFERENCES workflow_hierarchy(workflow_id) ON DELETE CASCADE
);

-- 4. Extend existing workflow_run_stats table with Prefect integration
ALTER TABLE workflow_run_stats 
ADD COLUMN IF NOT EXISTS prefect_flow_run_id VARCHAR(255),
ADD COLUMN IF NOT EXISTS prefect_deployment_id VARCHAR(255),
ADD COLUMN IF NOT EXISTS prefect_flow_run_name VARCHAR(255),
ADD COLUMN IF NOT EXISTS flow_run_tags JSONB,
ADD COLUMN IF NOT EXISTS flow_parameters JSONB,
ADD COLUMN IF NOT EXISTS retry_count INTEGER DEFAULT 0,
ADD COLUMN IF NOT EXISTS max_retries INTEGER DEFAULT 3;

-- 5. Extend existing node_run_details table with Prefect task information
ALTER TABLE node_run_details 
ADD COLUMN IF NOT EXISTS prefect_task_run_id VARCHAR(255),
ADD COLUMN IF NOT EXISTS prefect_task_run_name VARCHAR(255),
ADD COLUMN IF NOT EXISTS prefect_task_state VARCHAR(50),
ADD COLUMN IF NOT EXISTS task_retry_count INTEGER DEFAULT 0,
ADD COLUMN IF NOT EXISTS task_run_tags JSONB,
ADD COLUMN IF NOT EXISTS data_processing_engine VARCHAR(50) DEFAULT 'polars',
ADD COLUMN IF NOT EXISTS memory_usage_mb NUMERIC,
ADD COLUMN IF NOT EXISTS cpu_usage_percent NUMERIC,
ADD COLUMN IF NOT EXISTS cache_hit BOOLEAN DEFAULT FALSE;

-- 6. Workflow execution metrics table for enhanced observability
CREATE TABLE IF NOT EXISTS workflow_execution_metrics (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workflow_run_id VARCHAR(255) NOT NULL,
    workflow_id VARCHAR(255) NOT NULL,
    metric_name VARCHAR(100) NOT NULL,
    metric_value NUMERIC,
    metric_unit VARCHAR(50),
    metric_type VARCHAR(50), -- 'performance', 'data_quality', 'resource_usage'
    node_id VARCHAR(255), -- NULL for workflow-level metrics
    recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (workflow_id) REFERENCES workflow_hierarchy(workflow_id) ON DELETE CASCADE
);

-- 7. Data lineage tracking table
CREATE TABLE IF NOT EXISTS workflow_data_lineage (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workflow_run_id VARCHAR(255) NOT NULL,
    source_node_id VARCHAR(255) NOT NULL,
    target_node_id VARCHAR(255) NOT NULL,
    data_path VARCHAR(500), -- Storage path or connection string
    record_count BIGINT,
    data_size_bytes BIGINT,
    checksum VARCHAR(255), -- Data integrity verification
    transformation_applied JSONB, -- What transformations were applied
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 8. Create indexes for optimal query performance
CREATE INDEX IF NOT EXISTS idx_workflow_hierarchy_workspace ON workflow_hierarchy(workspace_id);
CREATE INDEX IF NOT EXISTS idx_workflow_hierarchy_folder ON workflow_hierarchy(folder_id);
CREATE INDEX IF NOT EXISTS idx_workflow_hierarchy_job ON workflow_hierarchy(job_id);
CREATE INDEX IF NOT EXISTS idx_workflow_hierarchy_workflow ON workflow_hierarchy(workflow_id);

CREATE INDEX IF NOT EXISTS idx_workflow_nodes_workflow ON workflow_nodes(workflow_id);
CREATE INDEX IF NOT EXISTS idx_workflow_nodes_type ON workflow_nodes(connector_type);
CREATE INDEX IF NOT EXISTS idx_workflow_nodes_sequence ON workflow_nodes(workflow_id, sequence_number);

CREATE INDEX IF NOT EXISTS idx_workflow_dependencies_workflow ON workflow_dependencies(workflow_id);
CREATE INDEX IF NOT EXISTS idx_workflow_dependencies_child ON workflow_dependencies(child_node_id);
CREATE INDEX IF NOT EXISTS idx_workflow_dependencies_parent ON workflow_dependencies(parent_node_id);

CREATE INDEX IF NOT EXISTS idx_workflow_run_stats_prefect ON workflow_run_stats(prefect_flow_run_id);
CREATE INDEX IF NOT EXISTS idx_node_run_details_prefect ON node_run_details(prefect_task_run_id);

CREATE INDEX IF NOT EXISTS idx_execution_metrics_workflow ON workflow_execution_metrics(workflow_id);
CREATE INDEX IF NOT EXISTS idx_execution_metrics_run ON workflow_execution_metrics(workflow_run_id);
CREATE INDEX IF NOT EXISTS idx_execution_metrics_type ON workflow_execution_metrics(metric_type);

CREATE INDEX IF NOT EXISTS idx_data_lineage_run ON workflow_data_lineage(workflow_run_id);
CREATE INDEX IF NOT EXISTS idx_data_lineage_source ON workflow_data_lineage(source_node_id);
CREATE INDEX IF NOT EXISTS idx_data_lineage_target ON workflow_data_lineage(target_node_id);

-- 9. Create views for commonly used queries
CREATE OR REPLACE VIEW vw_workflow_overview AS
SELECT 
    wh.workspace_id,
    wh.workspace_name,
    wh.folder_id,
    wh.folder_name,
    wh.job_id,
    wh.job_name,
    wh.workflow_id,
    wh.workflow_name,
    wh.workflow_status,
    wh.created_by,
    wh.created_at,
    COUNT(wn.node_id) as total_nodes,
    COUNT(CASE WHEN wn.connector_type = 'postgresql' THEN 1 END) as database_nodes,
    COUNT(CASE WHEN wn.connector_category = 'Transformations' THEN 1 END) as transformation_nodes
FROM workflow_hierarchy wh
LEFT JOIN workflow_nodes wn ON wh.workflow_id = wn.workflow_id
WHERE wh.is_active = TRUE
GROUP BY wh.workspace_id, wh.workspace_name, wh.folder_id, wh.folder_name, 
         wh.job_id, wh.job_name, wh.workflow_id, wh.workflow_name, 
         wh.workflow_status, wh.created_by, wh.created_at;

-- 10. Create view for workflow execution summary
CREATE OR REPLACE VIEW vw_workflow_execution_summary AS
SELECT 
    wrs.workflow_id,
    wh.workflow_name,
    wrs.workflow_run_id,
    wrs.prefect_flow_run_id,
    wrs.run_status,
    wrs.start_timestamp,
    wrs.end_timestamp,
    EXTRACT(EPOCH FROM (wrs.end_timestamp - wrs.start_timestamp)) as execution_time_seconds,
    COUNT(nrd.node_id) as total_tasks,
    COUNT(CASE WHEN nrd.node_status = 'Completed' THEN 1 END) as completed_tasks,
    COUNT(CASE WHEN nrd.node_status = 'Failed' THEN 1 END) as failed_tasks,
    COUNT(CASE WHEN nrd.node_status = 'Running' THEN 1 END) as running_tasks,
    SUM(nrd.target_rows) as total_rows_processed,
    SUM(nrd.parent_storage_size) as total_data_processed_mb
FROM workflow_run_stats wrs
JOIN workflow_hierarchy wh ON wrs.workflow_id = wh.workflow_id
LEFT JOIN node_run_details nrd ON wrs.workflow_run_id = nrd.workflow_run_id
GROUP BY wrs.workflow_id, wh.workflow_name, wrs.workflow_run_id, 
         wrs.prefect_flow_run_id, wrs.run_status, wrs.start_timestamp, wrs.end_timestamp;

-- 11. Function to get workflow dependency graph
CREATE OR REPLACE FUNCTION get_workflow_dependency_graph(input_workflow_id VARCHAR)
RETURNS TABLE (
    node_id VARCHAR,
    node_name VARCHAR,
    node_type VARCHAR,
    sequence_number INTEGER,
    parent_nodes TEXT[],
    child_nodes TEXT[]
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        wn.node_id::VARCHAR,
        wn.node_name::VARCHAR,
        wn.connector_type::VARCHAR,
        wn.sequence_number::INTEGER,
        COALESCE(
            ARRAY(
                SELECT wd_parent.parent_node_id 
                FROM workflow_dependencies wd_parent 
                WHERE wd_parent.child_node_id = wn.node_id 
                AND wd_parent.workflow_id = input_workflow_id
                AND wd_parent.is_active = TRUE
            ), 
            ARRAY[]::TEXT[]
        ) as parent_nodes,
        COALESCE(
            ARRAY(
                SELECT wd_child.child_node_id 
                FROM workflow_dependencies wd_child 
                WHERE wd_child.parent_node_id = wn.node_id 
                AND wd_child.workflow_id = input_workflow_id
                AND wd_child.is_active = TRUE
            ), 
            ARRAY[]::TEXT[]
        ) as child_nodes
    FROM workflow_nodes wn
    WHERE wn.workflow_id = input_workflow_id
    AND wn.is_enabled = TRUE
    ORDER BY wn.sequence_number;
END;
$$ LANGUAGE plpgsql;

import json
import pandas as pd
from datetime import datetime
import networkx as nx
import matplotlib.pyplot as plt

# Parse the connection information and dependencies
connection_info_str = '{"342cc1a8-264c-4bed-b46b-c51853f93cd0":{"connector_id":"e94b3e96-e017-4fac-aa17-2e94d4ac631c","connector_type":"postgresql","node_name":"PostgreSQL","connector_category":"Databases","dbDetails":{"db":"neondb","filter":"","schema":"public","sql":"","table":"employees","type":"sql","onErrors":"fail"},"transformations":{},"field_mapping":[],"sequence":1},"0e5c8da9-4ce9-4b81-a002-3bf8abee858e":{"connector_id":"e94b3e96-e017-4fac-aa17-2e94d4ac631c","connector_type":"postgresql","node_name":"PostgreSQL1","connector_category":"Databases","dbDetails":{"db":"neondb","schema":"public","table":"employees_target","truncateTable":false},"transformations":{},"field_mapping":[{"id":"emp_name-0.3570860318165642","value":"emp_name","name":"emp_name","type":"character varying","selected":true,"target":"emp_name","targetType":"character varying","expression":"","tableName":"employees"},{"id":"department-0.21119636505544392","value":"department","name":"department","type":"character varying","selected":true,"target":"department","targetType":"character varying","expression":"","tableName":"employees"},{"id":"job_title-0.24809578944774824","value":"job_title","name":"job_title","type":"character varying","selected":true,"target":"job_title","targetType":"character varying","expression":"","tableName":"employees"},{"id":"hire_date-0.7097862642973074","value":"hire_date","name":"hire_date","type":"date","selected":true,"target":"hire_date","targetType":"character varying","expression":"","tableName":"employees"},{"id":"birth_date-0.45689523162020207","value":"birth_date","name":"birth_date","type":"date","selected":true,"target":"birth_date","targetType":"character varying","expression":"","tableName":"employees"},{"id":"years_of_experience-0.12986324750329437","value":"years_of_experience","name":"years_of_experience","type":"integer","selected":true,"target":"years_of_experience","targetType":"numeric","expression":"","tableName":"employees"},{"id":"performance_rating-0.03322295718092794","value":"performance_rating","name":"performance_rating","type":"integer","selected":true,"target":"performance_rating","targetType":"numeric","expression":"","tableName":"employees"}],"sequence":3},"6cbc8c1b-b03e-4a99-a067-cce58ed4819a":{"connector_type":"expression","node_name":"Expression","connector_category":"Transformations","dbDetails":{},"transformations":{"type":"expression","source":{"connector_id":"e94b3e96-e017-4fac-aa17-2e94d4ac631c","connector_name":"postgresql","dbDetails":{"db":"neondb","filter":"","schema":"public","sql":"","table":"employees","type":"sql","onErrors":"fail"},"columns":[{"value":"emp_name","name":"emp_name","type":"character varying","selected":true,"table":"employees","connector_id":"e94b3e96-e017-4fac-aa17-2e94d4ac631c","isTarget":false,"expression":{"value":""}},{"value":"department","name":"department","type":"character varying","selected":true,"table":"employees","connector_id":"e94b3e96-e017-4fac-aa17-2e94d4ac631c","isTarget":false,"expression":{"value":""}},{"value":"job_title","name":"job_title","type":"character varying","selected":true,"table":"employees","connector_id":"e94b3e96-e017-4fac-aa17-2e94d4ac631c","isTarget":false,"expression":{"value":""}},{"value":"hire_date","name":"hire_date","type":"date","selected":true,"table":"employees","connector_id":"e94b3e96-e017-4fac-aa17-2e94d4ac631c","isTarget":false,"expression":{"value":""}},{"value":"birth_date","name":"birth_date","type":"date","selected":true,"table":"employees","connector_id":"e94b3e96-e017-4fac-aa17-2e94d4ac631c","isTarget":false,"expression":{"value":""}},{"value":"years_of_experience","name":"years_of_experience","type":"integer","selected":true,"table":"employees","connector_id":"e94b3e96-e017-4fac-aa17-2e94d4ac631c","isTarget":false,"expression":{"value":""}},{"value":"performance_rating","name":"performance_rating","type":"integer","selected":true,"table":"employees","connector_id":"e94b3e96-e017-4fac-aa17-2e94d4ac631c","isTarget":false,"expression":{"value":""}}]},"outputColumns":[{"value":"emp_name","name":"emp_name","type":"character varying","selected":true,"table":"employees","connector_id":"e94b3e96-e017-4fac-aa17-2e94d4ac631c","isTarget":false,"expression":{"value":""}},{"value":"department","name":"department","type":"character varying","selected":true,"table":"employees","connector_id":"e94b3e96-e017-4fac-aa17-2e94d4ac631c","isTarget":false,"expression":{"value":""}},{"value":"job_title","name":"job_title","type":"character varying","selected":true,"table":"employees","connector_id":"e94b3e96-e017-4fac-aa17-2e94d4ac631c","isTarget":false,"expression":{"value":""}},{"value":"hire_date","name":"hire_date","type":"date","selected":true,"table":"employees","connector_id":"e94b3e96-e017-4fac-aa17-2e94d4ac631c","isTarget":false,"expression":{"value":""}},{"value":"birth_date","name":"birth_date","type":"date","selected":true,"table":"employees","connector_id":"e94b3e96-e017-4fac-aa17-2e94d4ac631c","isTarget":false,"expression":{"value":""}},{"value":"years_of_experience","name":"years_of_experience","type":"integer","selected":true,"table":"employees","connector_id":"e94b3e96-e017-4fac-aa17-2e94d4ac631c","isTarget":false,"expression":{"value":""}},{"value":"performance_rating","name":"performance_rating","type":"integer","selected":true,"table":"employees","connector_id":"e94b3e96-e017-4fac-aa17-2e94d4ac631c","isTarget":false,"expression":{"value":""}}]},"field_mapping":[{"id":"emp_name-0.38175007455830345","value":"emp_name","name":"emp_name","type":"character varying","selected":true,"target":"emp_name","targetType":"character varying","expression":""},{"id":"department-0.21251186770965613","value":"department","name":"department","type":"character varying","selected":true,"target":"department","targetType":"character varying","expression":""},{"id":"job_title-0.7594103318223108","value":"job_title","name":"job_title","type":"character varying","selected":true,"target":"job_title","targetType":"character varying","expression":""},{"id":"hire_date-0.19550201576847615","value":"hire_date","name":"hire_date","type":"date","selected":true,"target":"hire_date","targetType":"date","expression":""},{"id":"birth_date-0.6458918253148211","value":"birth_date","name":"birth_date","type":"date","selected":true,"target":"birth_date","targetType":"date","expression":""},{"id":"years_of_experience-0.29612372657681696","value":"years_of_experience","name":"years_of_experience","type":"integer","selected":true,"target":"years_of_experience","targetType":"integer","expression":""},{"id":"performance_rating-0.16236231058256412","value":"performance_rating","name":"performance_rating","type":"integer","selected":true,"target":"performance_rating","targetType":"integer","expression":""}],"sequence":2}}'

dependencies_str = '{"342cc1a8-264c-4bed-b46b-c51853f93cd0":[],"0e5c8da9-4ce9-4b81-a002-3bf8abee858e":["6cbc8c1b-b03e-4a99-a067-cce58ed4819a"],"6cbc8c1b-b03e-4a99-a067-cce58ed4819a":["342cc1a8-264c-4bed-b46b-c51853f93cd0"]}'

# Parse JSON data
connections = json.loads(connection_info_str)
dependencies = json.loads(dependencies_str)

print("="*80)
print("DATA PIPELINE ANALYSIS")
print("="*80)

# 1. Pipeline Overview
print("\n1. PIPELINE OVERVIEW")
print("\n5. DETAILED DEPENDENCY MAPPING")
print("-" * 40)

print("Node ID to Name Mapping:")
for node_id, config in connections.items():
    print(f"  {node_id} -> {config['node_name']} ({config['connector_type']})")

print("\nDependency Analysis by Node:")
for node_id, config in connections.items():
    print(f"\nNode: {config['node_name']} ({node_id[:8]}...)")
    print(f"  Sequence: {config['sequence']}")
    
    # Direct dependencies
    deps = dependencies.get(node_id, [])
    if deps:
        print(f"  Direct Dependencies ({len(deps)}):")
        for dep_id in deps:
            dep_config = connections[dep_id]
            print(f"    - {dep_config['node_name']} (seq: {dep_config['sequence']}, ID: {dep_id[:8]}...)")
    else:
        print(f"  Direct Dependencies: None (ROOT NODE)")
    
    # Nodes that depend on this one
    dependents = [child_id for child_id, parent_ids in dependencies.items() if node_id in parent_ids]
    if dependents:
        print(f"  Dependent Nodes ({len(dependents)}):")
        for dep_id in dependents:
            dep_config = connections[dep_id]
            print(f"    - {dep_config['node_name']} (seq: {dep_config['sequence']}, ID: {dep_id[:8]}...)")
    else:
        print(f"  Dependent Nodes: None (LEAF NODE)")

# Create a visual representation of the dependency graph
print("\n6. DEPENDENCY GRAPH VISUALIZATION")
print("-" * 40)

def create_dependency_tree():
    """Create a text-based tree visualization"""
    # Find root nodes
    root_nodes = [node_id for node_id, deps in dependencies.items() if not deps]
    
    def print_tree(node_id, prefix="", is_last=True):
        config = connections[node_id]
        connector = "└── " if is_last else "├── "
        print(f"{prefix}{connector}{config['node_name']} (seq: {config['sequence']}, type: {config['connector_type']})")
        
        # Find children
        children = [child_id for child_id, parent_ids in dependencies.items() if node_id in parent_ids]
        
        for i, child_id in enumerate(children):
            is_last_child = i == len(children) - 1
            extension = "    " if is_last else "│   "
            print_tree(child_id, prefix + extension, is_last_child)
    
    print("Pipeline Dependency Tree:")
    for i, root_id in enumerate(root_nodes):
        is_last_root = i == len(root_nodes) - 1
        print_tree(root_id, "", is_last_root)

create_dependency_tree()

# 7. FIELD MAPPING & TRANSFORMATIONS
print(f"Total Nodes: {len(connections)}")
print(f"Database: neondb (PostgreSQL)")
print(f"Schema: public")

# 2. Node Analysis
print("\n2. NODE ANALYSIS")
print("-" * 40)

for node_id, config in connections.items():
    print(f"\nNode ID: {node_id}")
    print(f"  Name: {config['node_name']}")
    print(f"  Type: {config['connector_type']}")
    print(f"  Category: {config['connector_category']}")
    print(f"  Sequence: {config['sequence']}")
    
    if 'dbDetails' in config and 'table' in config['dbDetails']:
        table = config['dbDetails'].get('table', 'N/A')
        print(f"  Table: {table}")

# 3. DEPENDENCY GRAPH ANALYSIS
print("\n3. DEPENDENCY GRAPH ANALYSIS")
print("-" * 40)

print("Node Dependencies (Parent-Child Relationships):")
print("Format: Child Node -> [Parent Nodes]")
print()

# Analyze dependencies in detail
dependency_details = []
for child_node_id, parent_node_ids in dependencies.items():
    child_name = connections[child_node_id]['node_name']
    child_type = connections[child_node_id]['connector_type']
    child_seq = connections[child_node_id]['sequence']
    
    if parent_node_ids:
        parent_names = []
        for parent_id in parent_node_ids:
            parent_name = connections[parent_id]['node_name']
            parent_type = connections[parent_id]['connector_type']
            parent_seq = connections[parent_id]['sequence']
            parent_names.append(f"{parent_name} (seq: {parent_seq}, type: {parent_type})")
        
        dependency_details.append({
            'Child': f"{child_name} (seq: {child_seq})",
            'Child_Type': child_type,
            'Child_ID': child_node_id,
            'Parents': parent_names,
            'Parent_IDs': parent_node_ids
        })
        
        print(f"{child_name} (ID: {child_node_id[:8]}..., seq: {child_seq})")
        print(f"  Type: {child_type}")
        print(f"  Depends on: {', '.join(parent_names)}")
        print()
    else:
        dependency_details.append({
            'Child': f"{child_name} (seq: {child_seq})",
            'Child_Type': child_type,
            'Child_ID': child_node_id,
            'Parents': ['ROOT NODE - No dependencies'],
            'Parent_IDs': []
        })
        print(f"{child_name} (ID: {child_node_id[:8]}..., seq: {child_seq})")
        print(f"  Type: {child_type}")
        print(f"  Status: ROOT NODE - No dependencies")
        print()

# Create reverse dependency mapping (find children for each parent)
print("Reverse Dependencies (Parent-Child Relationships):")
print("Format: Parent Node -> [Child Nodes]")
print()

reverse_deps = {}
for child_id, parent_ids in dependencies.items():
    for parent_id in parent_ids:
        if parent_id not in reverse_deps:
            reverse_deps[parent_id] = []
        reverse_deps[parent_id].append(child_id)

# Also include nodes with no children
for node_id in connections.keys():
    if node_id not in reverse_deps:
        reverse_deps[node_id] = []

for parent_id, child_ids in reverse_deps.items():
    parent_name = connections[parent_id]['node_name']
    parent_seq = connections[parent_id]['sequence']
    
    if child_ids:
        child_names = []
        for child_id in child_ids:
            child_name = connections[child_id]['node_name']
            child_seq = connections[child_id]['sequence']
            child_names.append(f"{child_name} (seq: {child_seq})")
        
        print(f"{parent_name} (ID: {parent_id[:8]}..., seq: {parent_seq})")
        print(f"  Children: {', '.join(child_names)}")
        print()
    else:
        print(f"{parent_name} (ID: {parent_id[:8]}..., seq: {parent_seq})")
        print(f"  Status: LEAF NODE - No children")
        print()

# 4. EXECUTION PATH ANALYSIS
print("\n4. EXECUTION PATH ANALYSIS")
print("-" * 40)

def get_execution_path():
    """Determine the correct execution path based on dependencies"""
    # Find root nodes (no dependencies)
    root_nodes = [node_id for node_id, deps in dependencies.items() if not deps]
    
    # Topological sort to get execution order
    visited = set()
    path = []
    
    def dfs(node_id):
        if node_id in visited:
            return
        visited.add(node_id)
        
        # Visit all dependencies first
        for dep_id in dependencies.get(node_id, []):
            dfs(dep_id)
        
        path.append(node_id)
    
    # Start DFS from all nodes to ensure we get complete path
    for node_id in connections.keys():
        dfs(node_id)
    
    return path

execution_path = get_execution_path()

print("Correct Execution Order (based on dependencies):")
for i, node_id in enumerate(execution_path, 1):
    node_name = connections[node_id]['node_name']
    node_type = connections[node_id]['connector_type']
    sequence = connections[node_id]['sequence']
    deps = dependencies.get(node_id, [])
    
    print(f"Step {i}: {node_name}")
    print(f"  Node ID: {node_id}")
    print(f"  Type: {node_type}")
    print(f"  Sequence: {sequence}")
    print(f"  Dependencies: {len(deps)} parent(s)")
    if deps:
        dep_names = [connections[dep]['node_name'] for dep in deps]
        print(f"  Must wait for: {', '.join(dep_names)}")
    print()

# Validate sequence vs dependency order
print("Sequence vs Dependency Validation:")
sequence_order = sorted(connections.items(), key=lambda x: x[1]['sequence'])
dependency_order = [(node_id, connections[node_id]) for node_id in execution_path]

print("Sequence Order vs Dependency Order:")
for i, ((seq_id, seq_config), (dep_id, dep_config)) in enumerate(zip(sequence_order, dependency_order)):
    match = "✓" if seq_id == dep_id else "✗"
    print(f"Position {i+1}: {match}")
    print(f"  By Sequence: {seq_config['node_name']} (seq: {seq_config['sequence']})")
    print(f"  By Dependencies: {dep_config['node_name']} (seq: {dep_config['sequence']})")
    if seq_id != dep_id:
        print(f"  ⚠️  MISMATCH DETECTED!")
    print()

# 4. Field Mapping Analysis
print("\n4. FIELD MAPPING & TRANSFORMATIONS")
print("-" * 40)

# Analyze field mappings for each node
for node_id, config in connections.items():
    if config.get('field_mapping'):
        print(f"\n{config['node_name']} Field Mappings:")
        field_mappings = config['field_mapping']
        
        # Create DataFrame for better visualization
        mapping_data = []
        for mapping in field_mappings:
            mapping_data.append({
                'Source Field': mapping['name'],
                'Source Type': mapping['type'],
                'Target Field': mapping['target'],
                'Target Type': mapping['targetType'],
                'Has Expression': bool(mapping.get('expression', '').strip())
            })
        
        df = pd.DataFrame(mapping_data)
        print(df.to_string(index=False))

# 5. Data Type Analysis
print("\n5. DATA TYPE ANALYSIS")
print("-" * 40)

# Analyze data type conversions
type_conversions = []
for node_id, config in connections.items():
    if config.get('field_mapping'):
        for mapping in config['field_mapping']:
            source_type = mapping['type']
            target_type = mapping['targetType']
            if source_type != target_type:
                type_conversions.append({
                    'Node': config['node_name'],
                    'Field': mapping['name'],
                    'From': source_type,
                    'To': target_type
                })

if type_conversions:
    print("Data Type Conversions:")
    conversion_df = pd.DataFrame(type_conversions)
    print(conversion_df.to_string(index=False))
else:
    print("No data type conversions detected.")

# 6. Pipeline Architecture Summary
print("\n6. PIPELINE ARCHITECTURE SUMMARY")
print("-" * 40)

print("Pipeline Flow:")
print("1. Source: PostgreSQL.employees table")
print("2. Transform: Expression node (pass-through with field mappings)")
print("3. Target: PostgreSQL1.employees_target table")

print("\nKey Characteristics:")
print("- Simple ETL pipeline with minimal transformations")
print("- Same database (neondb) for source and target")
print("- Date fields converted to character varying in target")
print("- Integer fields converted to numeric in target")
print("- All source fields are selected and mapped")

# 7. Potential Issues & Recommendations
print("\n7. POTENTIAL ISSUES & RECOMMENDATIONS")
print("-" * 40)

print("Potential Issues:")
print("- Date to string conversion may lose date functionality")
print("- No data validation or error handling visible")
print("- No incremental loading strategy apparent")

print("\nRecommendations:")
print("- Consider keeping date fields as date type in target")
print("- Implement data quality checks")
print("- Add error handling and logging")
print("- Consider adding filters or transformations if needed")

# 8. Field Summary
print("\n8. FIELD SUMMARY")
print("-" * 40)

# Get all unique fields from the pipeline
all_fields = set()
for node_id, config in connections.items():
    if config.get('field_mapping'):
        for mapping in config['field_mapping']:
            all_fields.add(mapping['name'])

print(f"Total Fields Processed: {len(all_fields)}")
print("Fields:", ', '.join(sorted(all_fields)))

print("\n" + "="*80)
print("ANALYSIS COMPLETE")
print("="*80)

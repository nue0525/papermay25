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

# 8. DEPENDENCY OPTIMIZATION ANALYSIS
print("\n8. DEPENDENCY OPTIMIZATION ANALYSIS")
print("-" * 40)

def analyze_dependency_efficiency():
    """Analyze the efficiency and optimization opportunities in the dependency structure"""
    
    print("Pipeline Efficiency Analysis:")
    print()
    
    # Calculate dependency depth
    def get_dependency_depth(node_id, visited=None):
        if visited is None:
            visited = set()
        if node_id in visited:
            return 0  # Circular dependency protection
        visited.add(node_id)
        
        deps = dependencies.get(node_id, [])
        if not deps:
            return 0
        return 1 + max(get_dependency_depth(dep_id, visited.copy()) for dep_id in deps)
    
    depths = {}
    for node_id in connections.keys():
        depths[node_id] = get_dependency_depth(node_id)
    
    max_depth = max(depths.values())
    print(f"Pipeline Depth: {max_depth} levels")
    print(f"Critical Path Length: {max_depth + 1} nodes")
    print()
    
    # Analyze parallelization opportunities
    levels = {}
    for node_id, depth in depths.items():
        if depth not in levels:
            levels[depth] = []
        levels[depth].append(node_id)
    
    print("Execution Levels (nodes that can run in parallel):")
    for level in sorted(levels.keys()):
        node_names = [connections[node_id]['node_name'] for node_id in levels[level]]
        print(f"  Level {level}: {', '.join(node_names)} ({len(node_names)} node(s))")
    
    parallel_potential = sum(1 for level_nodes in levels.values() if len(level_nodes) > 1)
    print(f"\nParallelization Potential: {'Low' if parallel_potential == 0 else 'Medium' if parallel_potential == 1 else 'High'}")
    print(f"Reason: {'Linear pipeline' if parallel_potential == 0 else f'{parallel_potential} levels with multiple nodes'}")

analyze_dependency_efficiency()

# 9. DEPENDENCY VALIDATION & CHECKS
print("\n9. DEPENDENCY VALIDATION & CHECKS")
print("-" * 40)

def validate_dependencies():
    """Perform comprehensive dependency validation"""
    
    issues = []
    warnings = []
    recommendations = []
    
    print("Dependency Health Check:")
    print()
    
    # Check 1: Circular dependencies
    def has_circular_dependency(node_id, path=None):
        if path is None:
            path = []
        if node_id in path:
            return path + [node_id]
        
        path = path + [node_id]
        for dep_id in dependencies.get(node_id, []):
            result = has_circular_dependency(dep_id, path)
            if result:
                return result
        return None
    
    circular_deps = []
    for node_id in connections.keys():
        circular = has_circular_dependency(node_id)
        if circular:
            circular_deps.append(circular)
    
    if circular_deps:
        issues.append("Circular Dependencies Detected")
        for cycle in circular_deps:
            cycle_names = [connections[nid]['node_name'] for nid in cycle]
            print(f"  ❌ Circular dependency: {' -> '.join(cycle_names)}")
    else:
        print("  ✅ No circular dependencies detected")
    
    # Check 2: Orphaned nodes
    all_referenced = set()
    for deps in dependencies.values():
        all_referenced.update(deps)
    
    orphaned = set(connections.keys()) - all_referenced - set(dependencies.keys())
    if orphaned:
        warnings.append("Orphaned Nodes Found")
        for node_id in orphaned:
            print(f"  ⚠️  Orphaned node: {connections[node_id]['node_name']}")
    else:
        print("  ✅ No orphaned nodes")
    
    # Check 3: Missing dependencies
    missing_deps = []
    for node_id, deps in dependencies.items():
        for dep_id in deps:
            if dep_id not in connections:
                missing_deps.append((node_id, dep_id))
    
    if missing_deps:
        issues.append("Missing Dependency Nodes")
        for node_id, missing_dep in missing_deps:
            print(f"  ❌ {connections[node_id]['node_name']} depends on missing node: {missing_dep}")
    else:
        print("  ✅ All dependencies exist")
    
    # Check 4: Sequence vs Dependency order validation
    sequence_issues = []
    for node_id, deps in dependencies.items():
        node_seq = connections[node_id]['sequence']
        for dep_id in deps:
            dep_seq = connections[dep_id]['sequence']
            if dep_seq >= node_seq:
                sequence_issues.append((node_id, dep_id))
    
    if sequence_issues:
        warnings.append("Sequence Order Issues")
        for node_id, dep_id in sequence_issues:
            node_name = connections[node_id]['node_name']
            dep_name = connections[dep_id]['node_name']
            node_seq = connections[node_id]['sequence']
            dep_seq = connections[dep_id]['sequence']
            print(f"  ⚠️  {node_name} (seq:{node_seq}) should run after {dep_name} (seq:{dep_seq})")
    else:
        print("  ✅ Sequence order matches dependencies")
    
    return issues, warnings, recommendations

issues, warnings, recommendations = validate_dependencies()

# 10. PERFORMANCE OPTIMIZATION RECOMMENDATIONS
print("\n10. PERFORMANCE OPTIMIZATION RECOMMENDATIONS")
print("-" * 40)

def generate_optimization_recommendations():
    """Generate specific recommendations for pipeline optimization"""
    
    print("Performance Optimization Opportunities:")
    print()
    
    # Analyze current structure
    source_nodes = [nid for nid, config in connections.items() 
                   if config['connector_type'] == 'postgresql' and not dependencies.get(nid, [])]
    target_nodes = [nid for nid, deps in dependencies.items() 
                   if not any(nid in other_deps for other_deps in dependencies.values())]
    transform_nodes = [nid for nid, config in connections.items() 
                      if config['connector_type'] == 'expression']
    
    print("Current Architecture Analysis:")
    print(f"  Source Nodes: {len(source_nodes)}")
    print(f"  Transform Nodes: {len(transform_nodes)}")  
    print(f"  Target Nodes: {len(target_nodes)}")
    print()
    
    # Specific recommendations
    recommendations = []
    
    # 1. Transformation readiness optimization
    if transform_nodes:
        transform_config = connections[transform_nodes[0]]
        field_mappings = transform_config.get('field_mapping', [])
        
        # Check transformation readiness for future expressions
        passthrough_fields = [f for f in field_mappings if not f.get('expression', '').strip()]
        if len(passthrough_fields) == len(field_mappings):
            recommendations.append({
                'type': 'Architecture',
                'priority': 'Medium',
                'title': 'Optimize Expression Node for Future Transformations',
                'description': 'Currently pass-through but ready for expressions. Consider adding validation logic or computed fields.',
                'impact': 'Better preparation for future business logic, data validation capabilities'
            })
            
            # Suggest specific expression opportunities
            recommendations.append({
                'type': 'Enhancement',
                'priority': 'Low',
                'title': 'Add Computed Fields and Validations',
                'description': 'Consider adding: age calculation from birth_date, tenure from hire_date, performance categories',
                'impact': 'Enhanced data quality, business value addition'
            })
    
    # 2. Data type optimization
    type_changes = []
    for node_id, config in connections.items():
        for mapping in config.get('field_mapping', []):
            if mapping['type'] != mapping['targetType']:
                type_changes.append({
                    'field': mapping['name'],
                    'from': mapping['type'],
                    'to': mapping['targetType'],
                    'node': config['node_name']
                })
    
    if type_changes:
        date_to_string = [tc for tc in type_changes if 'date' in tc['from'] and 'character' in tc['to']]
        if date_to_string:
            recommendations.append({
                'type': 'Data Quality',
                'priority': 'Medium',
                'title': 'Preserve Date Data Types',
                'description': f"Converting {len(date_to_string)} date fields to strings loses functionality",
                'impact': 'Better query performance, date operations support'
            })
    
    # 3. Error handling
    error_handling = any(config.get('dbDetails', {}).get('onErrors') for config in connections.values())
    if not error_handling:
        recommendations.append({
            'type': 'Reliability',
            'priority': 'High', 
            'title': 'Implement Error Handling',
            'description': 'No error handling strategy detected',
            'impact': 'Improved reliability, easier debugging'
        })
    
    # 4. Incremental loading
    has_filters = any(config.get('dbDetails', {}).get('filter') for config in connections.values())
    if not has_filters:
        recommendations.append({
            'type': 'Performance',
            'priority': 'Medium',
            'title': 'Add Incremental Loading',
            'description': 'No filters detected - consider incremental updates',
            'impact': 'Faster execution, reduced resource usage'
        })
    
    # Display recommendations
    for i, rec in enumerate(recommendations, 1):
        priority_icon = {'High': '🔥', 'Medium': '⚡', 'Low': '💡'}.get(rec['priority'], '📝')
        print(f"{i}. {priority_icon} {rec['title']} [{rec['type']} - {rec['priority']} Priority]")
        print(f"   Description: {rec['description']}")
        print(f"   Impact: {rec['impact']}")
        print()
    
    return recommendations

optimization_recs = generate_optimization_recommendations()

# 11. DEPENDENCY MONITORING STRATEGY
print("\n11. DEPENDENCY MONITORING STRATEGY")
print("-" * 40)

def create_monitoring_strategy():
    """Create a monitoring strategy for the pipeline dependencies"""
    
    print("Recommended Monitoring Points:")
    print()
    
    # Critical path monitoring
    execution_path = get_execution_path()
    print("Critical Path Monitoring:")
    for i, node_id in enumerate(execution_path):
        config = connections[node_id]
        print(f"  {i+1}. Monitor {config['node_name']}")
        
        if config['connector_type'] == 'postgresql':
            if 'employees_target' in config.get('dbDetails', {}).get('table', ''):
                print(f"     - Row count validation")
                print(f"     - Data quality checks")
                print(f"     - Execution time tracking")
            else:
                print(f"     - Source availability")
                print(f"     - Connection health")
        elif config['connector_type'] == 'expression':
            print(f"     - Transformation success rate")
            print(f"     - Field mapping validation")
        print()
    
    # Dependency-specific monitoring
    print("Dependency Health Monitoring:")
    print("  - Dependency chain validation before execution")
    print("  - Node availability checks")
    print("  - Sequence order verification")
    print("  - Resource dependency monitoring")
    print()
    
    # Alert thresholds
    print("Recommended Alert Thresholds:")
    print("  - Pipeline execution time > 2x baseline")
    print("  - Any node failure in critical path")
    print("  - Dependency validation failures")
    print("  - Data volume deviation > 25%")

create_monitoring_strategy()

# 12. EXPRESSION NODE ENHANCEMENT OPPORTUNITIES
print("\n12. EXPRESSION NODE ENHANCEMENT OPPORTUNITIES")
print("-" * 40)

def analyze_expression_opportunities():
    """Analyze opportunities for adding meaningful expressions to the transform node"""
    
    # Find the expression node
    expression_node = None
    for node_id, config in connections.items():
        if config['connector_type'] == 'expression':
            expression_node = (node_id, config)
            break
    
    if not expression_node:
        print("No expression node found")
        return
    
    node_id, config = expression_node
    field_mappings = config.get('field_mapping', [])
    
    print("Current Expression Node Analysis:")
    print(f"  Node: {config['node_name']}")
    print(f"  Fields processed: {len(field_mappings)}")
    print(f"  Current expressions: {sum(1 for f in field_mappings if f.get('expression', '').strip())}")
    print()
    
    # Suggest meaningful expressions based on available fields
    field_types = {}
    for mapping in field_mappings:
        field_types[mapping['name']] = mapping['type']
    
    print("Suggested Expression Enhancements:")
    print()
    
    suggestions = []
    
    # Date-based calculations
    if 'birth_date' in field_types and 'hire_date' in field_types:
        suggestions.append({
            'category': 'Date Calculations',
            'expressions': [
                {
                    'field': 'age_at_hire',
                    'expression': 'EXTRACT(YEAR FROM AGE(hire_date, birth_date))',
                    'description': 'Calculate age when hired',
                    'type': 'integer'
                },
                {
                    'field': 'current_age',
                    'expression': 'EXTRACT(YEAR FROM AGE(CURRENT_DATE, birth_date))',
                    'description': 'Calculate current age',
                    'type': 'integer'
                },
                {
                    'field': 'tenure_years',
                    'expression': 'EXTRACT(YEAR FROM AGE(CURRENT_DATE, hire_date))',
                    'description': 'Calculate years of service',
                    'type': 'integer'
                }
            ]
        })
    
    # Performance-based calculations
    if 'performance_rating' in field_types and 'years_of_experience' in field_types:
        suggestions.append({
            'category': 'Performance Analytics',
            'expressions': [
                {
                    'field': 'performance_category',
                    'expression': '''CASE 
                        WHEN performance_rating >= 4 THEN 'Excellent'
                        WHEN performance_rating >= 3 THEN 'Good' 
                        WHEN performance_rating >= 2 THEN 'Satisfactory'
                        ELSE 'Needs Improvement'
                    END''',
                    'description': 'Categorize performance ratings',
                    'type': 'character varying'
                },
                {
                    'field': 'experience_level',
                    'expression': '''CASE 
                        WHEN years_of_experience >= 10 THEN 'Senior'
                        WHEN years_of_experience >= 5 THEN 'Mid-Level'
                        WHEN years_of_experience >= 2 THEN 'Junior'
                        ELSE 'Entry Level'
                    END''',
                    'description': 'Categorize experience levels',
                    'type': 'character varying'
                }
            ]
        })
    
    # Data validation expressions
    suggestions.append({
        'category': 'Data Validation',
        'expressions': [
            {
                'field': 'data_quality_flag',
                'expression': '''CASE 
                    WHEN emp_name IS NULL OR emp_name = '' THEN 'Invalid Name'
                    WHEN department IS NULL OR department = '' THEN 'Missing Department'
                    WHEN hire_date > CURRENT_DATE THEN 'Future Hire Date'
                    WHEN birth_date > CURRENT_DATE THEN 'Future Birth Date'
                    ELSE 'Valid'
                END''',
                'description': 'Flag data quality issues',
                'type': 'character varying'
            },
            {
                'field': 'record_processed_at',
                'expression': 'CURRENT_TIMESTAMP',
                'description': 'Track when record was processed',
                'type': 'timestamp'
            }
        ]
    })
    
    # Display suggestions
    for suggestion in suggestions:
        print(f"📊 {suggestion['category']}:")
        for expr in suggestion['expressions']:
            print(f"  • {expr['field']} ({expr['type']})")
            print(f"    Expression: {expr['expression']}")
            print(f"    Purpose: {expr['description']}")
            print()
    
    return suggestions

expression_suggestions = analyze_expression_opportunities()

# 13. FUTURE-READY OPTIMIZATION RECOMMENDATIONS  
print("\n13. FUTURE-READY OPTIMIZATION RECOMMENDATIONS")
print("-" * 40)

def generate_future_ready_recommendations():
    """Generate recommendations that prepare the pipeline for future enhancements"""
    
    print("Recommendations for Expression Node Enhancement:")
    print()
    
    future_recommendations = [
        {
            'type': 'Scalability',
            'priority': 'High',
            'title': 'Implement Expression Error Handling',
            'description': 'Add error handling for complex expressions to prevent pipeline failures',
            'implementation': 'Use TRY-CATCH blocks or COALESCE for safe expressions',
            'impact': 'Robust pipeline execution when adding business logic'
        },
        {
            'type': 'Performance',  
            'priority': 'High',
            'title': 'Optimize Target Data Types',
            'description': 'Keep dates as date types instead of converting to varchar for better performance',
            'implementation': 'Change hire_date and birth_date target types back to date',
            'impact': 'Better query performance, date arithmetic support'
        },
        {
            'type': 'Monitoring',
            'priority': 'Medium', 
            'title': 'Add Expression Execution Metrics',
            'description': 'Track transformation success rates and execution times',
            'implementation': 'Add logging for expression evaluation and performance counters',
            'impact': 'Better visibility into transformation performance'
        },
        {
            'type': 'Data Quality',
            'priority': 'Medium',
            'title': 'Implement Data Validation Layer',
            'description': 'Add validation expressions before complex transformations',
            'implementation': 'Create validation rules in expression node for data quality checks',
            'impact': 'Improved data quality, early error detection'
        },
        {
            'type': 'Architecture',
            'priority': 'Low',
            'title': 'Prepare for Expression Versioning',
            'description': 'Design expressions to be versionable for future updates',
            'implementation': 'Document expression logic and maintain version history',
            'impact': 'Easier maintenance and rollback capabilities'
        }
    ]
    
    # Display recommendations with implementation focus
    for i, rec in enumerate(future_recommendations, 1):
        priority_icon = {'High': '🔥', 'Medium': '⚡', 'Low': '💡'}.get(rec['priority'], '📝')
        print(f"{i}. {priority_icon} {rec['title']} [{rec['type']} - {rec['priority']} Priority]")
        print(f"   Description: {rec['description']}")
        print(f"   Implementation: {rec['implementation']}")
        print(f"   Impact: {rec['impact']}")
        print()
    
print("\n" + "="*80)
print("DEPENDENCY OPTIMIZATION & EXPRESSION ENHANCEMENT ANALYSIS COMPLETE")
print("="*80)

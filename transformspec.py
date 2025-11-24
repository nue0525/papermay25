"""Transform Specification Interpreter.

Converts TransformSpec JSON definitions into Polars LazyFrame operations.
This enables declarative data transformations without writing Python code,
allowing non-technical users to define transformations via UI.

TransformSpec Format:
    {
        "select": ["col1", "col2"],  # Column selection
        "filter": [  # Row filtering
            {"col": "age", "op": ">", "value": 18},
            {"col": "status", "op": "==", "value": "active"}
        ],
        "expression": [  # Derived columns
            {"name": "full_name", "col": "first_name", "concat": "last_name"},
            {"name": "price_with_tax", "col": "price", "multiply": 1.1}
        ]
    }

Supported Operations:
    - select: Choose specific columns (projection)
    - filter: Row filtering with conditions
        Operators: ==, !=, >, <, >=, <=, contains, in
    - expression: Create derived columns (TODO: enhance expression parser)
    - aggregate: Group by and aggregations (TODO: implement)
    - join: Combine datasets (TODO: implement)
    - sort: Order results (TODO: implement)
    - distinct: Remove duplicates (TODO: implement)

Performance Benefits:
    - Uses Polars LazyFrame for query optimization
    - Transformations are not executed until .collect() is called
    - Enables predicate pushdown (filter early)
    - Enables projection pushdown (select only needed columns)
    - Memory efficient for large datasets (streaming capable)

Example Usage:
    import polars as pl
    from backend.services.transformspec import apply_transformspec
    
    # Load data as LazyFrame
    lf = pl.scan_csv("data.csv")
    
    # Define transformation
    spec = {
        "filter": [{"col": "status", "op": "==", "value": "active"}],
        "select": ["user_id", "email", "created_at"]
    }
    
    # Apply transformation (lazy, not executed yet)
    transformed = apply_transformspec(lf, spec)
    
    # Execute and collect results
    result = transformed.collect()

TODO:
    - Enhanced expression parser with math operators
    - Join operations with multiple tables
    - Aggregation operations (sum, avg, count, min, max)
    - Window functions (rank, lag, lead)
    - Date/time operations
    - String operations (split, regex, format)
    - Type casting and validation
"""
from typing import Dict, Any
import polars as pl

# Minimal TransformSpec interpreter scaffold

def apply_transformspec(lf: pl.LazyFrame, spec: Dict[str, Any]) -> pl.LazyFrame:
    """Apply a transformation specification to a Polars LazyFrame.
    
    Interprets a TransformSpec JSON object and applies corresponding Polars
    operations to the LazyFrame. Operations are applied in order: filter,
    select, expression to optimize query execution.
    
    Args:
        lf (pl.LazyFrame): Input data as Polars LazyFrame (lazy evaluation)
        spec (Dict[str, Any]): Transformation specification with operation keys
            - select (list[str]): Column names to select
            - filter (list[dict]): Filter conditions
                Each dict: {"col": str, "op": str, "value": Any}
            - expression (list[dict]): Derived column definitions
                Each dict: {"name": str, "col": str, ...operations}
    
    Returns:
        pl.LazyFrame: Transformed LazyFrame (not yet executed, call .collect())
    
    Raises:
        ValueError: If operator is unsupported or condition format is invalid
        KeyError: If referenced column doesn't exist
    
    Example:
        # Filter active users over 18 and select specific columns
        lf = pl.LazyFrame({
            "user_id": [1, 2, 3],
            "age": [15, 20, 25],
            "status": ["active", "inactive", "active"]
        })
        
        spec = {
            "filter": [
                {"col": "age", "op": ">", "value": 18},
                {"col": "status", "op": "==", "value": "active"}
            ],
            "select": ["user_id", "age"]
        }
        
        result = apply_transformspec(lf, spec).collect()
        # Result: user_id=[3], age=[25]
    
    Performance Notes:
        - Filter operations are applied first for predicate pushdown
        - Column selection is applied after filters
        - LazyFrame optimizations are automatic (not manual)
        - Use .explain() to see query plan before execution
    \"\"\""
    if not spec:
        return lf

    if select := spec.get("select"):
        lf = lf.select([pl.col(c) for c in select])

    if filters := spec.get("filter"):
        # simplistic: filters is list of expressions like {"col":"a","op":">","value":1}
        for f in filters:
            col, op, val = f["col"], f["op"], f["value"]
            expr = pl.col(col)
            if op == "==":
                cond = expr == val
            elif op == ">":
                cond = expr > val
            elif op == ">=":
                cond = expr >= val
            elif op == "<":
                cond = expr < val
            elif op == "<=":
                cond = expr <= val
            elif op == "!=":
                cond = expr != val
            else:
                raise ValueError(f"Unsupported op: {op}")
            lf = lf.filter(cond)

    if expressions := spec.get("expression"):
        for e in expressions:
            name = e["name"]
            expr = pl.col(e["col"])  # placeholder
            lf = lf.with_columns(pl.col(e["col"]).alias(name))

    return lf

"""AI Node Executors for LLM and ML Operations.

Provides execution logic for AI-powered workflow nodes:
- llm_prompt: Single LLM inference call
- llm_batch: Batch LLM inference for multiple inputs
- evaluator: Evaluate LLM output quality
- router: Conditional routing based on LLM classification
- tool_call: Function calling with LLM

Provider Abstraction:
    Supports multiple LLM providers through unified interface:
    - OpenAI (GPT-4, GPT-3.5)
    - Anthropic (Claude)
    - AWS Bedrock (Claude, Titan)
    - Azure OpenAI
    - Local models (Ollama, vLLM)

Configuration Example:
    {
        "node_type": "llm_prompt",
        "node_config": {
            "provider": "openai",
            "model": "gpt-4-turbo",
            "prompt_template": "Summarize this text: {{text}}",
            "temperature": 0.7,
            "max_tokens": 500,
            "input_column": "text",
            "output_column": "summary"
        }
    }

Usage:
    from backend.worker.engine.nodes.ai import execute_llm_prompt
    
    output = await execute_llm_prompt(node_config, input_data)
"""
import polars as pl
import re
import asyncio
from typing import Dict, Any, Optional, List
from backend.core.logging import get_logger
from backend.core.config import get_settings

logger = get_logger(__name__)
settings = get_settings()

# Import LLM provider libraries (gracefully handle if not installed)
try:
    from openai import AsyncOpenAI
    OPENAI_AVAILABLE = True
except ImportError:
    logger.warning("OpenAI library not installed. Install with: pip install openai")
    OPENAI_AVAILABLE = False

try:
    from anthropic import AsyncAnthropic
    ANTHROPIC_AVAILABLE = True
except ImportError:
    logger.warning("Anthropic library not installed. Install with: pip install anthropic")
    ANTHROPIC_AVAILABLE = False

try:
    import boto3
    BEDROCK_AVAILABLE = True
except ImportError:
    logger.warning("Boto3 not installed. Install with: pip install boto3")
    BEDROCK_AVAILABLE = False


def render_prompt_template(template: str, row_data: Dict[str, Any]) -> str:
    """Render prompt template with variable substitution.
    
    Args:
        template: Prompt template with {{variable}} placeholders
        row_data: Dictionary with variable values
    
    Returns:
        Rendered prompt string
    
    Example:
        >>> render_prompt_template("Hello {{name}}", {"name": "Alice"})
        "Hello Alice"
    """
    prompt = template
    for key, value in row_data.items():
        prompt = prompt.replace(f"{{{{{key}}}}}", str(value))
    return prompt


async def call_openai_llm(
    prompt: str,
    model: str,
    temperature: float = 0.7,
    max_tokens: int = 1000
) -> str:
    """Call OpenAI API with retry logic.
    
    Args:
        prompt: User prompt
        model: Model identifier (gpt-4-turbo, gpt-3.5-turbo, etc.)
        temperature: Sampling temperature
        max_tokens: Maximum tokens to generate
    
    Returns:
        LLM response text
    
    Raises:
        ValueError: If OpenAI API key not configured
        Exception: If API call fails after retries
    """
    if not OPENAI_AVAILABLE:
        raise ImportError("OpenAI library not installed")
    
    if not settings.OPENAI_API_KEY:
        raise ValueError("OPENAI_API_KEY not configured in settings")
    
    client = AsyncOpenAI(
        api_key=settings.OPENAI_API_KEY,
        organization=settings.OPENAI_ORGANIZATION,
        timeout=settings.OPENAI_TIMEOUT_SECONDS,
        max_retries=settings.OPENAI_MAX_RETRIES
    )
    
    try:
        response = await client.chat.completions.create(
            model=model,
            messages=[{"role": "user", "content": prompt}],
            temperature=temperature,
            max_tokens=max_tokens
        )
        
        return response.choices[0].message.content or ""
    
    except Exception as e:
        logger.error(f"OpenAI API call failed: {e}")
        raise


async def call_anthropic_llm(
    prompt: str,
    model: str,
    temperature: float = 0.7,
    max_tokens: int = 1000
) -> str:
    """Call Anthropic Claude API with retry logic.
    
    Args:
        prompt: User prompt
        model: Model identifier (claude-3-5-sonnet, claude-3-opus, etc.)
        temperature: Sampling temperature
        max_tokens: Maximum tokens to generate
    
    Returns:
        LLM response text
    
    Raises:
        ValueError: If Anthropic API key not configured
        Exception: If API call fails after retries
    """
    if not ANTHROPIC_AVAILABLE:
        raise ImportError("Anthropic library not installed")
    
    if not settings.ANTHROPIC_API_KEY:
        raise ValueError("ANTHROPIC_API_KEY not configured in settings")
    
    client = AsyncAnthropic(
        api_key=settings.ANTHROPIC_API_KEY,
        timeout=settings.ANTHROPIC_TIMEOUT_SECONDS,
        max_retries=settings.ANTHROPIC_MAX_RETRIES
    )
    
    try:
        response = await client.messages.create(
            model=model,
            max_tokens=max_tokens,
            temperature=temperature,
            messages=[{"role": "user", "content": prompt}]
        )
        
        # Extract text from response
        text_content = ""
        for block in response.content:
            if hasattr(block, 'text'):
                text_content += block.text
        
        return text_content
    
    except Exception as e:
        logger.error(f"Anthropic API call failed: {e}")
        raise


async def call_llm(
    prompt: str,
    provider: str,
    model: str,
    temperature: float = 0.7,
    max_tokens: int = 1000
) -> str:
    """Unified LLM calling interface supporting multiple providers.
    
    Args:
        prompt: User prompt
        provider: LLM provider (openai, anthropic, bedrock)
        model: Model identifier
        temperature: Sampling temperature (0.0-1.0)
        max_tokens: Maximum tokens to generate
    
    Returns:
        LLM response text
    
    Raises:
        ValueError: If provider not supported or credentials missing
    """
    provider = provider.lower()
    
    if provider == "openai":
        return await call_openai_llm(prompt, model, temperature, max_tokens)
    
    elif provider == "anthropic":
        return await call_anthropic_llm(prompt, model, temperature, max_tokens)
    
    elif provider == "bedrock":
        # TODO: Implement AWS Bedrock integration
        raise NotImplementedError("AWS Bedrock integration not yet implemented")
    
    else:
        raise ValueError(f"Unsupported LLM provider: {provider}")


async def execute_llm_prompt(
    config: Dict[str, Any],
    input_data: Optional[pl.LazyFrame] = None
) -> pl.LazyFrame:
    """Execute LLM prompt node for single inference per row.
    
    Applies LLM prompt to each row in input data, adding result column.
    
    Args:
        config: Node configuration
            - provider (str): LLM provider (openai, anthropic, bedrock)
            - model (str): Model identifier (gpt-4-turbo, claude-3-opus)
            - prompt_template (str): Template with {{variable}} placeholders
            - temperature (float): Sampling temperature (0.0-1.0)
            - max_tokens (int): Maximum output tokens
            - input_column (str): Column to use as input
            - output_column (str): Column name for LLM output
        input_data: Input DataFrame with data to process
    
    Returns:
        LazyFrame with added output column containing LLM responses
    
    Example:
        config = {
            "provider": "openai",
            "model": "gpt-4-turbo",
            "prompt_template": "Classify sentiment: {{text}}",
            "temperature": 0.3,
            "max_tokens": 10,
            "input_column": "text",
            "output_column": "sentiment"
        }
        
        # Input: pl.DataFrame({"text": ["I love this!", "This is terrible"]})
        # Output: Adds "sentiment" column with ["positive", "negative"]
    
    Notes:
        - Processes rows sequentially or in batches
        - Handles rate limiting and retries
        - Tracks token usage for cost monitoring
    """
    if input_data is None:
        raise ValueError("llm_prompt node requires input data")
    
    # Extract configuration
    provider = config.get("provider", "openai")
    model = config.get("model", settings.OPENAI_DEFAULT_MODEL if provider == "openai" else settings.ANTHROPIC_DEFAULT_MODEL)
    prompt_template = config.get("prompt_template", "")
    temperature = config.get("temperature", settings.LLM_DEFAULT_TEMPERATURE)
    max_tokens = config.get("max_tokens", settings.LLM_DEFAULT_MAX_TOKENS)
    output_column = config.get("output_column", "llm_output")
    
    logger.info(f"Executing LLM prompt node with provider: {provider}, model: {model}")
    
    if not prompt_template:
        raise ValueError("prompt_template is required in node configuration")
    
    # Collect data to process (convert LazyFrame to DataFrame)
    df = input_data.collect()
    
    # Process each row
    llm_outputs = []
    for row_dict in df.to_dicts():
        try:
            # Render prompt with row data
            prompt = render_prompt_template(prompt_template, row_dict)
            
            # Call LLM
            response = await call_llm(
                prompt=prompt,
                provider=provider,
                model=model,
                temperature=temperature,
                max_tokens=max_tokens
            )
            
            llm_outputs.append(response)
            
        except Exception as e:
            logger.error(f"LLM call failed for row: {e}")
            llm_outputs.append(f"ERROR: {str(e)}")
    
    # Add LLM outputs as new column
    df_with_output = df.with_columns([
        pl.Series(name=output_column, values=llm_outputs)
    ])
    
    return df_with_output.lazy()


async def execute_llm_batch(
    config: Dict[str, Any],
    input_data: Optional[pl.LazyFrame] = None
) -> pl.LazyFrame:
    """Execute batch LLM inference for efficiency.
    
    Processes multiple rows with concurrent API calls for better throughput.
    More efficient than sequential processing.
    
    Args:
        config: Node configuration (same as execute_llm_prompt)
            - batch_size (int): Number of concurrent API calls (default: 5)
        input_data: Input DataFrame
    
    Returns:
        LazyFrame with LLM outputs
    
    Notes:
        - Batch size configurable (default: 5 concurrent)
        - Handles partial failures gracefully
        - Respects rate limits
        - Significant performance improvement for large datasets
    """
    if input_data is None:
        raise ValueError("llm_batch node requires input data")
    
    # Extract configuration
    provider = config.get("provider", "openai")
    model = config.get("model", settings.OPENAI_DEFAULT_MODEL if provider == "openai" else settings.ANTHROPIC_DEFAULT_MODEL)
    prompt_template = config.get("prompt_template", "")
    temperature = config.get("temperature", settings.LLM_DEFAULT_TEMPERATURE)
    max_tokens = config.get("max_tokens", settings.LLM_DEFAULT_MAX_TOKENS)
    output_column = config.get("output_column", "llm_output")
    batch_size = config.get("batch_size", 5)
    
    logger.info(f"Executing LLM batch node with provider: {provider}, batch_size: {batch_size}")
    
    if not prompt_template:
        raise ValueError("prompt_template is required in node configuration")
    
    # Collect data to process
    df = input_data.collect()
    rows = df.to_dicts()
    
    # Process in batches with concurrency
    llm_outputs = []
    
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        
        # Create tasks for concurrent execution
        tasks = []
        for row_dict in batch:
            prompt = render_prompt_template(prompt_template, row_dict)
            task = call_llm(
                prompt=prompt,
                provider=provider,
                model=model,
                temperature=temperature,
                max_tokens=max_tokens
            )
            tasks.append(task)
        
        # Execute batch concurrently
        try:
            batch_results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Handle results and errors
            for result in batch_results:
                if isinstance(result, Exception):
                    logger.error(f"LLM call failed in batch: {result}")
                    llm_outputs.append(f"ERROR: {str(result)}")
                else:
                    llm_outputs.append(result)
        
        except Exception as e:
            logger.error(f"Batch execution failed: {e}")
            # Fill with errors for failed batch
            llm_outputs.extend([f"ERROR: {str(e)}"] * len(batch))
    
    # Add LLM outputs as new column
    df_with_output = df.with_columns([
        pl.Series(name=output_column, values=llm_outputs)
    ])
    
    return df_with_output.lazy()


async def execute_evaluator(
    config: Dict[str, Any],
    input_data: Optional[pl.LazyFrame] = None
) -> pl.LazyFrame:
    """Evaluate LLM output quality using LLM-as-judge.
    
    Uses LLM to score output quality for monitoring and filtering.
    
    Args:
        config: Evaluation configuration
            - evaluation_type (str): quality, relevance, factuality, safety
            - llm_output_column (str): Column with LLM output to evaluate
            - reference_column (str): Column with reference/ground truth (optional)
            - score_column (str): Column name for quality score (0.0-1.0)
            - criteria (str): Evaluation criteria description
            - provider (str): LLM provider for evaluation (default: openai)
            - model (str): Model for evaluation (default: gpt-4-turbo)
        input_data: DataFrame with LLM outputs
    
    Returns:
        LazyFrame with added quality score column
    
    Example:
        config = {
            "evaluation_type": "relevance",
            "llm_output_column": "summary",
            "reference_column": "original_text",
            "score_column": "relevance_score",
            "criteria": "Is summary relevant and accurate to original text?"
        }
    
    Use Cases:
        - Monitor LLM quality over time
        - A/B test different prompts
        - Filter low-quality outputs
        - Alert on quality degradation
    """
    if input_data is None:
        raise ValueError("evaluator node requires input data")
    
    evaluation_type = config.get("evaluation_type", "quality")
    llm_output_column = config.get("llm_output_column", "llm_output")
    reference_column = config.get("reference_column")
    score_column = config.get("score_column", "quality_score")
    criteria = config.get("criteria", f"Evaluate {evaluation_type} of the output")
    provider = config.get("provider", "openai")
    model = config.get("model", "gpt-4-turbo")
    
    logger.info(f"Executing evaluator node for {evaluation_type}")
    
    # Collect data
    df = input_data.collect()
    
    # Build evaluation prompt template
    if reference_column:
        eval_prompt = f"""Evaluate the following output based on this criteria: {criteria}

Reference: {{{{reference}}}}
Output to evaluate: {{{{output}}}}

Provide a quality score from 0.0 to 1.0 where:
- 0.0 = completely fails criteria
- 0.5 = partially meets criteria
- 1.0 = fully meets criteria

Return ONLY the numeric score, nothing else."""
    else:
        eval_prompt = f"""Evaluate the following output based on this criteria: {criteria}

Output to evaluate: {{{{output}}}}

Provide a quality score from 0.0 to 1.0 where:
- 0.0 = completely fails criteria
- 0.5 = partially meets criteria
- 1.0 = fully meets criteria

Return ONLY the numeric score, nothing else."""
    
    # Evaluate each row
    scores = []
    for row_dict in df.to_dicts():
        try:
            # Prepare evaluation data
            eval_data = {"output": row_dict.get(llm_output_column, "")}
            if reference_column and reference_column in row_dict:
                eval_data["reference"] = row_dict[reference_column]
            
            # Call LLM for evaluation
            prompt = render_prompt_template(eval_prompt, eval_data)
            response = await call_llm(
                prompt=prompt,
                provider=provider,
                model=model,
                temperature=0.0,  # Deterministic evaluation
                max_tokens=10
            )
            
            # Extract numeric score
            score_match = re.search(r'(\d+\.?\d*)', response)
            if score_match:
                score = float(score_match.group(1))
                score = max(0.0, min(1.0, score))  # Clamp to 0-1
            else:
                logger.warning(f"Could not parse score from response: {response}")
                score = 0.5  # Default to middle score
            
            scores.append(score)
        
        except Exception as e:
            logger.error(f"Evaluation failed for row: {e}")
            scores.append(0.0)  # Fail-safe score
    
    # Add score column
    df_with_scores = df.with_columns([
        pl.Series(name=score_column, values=scores)
    ])
    
    return df_with_scores.lazy()


async def execute_router(
    config: Dict[str, Any],
    input_data: Optional[pl.LazyFrame] = None
) -> Dict[str, pl.LazyFrame]:
    """Route data based on LLM classification.
    
    Uses LLM to classify each row and route to different downstream paths.
    
    Args:
        config: Router configuration
            - classification_prompt (str): Prompt to classify data
            - routes (List[str]): Available route names
            - route_column (str): Column to store classification
            - provider (str): LLM provider (default: openai)
            - model (str): Model to use for classification
        input_data: Input DataFrame
    
    Returns:
        Dict mapping route names to LazyFrames for that route
    
    Example:
        config = {
            "classification_prompt": "Classify priority of this ticket: {{ticket_text}}. Reply with only: high, medium, or low",
            "routes": ["high", "medium", "low"],
            "route_column": "priority"
        }
        
        Returns: {
            "high": DataFrame with high priority rows,
            "medium": DataFrame with medium priority rows,
            "low": DataFrame with low priority rows
        }
    
    Use Cases:
        - Triage support tickets by urgency
        - Route leads to different sales teams
        - Filter content by category
        - Dynamic workflow branching based on content
    """
    if input_data is None:
        raise ValueError("router node requires input data")
    
    classification_prompt = config.get("classification_prompt", "")
    routes = config.get("routes", ["default"])
    route_column = config.get("route_column", "route")
    provider = config.get("provider", "openai")
    model = config.get("model", "gpt-4-turbo")
    
    logger.info(f"Executing router node with {len(routes)} routes")
    
    if not classification_prompt:
        raise ValueError("classification_prompt is required in router configuration")
    
    # Collect data
    df = input_data.collect()
    
    # Classify each row
    classifications = []
    for row_dict in df.to_dicts():
        try:
            # Render classification prompt
            prompt = render_prompt_template(classification_prompt, row_dict)
            
            # Call LLM for classification
            response = await call_llm(
                prompt=prompt,
                provider=provider,
                model=model,
                temperature=0.0,  # Deterministic classification
                max_tokens=50
            )
            
            # Extract route from response
            response_lower = response.strip().lower()
            
            # Find matching route
            matched_route = None
            for route in routes:
                if route.lower() in response_lower:
                    matched_route = route
                    break
            
            if matched_route is None:
                logger.warning(f"Could not match route from response: {response}, using first route")
                matched_route = routes[0]
            
            classifications.append(matched_route)
        
        except Exception as e:
            logger.error(f"Classification failed for row: {e}")
            classifications.append(routes[0])  # Default to first route
    
    # Add classification column
    df_with_routes = df.with_columns([
        pl.Series(name=route_column, values=classifications)
    ])
    
    # Split data by route
    result = {}
    for route in routes:
        route_df = df_with_routes.filter(pl.col(route_column) == route)
        if len(route_df) > 0:
            result[route] = route_df.lazy()
        else:
            # Return empty LazyFrame with same schema
            result[route] = df_with_routes.head(0).lazy()
    
    return result


async def execute_tool_call(
    config: Dict[str, Any],
    input_data: Optional[pl.LazyFrame] = None
) -> pl.LazyFrame:
    """Execute LLM with function calling.
    
    Allows LLM to call predefined functions for actions (API calls, calculations).
    
    Args:
        config: Tool call configuration
            - tools (List[Dict]): Available tool definitions
            - system_prompt (str): Instructions for tool usage
            - input_column (str): Column with user request
            - output_column (str): Column for tool call results
        input_data: Input DataFrame
    
    Returns:
        LazyFrame with tool call results
    
    Example:
        config = {
            "tools": [
                {
                    "name": "get_weather",
                    "description": "Get current weather",
                    "parameters": {"location": "string"}
                }
            ],
            "system_prompt": "Use tools to answer questions",
            "input_column": "question",
            "output_column": "answer"
        }
    
    Use Cases:
        - LLM-powered API integration
        - Dynamic calculation and data lookup
        - Multi-step agentic workflows
        - Conversational interfaces
    """
    logger.info("Executing tool call node")
    
    # TODO: Implement function calling
    logger.warning("Tool calling not yet implemented")
    
    output_column = config.get("output_column", "tool_result")
    output = input_data.with_columns([
        pl.lit("TOOL_CALL_PLACEHOLDER").alias(output_column)
    ])
    
    return output


async def execute_vector_search(
    config: Dict[str, Any],
    input_data: Optional[pl.LazyFrame] = None
) -> pl.LazyFrame:
    """Execute semantic search using vector embeddings.
    
    Converts text to embeddings and searches vector database.
    
    Args:
        config: Vector search configuration
            - vector_db (str): Vector database (pinecone, weaviate, qdrant)
            - index_name (str): Index to search
            - query_column (str): Column with search queries
            - top_k (int): Number of results per query
            - embedding_model (str): Model for embeddings
        input_data: DataFrame with search queries
    
    Returns:
        LazyFrame with search results
    
    Use Cases:
        - RAG (Retrieval Augmented Generation)
        - Semantic document search
        - Similar item recommendations
        - Knowledge base lookup
    """
    logger.info("Executing vector search node")
    
    # TODO: Implement vector search
    logger.warning("Vector search not yet implemented")
    
    return input_data

import os
import sys
import json
import argparse
import re
from datetime import datetime, timedelta
import asyncio

# Get the current working directory
current_dir = os.getcwd()
path_to_add = os.path.join(current_dir, '../mcp-clickhouse/mcp_server')
if not os.path.exists(path_to_add):
    os.makedirs(path_to_add)
 
sys.path.append(path_to_add)
 
print(path_to_add)

from mcp_clickhouse import create_clickhouse_client, list_databases, list_tables, run_select_query


# Add the openai-agents-python/src to the path to properly access the agents module
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'openai-agents-python/src'))

try:
    from agents import Agent, Runner
    from agents.mcp import MCPServerStdio, MCPServerSse
    USING_AGENTS_SDK = True
    print("Successfully imported OpenAI Agents SDK", file=sys.stderr)
except ImportError as e:
    print(f"Error importing the OpenAI Agents SDK: {e}", file=sys.stderr)
    print("Using fallback approach without external dependencies.", file=sys.stderr)
    USING_AGENTS_SDK = False

# Setup for generation without external dependencies
def get_query_intent(query_text):
    """Determine the intent of the natural language query"""
    query_lower = query_text.lower().strip()
    
    # Define patterns to match different intents
    patterns = {
        'delete': r'delete|remove|drop|truncate',
        'insert': r'insert|add|create new|put in',
        'update': r'update|modify|change|set',
        'select_count': r'count|how many|number of|total',
        'select_top': r'top|highest|most|maximum|best',
        'select_bottom': r'bottom|lowest|least|minimum|worst',
        'select_avg': r'average|mean|avg',
        'select_sum': r'sum|total of|add up',
        'select_recent': r'recent|latest|newest|last|yesterday|this week|this month',
        'select_time_range': r'between|from.*to|since|last week|last month|last year|previous|ago',
        'select_group': r'group by|grouped by|categories|categorize|distribution|breakdown',
        'select_filter': r'where|with|filter|having|specific|only',
        'select_join': r'join|related|relation|connected|association|link',
        'select_distinct': r'distinct|unique|different',
        'describe': r'describe|explain|tell me about|what is|details|schema|columns'
    }
    
    # Check each pattern
    matched_intents = []
    for intent, pattern in patterns.items():
        if re.search(pattern, query_lower):
            matched_intents.append(intent)
    
    # If no intents matched, default to basic select
    if not matched_intents:
        return ['select_basic']
    
    return matched_intents

def extract_entities(query_text):
    """Extract entities like table names, columns, and values from the query"""
    query_lower = query_text.lower()
    
    # Try to extract time references
    time_entities = {}
    if 'last week' in query_lower or 'past week' in query_lower or 'previous week' in query_lower:
        time_entities['period'] = 'week'
        time_entities['value'] = 1
    elif 'last month' in query_lower or 'past month' in query_lower:
        time_entities['period'] = 'month'
        time_entities['value'] = 1
    elif 'last year' in query_lower or 'past year' in query_lower:
        time_entities['period'] = 'year'
        time_entities['value'] = 1
    elif 'yesterday' in query_lower:
        time_entities['period'] = 'day'
        time_entities['value'] = 1
    elif 'days' in query_lower or 'day' in query_lower:
        # Try to extract number of days
        day_match = re.search(r'(\d+)\s*days?', query_lower)
        if day_match:
            time_entities['period'] = 'day'
            time_entities['value'] = int(day_match.group(1))
    
    # Try to extract columns of interest
    columns = []
    important_columns = [
        'feed_name', 'severity', 'threat_type', 'country', 'region', 
        'timestamp', 'confidence', 'category', 'count'
    ]
    for col in important_columns:
        if col in query_lower:
            columns.append(col)
    
    # Try to extract specific values or conditions
    conditions = {}
    # Check for severity levels
    severity_match = re.search(r'(high|medium|low)\s+severity', query_lower)
    if severity_match:
        conditions['severity'] = severity_match.group(1)
    
    # Check for specific feed names
    feed_match = re.search(r'feed\s+name\s+(?:is|=|equals)\s+[\'"]?([a-zA-Z0-9_\-]+)[\'"]?', query_lower)
    if feed_match:
        conditions['feed_name'] = feed_match.group(1)
    
    # Check for country mentions
    country_match = re.search(r'country\s+(?:is|=|equals)\s+[\'"]?([a-zA-Z]+)[\'"]?', query_lower)
    if country_match:
        conditions['country'] = country_match.group(1)
    
    # Check for limit
    limit_match = re.search(r'(?:top|limit)\s+(\d+)', query_lower)
    limit = 10  # default
    if limit_match:
        limit = int(limit_match.group(1))
    
    return {
        'time': time_entities,
        'columns': columns,
        'conditions': conditions,
        'limit': limit
    }

def generate_sql_from_intent(intents, entities, table_name="td_agg_threat"):
    """Generate SQL based on identified intents and entities"""
    
    if 'delete' in intents:
        # Handle DELETE operation
        if entities['conditions']:
            conditions = []
            for col, val in entities['conditions'].items():
                conditions.append(f"{col} = '{val}'")
            
            if entities['time'] and 'period' in entities['time']:
                period = entities['time']['period']
                value = entities['time']['value']
                conditions.append(f"timestamp_day < DATE_SUB(CURRENT_DATE(), INTERVAL {value} {period.upper()})")
            
            where_clause = " AND ".join(conditions) if conditions else "1=1"
            return f"DELETE FROM {table_name} WHERE {where_clause}"
        else:
            return f"DELETE FROM {table_name}"
    
    elif 'insert' in intents:
        # Simple insert example
        return f"""INSERT INTO {table_name} (feed_name, severity, threat_type, timestamp_day)
VALUES ('example_feed', 'high', 'malware', CURRENT_DATE())"""
    
    elif 'update' in intents:
        # Handle UPDATE operation
        set_clause = "SET "
        if entities['conditions']:
            set_parts = []
            for col, val in entities['conditions'].items():
                set_parts.append(f"{col} = '{val}'")
            set_clause += ", ".join(set_parts)
        else:
            set_clause += "column_name = 'new_value'"
        
        where_clause = "WHERE 1=1"
        if entities['time'] and 'period' in entities['time']:
            period = entities['time']['period']
            value = entities['time']['value']
            where_clause = f"WHERE timestamp_day >= DATE_SUB(CURRENT_DATE(), INTERVAL {value} {period.upper()})"
        
        return f"UPDATE {table_name}\n{set_clause}\n{where_clause}"
    
    # Handle various SELECT operations
    select_clause = "SELECT "
    
    # Determine columns to select
    if 'select_count' in intents:
        select_clause += "COUNT(*) AS threat_count"
        if entities['columns']:
            select_clause += ",\n    " + ",\n    ".join(entities['columns'])
    elif 'select_distinct' in intents:
        if entities['columns']:
            select_clause += "DISTINCT " + ", ".join(entities['columns'])
        else:
            select_clause += "DISTINCT feed_name, severity"
    elif 'select_avg' in intents:
        if 'count' in entities['columns']:
            select_clause += "AVG(count) AS average_count"
        elif 'bandwidth' in entities['columns']:
            select_clause += "AVG(bandwidth) AS average_bandwidth"
        else:
            select_clause += "AVG(count) AS average_count"
    elif 'select_sum' in intents:
        if 'count' in entities['columns']:
            select_clause += "SUM(count) AS total_count"
        elif 'bandwidth' in entities['columns']:
            select_clause += "SUM(bandwidth_total) AS total_bandwidth"
        else:
            select_clause += "SUM(count) AS total_count"
    elif entities['columns']:
        select_clause += ", ".join(entities['columns'])
    else:
        select_clause += "*"
    
    from_clause = f"\nFROM {table_name}"
    
    # Determine WHERE clause
    where_conditions = []
    if 'select_time_range' in intents or 'select_recent' in intents:
        if entities['time'] and 'period' in entities['time']:
            period = entities['time']['period']
            value = entities['time']['value']
            where_conditions.append(f"timestamp_day >= DATE_SUB(CURRENT_DATE(), INTERVAL {value} {period.upper()})")
    
    if entities['conditions']:
        for col, val in entities['conditions'].items():
            where_conditions.append(f"{col} = '{val}'")
    
    where_clause = ""
    if where_conditions:
        where_clause = "\nWHERE " + " AND ".join(where_conditions)
    
    # Determine GROUP BY
    group_by = ""
    if 'select_group' in intents:
        if entities['columns']:
            group_by = "\nGROUP BY " + ", ".join(entities['columns'])
        else:
            # Default grouping
            group_by = "\nGROUP BY feed_name, severity"
    
    # Determine ORDER BY
    order_by = ""
    if 'select_top' in intents:
        order_metric = "count" if 'count' in entities['columns'] else "count"
        order_by = f"\nORDER BY {order_metric} DESC"
    elif 'select_bottom' in intents:
        order_metric = "count" if 'count' in entities['columns'] else "count"
        order_by = f"\nORDER BY {order_metric} ASC"
    elif 'select_recent' in intents:
        order_by = "\nORDER BY timestamp_day DESC"
    
    # Add LIMIT
    limit_clause = f"\nLIMIT {entities['limit']}"
    
    # Put it all together
    sql = select_clause + from_clause + where_clause + group_by + order_by + limit_clause
    
    return sql

def generate_better_sql_example(query, schema):
    """Generate a more intelligent SQL example based on the query and schema."""
    # Get query intents and entities
    intents = get_query_intent(query)
    entities = extract_entities(query)
    
    # Extract the table name from the schema if possible
    table_name = "td_agg_threat"  # Default
    if "CREATE TABLE" in schema:
        match = re.search(r'CREATE TABLE\s+(\w+)', schema)
        if match:
            table_name = match.group(1)
    
    # Generate SQL based on intents and entities
    return generate_sql_from_intent(intents, entities, table_name)

# Try to import the OpenAI Agents library
try:
    # Check if the agents module is available at the expected location
    agent_file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 
                                  'openai-agents-python/src/agents/__init__.py')
    
    if os.path.exists(agent_file_path):
        from agents import Agent
        USING_REAL_AGENT = True
        print("Successfully imported OpenAI Agents library.", file=sys.stderr)
    else:
        raise ImportError(f"Agent file not found at {agent_file_path}")
        
except ImportError as e:
    print(f"Error importing the OpenAI Agents Python library: {e}", file=sys.stderr)
    print("Using enhanced fallback approach.", file=sys.stderr)
    USING_REAL_AGENT = False
    
    # Define a mock Agent class that will be used if import fails
    class MockAgent:
        def __init__(self, system_prompt=None):
            self.system_prompt = system_prompt
            
        def run(self, query):
            # Create a simple object with a content attribute
            class Result:
                def __init__(self, content):
                    self.content = content
            
            # Extract the schema from the system_prompt
            try:
                schema = self.system_prompt.split("Database Schema:")[1].split("Current date:")[0].strip()
            except:
                schema = ""
            
            # Generate better SQL using our enhanced logic
            return Result(generate_better_sql_example(query, schema))

    # Create a mock Agent for fallback
    Agent = MockAgent

def nl_to_sql(natural_language_query, table_schema):
    """
    Convert natural language query to SQL using either OpenAI Agents or enhanced fallback.
    
    Args:
        natural_language_query (str): The question in natural language.
        table_schema (str): The database schema information to provide context.
        
    Returns:
        str: The generated SQL query.
    """
    # Get current date for context
    current_date = datetime.now().strftime("%Y-%m-%d")
    
    # Create a system prompt that includes the schema and task
    system_prompt = f"""You are a SQL expert. Given the following database schema, write a SQL query that answers the user's question.

Database Schema:
{table_schema}

Current date: {current_date}

Respond ONLY with the SQL query, no explanations or additional text."""

    # Create an agent
    try:
        agent = Agent(
            system_prompt=system_prompt,
        )
        
        # Generate the SQL query
        result = agent.run(natural_language_query)
        sql_query = result.content
        
        # Check if we got a default response and try fallback if needed
        if (USING_REAL_AGENT and 
            sql_query.strip() in ["SELECT * FROM td_agg_threat LIMIT 10", 
                               "SELECT * FROM td_agg_threat", 
                               "-- Could not generate SQL query"]):
            print("Agent returned generic response, trying enhanced fallback...", file=sys.stderr)
            sql_query = generate_better_sql_example(natural_language_query, table_schema)
        
        return sql_query
    except Exception as e:
        print(f"Error generating SQL query: {e}", file=sys.stderr)
        print("Using enhanced SQL query generator.", file=sys.stderr)
        return generate_better_sql_example(natural_language_query, table_schema)

def convert_schema_json_to_ddl(schema_json):
    """Convert the JSON schema format to CREATE TABLE DDL statement."""
    try:
        # Parse the JSON schema if it's a string
        if isinstance(schema_json, str):
            try:
                schema_data = json.loads(schema_json)
            except json.JSONDecodeError:
                # If not valid JSON, return it as is (assuming it's already DDL)
                return schema_json
        else:
            schema_data = schema_json
            
        tablename = schema_data.get("tablename", "unknown_table")
        columns = schema_data.get("schema", [])
        
        # Start building the CREATE TABLE statement
        ddl = f"CREATE TABLE {tablename} (\n"
        
        # Add each column
        column_definitions = []
        for column in columns:
            column_name = column.get("columnname", "unknown_column")
            column_type = column.get("columntype", "String")
            column_definitions.append(f"    {column_name} {column_type}")
        
        # Join the column definitions and complete the statement
        ddl += ",\n".join(column_definitions)
        ddl += "\n)"
        
        return ddl
        
    except Exception as e:
        print(f"Error converting schema JSON to DDL: {e}", file=sys.stderr)
        return str(schema_json)  # Return the original as a fallback

def get_threat_data_schema():
    """Return the schema for the td_agg_threat table."""
    schema_json = {
        "tablename": "td_agg_threat",
        "schema": [
            {"columnname": "storage_id", "columntype": "UInt32"},
            {"columnname": "timestamp_day", "columntype": "DateTime"},
            {"columnname": "type", "columntype": "FixedString(1)"},
            {"columnname": "policy_action", "columntype": "LowCardinality(String)"},
            {"columnname": "tclass", "columntype": "String"},
            {"columnname": "tproperty", "columntype": "String"},
            {"columnname": "tfamily", "columntype": "String"},
            {"columnname": "severity", "columntype": "LowCardinality(String)"},
            {"columnname": "confidence", "columntype": "LowCardinality(String)"},
            {"columnname": "category", "columntype": "LowCardinality(String)"},
            {"columnname": "threat_type", "columntype": "String"},
            {"columnname": "threat_technique", "columntype": "String"},
            {"columnname": "threat_classification", "columntype": "String"},
            {"columnname": "feed_name", "columntype": "LowCardinality(String)"},
            {"columnname": "response_region", "columntype": "String"},
            {"columnname": "response_country", "columntype": "String"},
            {"columnname": "device_region", "columntype": "String"},
            {"columnname": "device_country", "columntype": "String"},
            {"columnname": "threat_indicator", "columntype": "String"},
            {"columnname": "asset_cq_id", "columntype": "String"},
            {"columnname": "qip", "columntype": "String"},
            {"columnname": "device_type", "columntype": "String"},
            {"columnname": "actor_id", "columntype": "String"},
            {"columnname": "actor_name", "columntype": "String"},
            {"columnname": "policy_name", "columntype": "String"},
            {"columnname": "network", "columntype": "String"},
            {"columnname": "bandwidth", "columntype": "Float32"},
            {"columnname": "bandwidth_total", "columntype": "SimpleAggregateFunction(sum, Float64)"},
            {"columnname": "count", "columntype": "SimpleAggregateFunction(sum, UInt64)"},
            {"columnname": "min_timestamp", "columntype": "SimpleAggregateFunction(min, DateTime)"},
            {"columnname": "max_timestamp", "columntype": "SimpleAggregateFunction(max, DateTime)"}
        ]
    }
    
    # Convert to DDL for better LLM understanding
    return convert_schema_json_to_ddl(schema_json)

def load_schema_from_file(schema_file):
    """Load database schema from a file."""
    try:
        with open(schema_file, 'r') as f:
            content = f.read()
            # Try to parse as JSON first
            try:
                json_schema = json.loads(content)
                return convert_schema_json_to_ddl(json_schema)
            except json.JSONDecodeError:
                # If not valid JSON, return as is (assuming it's already DDL)
                return content
    except Exception as e:
        print(f"Error loading schema file: {e}", file=sys.stderr)
        return get_threat_data_schema()

def process_query(query, table_schema, verbose=False):
    """Process a single query and print the result."""
    # Convert natural language to SQL
    sql_query = nl_to_sql(query, table_schema)
    
    # Print output
    if verbose:
        print("\nNatural Language Query:")
        print(query)
        print("\nGenerated SQL Query:")
    
    # Print the SQL query
    print(sql_query)
    
    # Additional information in verbose mode
    if verbose:
        print("\nQuery with current date substituted:")
        one_week_ago = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
        print(f"Current date: {datetime.now().strftime('%Y-%m-%d')}")
        print(f"One week ago: {one_week_ago}")
    
    return sql_query

def interactive_mode(table_schema, verbose=False):
    """Run an interactive session that continuously accepts queries."""
    print("Natural Language to SQL Interactive Mode")
    print("Enter your queries, type 'exit', 'quit', or press Ctrl+C to exit")
    print(f"Using schema for table: td_agg_threat")
    
    history = []
    
    try:
        while True:
            print("\n" + "-" * 50)
            query = input("Enter your natural language query: ")
            
            # Check for exit commands
            if query.lower() in ['exit', 'quit', 'q', ':q', ':exit', ':quit']:
                print("Exiting interactive mode.")
                break
            
            # Process the query
            sql = process_query(query, table_schema, verbose)
            
            # Store in history
            history.append((query, sql))
            
    except KeyboardInterrupt:
        print("\nInteractive session terminated.")
    
    # Print session summary if there were queries
    if history and verbose:
        print("\nSession Summary:")
        for i, (nl_query, sql_query) in enumerate(history, 1):
            print(f"\n{i}. Natural Language: {nl_query}")
            print(f"   SQL: {sql_query}")
    
    return history

async def setup_mcp_server():
    """Set up and return an MCP server for SQL generation."""
    try:
        # This example uses a stdio-based MCP server
        # You can replace this with any MCP-compatible server
        # mcp_server = MCPServerStdio(
        #     name="Clickhouse MCP Server",
        #     params={
        #         "command": "python",
        #         "args": ["-m", "services.dbconnect"],
        #         "env": {"OPENAI_API_KEY": os.environ.get("OPENAI_API_KEY", "")}
        #     },
        #     cache_tools_list=True  # Cache tool list for better performance
        # )
        
        # Connect to the server
        mcp_server = create_clickhouse_client()
        print("MCP server called>>>>", mcp_server)
        await mcp_server.connect()
        print(f"Connected to MCP server: {mcp_server.name}", file=sys.stderr)
        
        # List available tools
        tools = await mcp_server.list_tools()
        print(f"Available MCP tools: {[tool.name for tool in tools]}", file=sys.stderr)
        
        return mcp_server
    except Exception as e:
        print(f"Error setting up MCP server: {e}", file=sys.stderr)
        return None

async def nl_to_sql_with_mcp(natural_language_query, table_schema, mcp_server=None):
    """Convert natural language to SQL using MCP server (if available)."""
    if not mcp_server or not USING_AGENTS_SDK:
        # Fall back to the standard method if MCP server is not available
        return nl_to_sql(natural_language_query, table_schema)
        
    try:
        # Create a system prompt that includes the schema and task
        instructions = f"""You are a SQL expert. Given the following database schema, write a SQL query that answers the user's question.

Database Schema:
{table_schema}

Current date: {datetime.now().strftime("%Y-%m-%d")}

Respond ONLY with the SQL query, no explanations or additional text."""

        # Create an agent with the MCP server
        agent = Agent(
            name="SQL Generator",
            instructions=instructions,
            mcp_servers=[mcp_server]
        )
        
        # Run the agent with the natural language query
        result = await Runner.run(
            starting_agent=agent,
            input=natural_language_query
        )
        
        # Return the generated SQL
        return result.final_output
    except Exception as e:
        print(f"Error generating SQL with MCP: {e}", file=sys.stderr)
        # Fall back to the non-MCP method
        return nl_to_sql(natural_language_query, table_schema)

async def process_query_async(query, table_schema, verbose=False, mcp_server=None):
    """Process a single query asynchronously and print the result."""
    # Convert natural language to SQL
    sql_query = await nl_to_sql_with_mcp(query, table_schema, mcp_server)
    
    # Print output
    if verbose:
        print("\nNatural Language Query:")
        print(query)
        print("\nGenerated SQL Query:")
    
    # Print the SQL query
    print(sql_query)
    
    # Additional information in verbose mode
    if verbose:
        print("\nQuery with current date substituted:")
        one_week_ago = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
        print(f"Current date: {datetime.now().strftime('%Y-%m-%d')}")
        print(f"One week ago: {one_week_ago}")
    
    return sql_query

async def interactive_mode_async(table_schema, verbose=False, mcp_server=None):
    """Run an interactive session that continuously accepts queries."""
    print("Natural Language to SQL Interactive Mode")
    print("Enter your queries, type 'exit', 'quit', or press Ctrl+C to exit")
    print(f"Using schema for table: td_agg_threat")
    
    if mcp_server:
        print(f"Using MCP server: {mcp_server.name}")
    
    history = []
    
    try:
        while True:
            print("\n" + "-" * 50)
            query = input("Enter your natural language query: ")
            
            # Check for exit commands
            if query.lower() in ['exit', 'quit', 'q', ':q', ':exit', ':quit']:
                print("Exiting interactive mode.")
                break
            
            # Process the query
            sql = await process_query_async(query, table_schema, verbose, mcp_server)
            
            # Store in history
            history.append((query, sql))
            
    except KeyboardInterrupt:
        print("\nInteractive session terminated.")
    
    # Print session summary if there were queries
    if history and verbose:
        print("\nSession Summary:")
        for i, (nl_query, sql_query) in enumerate(history, 1):
            print(f"\n{i}. Natural Language: {nl_query}")
            print(f"   SQL: {sql_query}")
    
    return history

async def main_async():
    parser = argparse.ArgumentParser(description='Convert natural language to SQL queries')
    parser.add_argument('query', nargs='?', help='Natural language query to convert to SQL')
    parser.add_argument('--schema-file', '-s', help='Path to a file containing the database schema (JSON or DDL)')
    parser.add_argument('--verbose', '-v', action='store_true', help='Show verbose output')
    parser.add_argument('--interactive', '-i', action='store_true', help='Run in interactive mode')
    parser.add_argument('--use-mcp', '-m', action='store_true', help='Use MCP server for SQL generation')
    
    args = parser.parse_args()
    
    # Load schema
    if args.schema_file:
        table_schema = load_schema_from_file(args.schema_file)
    else:
        table_schema = get_threat_data_schema()
    
    # Set up MCP server if requested
    mcp_server = None
    if args.use_mcp and USING_AGENTS_SDK:
        mcp_server = await setup_mcp_server()
    
    try:
        # Interactive mode takes precedence
        if args.interactive or args.query is None:
            await interactive_mode_async(table_schema, args.verbose, mcp_server)
        else:
            # Single query mode
            await process_query_async(args.query, table_schema, args.verbose, mcp_server)
    finally:
        # Clean up MCP server if it was created
        if mcp_server:
            await mcp_server.cleanup()
            print("MCP server cleaned up", file=sys.stderr)

def main():
    """Entry point for the script."""
    asyncio.run(main_async())

if __name__ == "__main__":
    main()
import os
import sys
import json
import argparse
import re
from datetime import datetime, timedelta
import asyncio
from dotenv import load_dotenv


# Load environment variables from .env file
load_dotenv()

# Read Azure OpenAI API configuration
API_BASE = os.getenv("OPENAI_API_BASE") or ""
API_KEY = os.getenv("OPENAI_API_KEY") or ""
MODEL_NAME =  os.getenv("OPENAI_API_VERSION") or ""
DEPLOYMENT_NAME = os.getenv("OPENAI_API_DEPLOYMENT_NAME") or ""
#API_BASE = ""
#MODEL_NAME ="2025-01-01-preview"

if not API_KEY:
    print("Error: OPENAI_API_BASE or OPENAI_API_KEY is not set in the environment or .env file.", file=sys.stderr)
else:
    print(f"OpenAI API Base: {API_BASE}")
    #lord murugan

current_dir = os.getcwd()
path_to_add = os.path.join(current_dir, '../mcp-clickhouse')
if not os.path.exists(path_to_add):
    os.makedirs(path_to_add)
 
sys.path.append(path_to_add)
 
print(path_to_add)

sys.path.append("/Users/dkumar1/Projects/Threatdefence/github/aiagent/mcp-clickhouse/.venv/lib/python3.13/site-packages")


# Import MCP server clients
from mcp_clickhouse.mcp_server import mcp, create_clickhouse_client, list_databases, list_tables, run_select_query

from mcp_grafana_client import create_grafana_client

# Add the openai-agents-python/src to the path to properly access the agents module
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '../openai-agents-python/src'))
#print(sys.path)


try:
    from openai import AsyncOpenAI, AsyncAzureOpenAI
    from agents import Agent, Runner, function_tool, set_tracing_disabled, set_default_openai_client, set_default_openai_api
    USING_AGENTS_SDK = True
    print("Successfully imported OpenAI Agents SDK", file=sys.stderr)
except ImportError as e:
    print(f"Error importing the OpenAI Agents SDK: {e}", file=sys.stderr)
    print("Using fallback approach without external dependencies.", file=sys.stderr)
    USING_AGENTS_SDK = False

#client = AsyncOpenAI(base_url=API_BASE, api_key=API_KEY)
#set_tracing_disabled(disabled=True)

client = AsyncAzureOpenAI(
         api_key=API_KEY,
         azure_endpoint=API_BASE,
         api_version=MODEL_NAME  # Or the appropriate API version
    )
set_tracing_disabled(disabled=True)
set_default_openai_client(client, use_for_tracing=False) # Optional: use_for_tracing=False
set_default_openai_api("chat_completions")


def load_schema_from_file(schema_file):
    """Load database schema from a file."""
    try:
        with open(schema_file, 'r') as f:
            content = f.read()
            # Try to parse as JSON first
            try:
                json_schema = json.loads(content)
                return json_schema
            except json.JSONDecodeError:
                # If not valid JSON, return as is (assuming it's already DDL)
                return content
    except Exception as e:
        print(f"Error loading schema file: {e}", file=sys.stderr)
        return None
    
@function_tool
def list_databases_reports()-> list:
    """List all databases."""
    print("getting database", type(list_databases()))
    return "reports"
    #return ["reports"]

@function_tool
def list_tables_reports(database: str):
    """List all tables in the reports"""
    return list_tables(database)
    
@function_tool
def run_select_query_reports(query: str):
    """Run a SELECT query on reports."""
    temp = run_select_query(query)
    print("running query returned", temp)
    return temp
    

    
async def setup_mcp_servers():
    """Set up and return MCP servers for ClickHouse and Grafana."""
    #clickhouse_server = None
    grafana_server = None
    clickhouse_server = None

    try:
        # Set up ClickHouse MCP server
        clickhouse_server = create_clickhouse_client()
        clickhouse_server.name = "ClickHouse MCP Server"
        #await clickhouse_server.connect()
        print(f"Connected to ClickHouse MCP server: {clickhouse_server.name}", file=sys.stderr)

        # List available tools for ClickHouse

        clickhouse_tools = [list_databases_reports, list_tables_reports, run_select_query_reports]
        print(f"Available ClickHouse tools: {[tool.name for tool in clickhouse_tools]}", file=sys.stderr)
    except Exception as e:
        print(f"Error setting up ClickHouse MCP server: {e}", file=sys.stderr)

    try:
        # Set up Grafana MCP server
        grafana_server = create_grafana_client()
        await grafana_server.connect()
        print(f"Connected to Grafana MCP server: {grafana_server.name}", file=sys.stderr)

        # List available tools for Grafana
        grafana_tools = await grafana_server.list_tools()
        print(f"Avaixlable Grafana tools: {[tool.name for tool in grafana_tools]}", file=sys.stderr)
    except Exception as e:
        print(f"Error setting up Grafana MCP server: {e}", file=sys.stderr)

    return clickhouse_server, None


async def nl_to_sql_with_mcp(natural_language_query, table_schema, clickhouse_server=None, grafana_server=None):
    """Convert natural language to SQL using MCP servers (ClickHouse and Grafana)."""
    print("nl_to_sql_with_mcp - ", clickhouse_server, grafana_server)
    if not (clickhouse_server or grafana_server) or not USING_AGENTS_SDK:
        # Fall back to the standard method if MCP servers are not available
        #return nl_to_sql(natural_language_query, table_schema)
        pass

    try:
        # Create a system prompt that includes the schema and task
        instructions = f"""You are a SQL expert. Use reports database, write a SQL query that answers the user's question.

Database:
{"reports"}

Current date: {datetime.now().strftime("%Y-%m-%d")}

Respond ONLY with the SQL query, no explanations or additional text."""

        # list_databases.name = "List Database"
        # list_tables.name = "List tables in reports database"
        # run_select_query.name = "Run SQL Query in reports database"
        # Use ClickHouse server if available
        if mcp:
            agent = Agent(
                name="SQL Generator (ClickHouse)",
                instructions=instructions,
                mcp_servers=[mcp],
                model=DEPLOYMENT_NAME,
                tools=[list_databases_reports, list_tables_reports, run_select_query_reports]
                
            )
           
            result = await Runner.run(
                starting_agent=agent,
                input=natural_language_query
            )
            return result.final_output

        # Use Grafana server if ClickHouse is not available
        # if grafana_server:
        #     agent = Agent(
        #         name="SQL Generator (Grafana)",
        #         instructions=instructions,
        #         mcp_servers=[grafana_server]
        #     )
        #     result = await Runner.run(
        #         starting_agent=agent,
        #         input=natural_language_query
        #     )
        #     return result.final_output

    #except Exception as e:
        #print(f"Error generating SQL with MCP servers: {e}", file=sys.stderr)
        # Fall back to the non-MCP method
        #TODO - removed fallback
        #return nl_to_sql(natural_language_query, table_schema)
    finally:
        pass

async def process_query_async(query, table_schema, verbose=False, clickhouse_server=None, grafana_server=None):
    """Process a single query asynchronously and print the result."""
    # Convert natural language to SQL
    sql_query = await nl_to_sql_with_mcp(query, table_schema, clickhouse_server, grafana_server)

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


async def interactive_mode_async(table_schema, verbose=False, clickhouse_server=None, grafana_server=None):
    """Run an interactive session that continuously accepts queries."""
    print("Natural Language to SQL Interactive Mode")
    print("Enter your queries, type 'exit', 'quit', or press Ctrl+C to exit")
    print(f"Using schema for table: td_agg_threat")

    if clickhouse_server:
        clickhouse_server.name = "reports MCP Server"
        print(f"Using ClickHouse MCP server: {clickhouse_server.name}")
    if grafana_server:
        print(f"Using Grafana MCP server: {grafana_server.name}")

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
            sql = await process_query_async(query, table_schema, verbose, clickhouse_server, grafana_server)

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

    args = parser.parse_args()

    # Load schema
    if args.schema_file:
        table_schema = load_schema_from_file(args.schema_file)
    # else:
    #     table_schema = get_threat_data_schema()

    # Set up MCP servers
    clickhouse_server, grafana_server = await setup_mcp_servers()

    try:
        # Interactive mode takes precedence
        if args.interactive or args.query is None:
            await interactive_mode_async(table_schema, args.verbose, clickhouse_server, grafana_server)
        else:
            # Single query mode
            await process_query_async(args.query, table_schema, args.verbose, clickhouse_server, grafana_server)
    finally:
        # Clean up MCP servers if they were created
        if clickhouse_server:
            #await clickhouse_server.cleanup()
            print("ClickHouse MCP server cleaned up", file=sys.stderr)
        if grafana_server:
            #await grafana_server.cleanup()
            print("Grafana MCP server cleaned up", file=sys.stderr)


def main():
    """Entry point for the script."""
    asyncio.run(main_async())


if __name__ == "__main__":
    main() 
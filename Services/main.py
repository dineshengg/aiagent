import os
import sys
import json
import argparse
import re
from datetime import datetime, timedelta
import asyncio

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
#from Services.functools import listdatabases, listtables, runselectquery
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

sys.path.append("../mcp-clickhouse/.venv/lib/python3.13/site-packages")
# Add the openai-agents-python/src to the path to properly access the agents module
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '../openai-agents-python/src'))
sys.path.append("/Users/dkumar1/Projects/Threatdefence/github/aiagent/mcp-clickhouse/.venv/lib/python3.13/site-packages")
current_dir = os.getcwd()
path_to_add = os.path.join(current_dir, '../mcp-clickhouse')
if not os.path.exists(path_to_add):
    os.makedirs(path_to_add)
 
sys.path.append(path_to_add)

from agents import function_tool
import mcp_clickhouse.mcp_server as mcp_my_app
# Import MCP server clients
from mcp_clickhouse.mcp_server import list_databases, list_tables, run_select_query
# Import MCP server clients
from mcp_clickhouse.mcp_server import mcp, create_clickhouse_client

from mcp_grafana_client import create_grafana_client, create_dashboard
from typing import Any

demodashboard = 1

# Add the openai-agents-python/src to the path to properly access the agents module
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '../openai-agents-python/src'))
#print(sys.path)

@function_tool
async def list_databases()-> list:
    """List database."""
    #print("getting database", type(list_databases()))
    print("getting database-1")
    return ["reports"]
    #return ["reports"]

@function_tool
async def list_tables(database: str) -> list[dict[str, Any]]:
    """List all tables."""
    print("list tables -2", database)
    # query = "SELECT COLUMN_NAME AS name, DATA_TYPE AS type FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'reports'"
    # result = mcp_my_app.runselectquery(query)
    # print("queries result", query)
    return mcp_my_app.list_tables(database)
    
@function_tool
async def run_select_query(query: str) -> str:
    """Run a SELECT query."""
    temp = mcp_my_app.run_select_query(query)
    print("running query returned", temp)
    return temp

try:
    from openai import AsyncOpenAI, AsyncAzureOpenAI
    from agents import Agent, Runner, set_tracing_disabled, set_default_openai_client, set_default_openai_api
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

        clickhouse_tools = [list_databases, list_tables, run_select_query]
        print(f"Available ClickHouse tools: {[tool.name for tool in clickhouse_tools]}", file=sys.stderr)
    except Exception as e:
        print(f"Error setting up ClickHouse MCP server: {e}", file=sys.stderr)

    try:
        # Set up Grafana MCP server
        grafana_server = create_grafana_client("", "", 30, False)
        #await grafana_server.connect()
        #print(f"Connected to Grafana MCP server: {grafana_server.name}", file=sys.stderr)
        print("Grafana server url: ", grafana_server.server_url)
        print("Grafana server api key: ", grafana_server.api_key)

        # List available tools for Grafana
        #grafana_tools = await grafana_server.list_tools()
        #print(f"Avaixlable Grafana tools: {[tool.name for tool in grafana_tools]}", file=sys.stderr)
    except Exception as e:
        print(f"Error setting up Grafana MCP server: {e}", file=sys.stderr)

    return clickhouse_server, grafana_server


async def nl_to_sql_with_mcp(natural_language_query, table_schema, clickhouse_server=None, grafana_server=None):
    """Convert natural language to SQL using MCP servers (ClickHouse and Grafana)."""
    print("nl_to_sql_with_mcp - ", clickhouse_server, grafana_server)
    if not (clickhouse_server or grafana_server) or not USING_AGENTS_SDK:
        # Fall back to the standard method if MCP servers are not available
        #return nl_to_sql(natural_language_query, table_schema)
        pass

    try:
        # Create a system prompt that includes the schema and task
        # Example schema fetched from MCP protocol
        database_name = "reports"
        #table_name = "example_table"
        column_list = ["timestamp", "metric_value", "category"]

        # Generate the prompt
        # prompt = f"""You are a SQL expert. Given the following database schema, write a SQL query that answers the user's question.

        # query tool list database use reports in it and then query the tool List all tables to get the list of table and then execute Run a SELECT query to get the table schema don't use any generic table or schemas

        # Current date: {datetime.now().strftime("%Y-%m-%d")}.
        # Please include the timestamp column if present in table and provide SELECT query as it needs to be displayed in grafana dashbaord

        # Respond ONLY with the SQL query, no explanations or additional text."""

        prompt = f"""You are a SQL expert. Write an SQL query that retrieves data for a time-series graph to be displayed in a Grafana dashboard.
        The query should:
        1. Include a `timestamp` column for the x-axis of the time-series graph.
        2. Fetch relevant metrics or values for the y-axis
        3. Use the database and table schema provided below.
        4. Limit the results to 1000 rows for performance.
        5. Use the tool list_databases, list_tables and run_select_query to get the sql query.
        6. Do not rely on your own knowledge.
        7. Give the sql query in one line no line break.
        8. Wait for the tool result if not throw error
        

        Respond ONLY with the SQL query. Do not include explanations or additional text."""

        #- Table: {table_name}
        #Database and Table Schema:
        #- Database: {database_name}
        

        #print(prompt)   

        # list_databases.name = "List Database"
        # list_tables.name = "List tables in reports database"
        # run_select_query.name = "Run SQL Query in reports database"
        # Use ClickHouse server if available
        if mcp:
            agent = Agent(
                name="SQL Generator (ClickHouse)",
                instructions=prompt,
                #mcp_servers=[mcp],
                model=DEPLOYMENT_NAME,
                tools=[list_databases, list_tables, run_select_query]
                
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

async def process_grafana_asynch(grafana_server, sql_query, json_content):
    print("process_grafana_asynch")
    global demodashboard
    demodashboard +=1
    dashbooard_title = f"B1TD Dashboard {demodashboard}"
    processed_json_content = generate_grafana_json(json_content, dashbooard_title, sql_query)
    print("generate_grafana_json")
    dashboard = create_dashboard(grafana_server, processed_json_content)
    print("create_dashboard")
    if dashboard:
        #print(f"Dashboard created successfully: {da    shboard.title} (UID: {dashboard.uid})")
        print(f"Dashboard created successfully! UID: {dashboard.uid}, URL: {dashboard.url}")
        print(f"Grafana URL: {grafana_server.server_url}{dashboard.url}")
        return f"http://localhost:3000{dashboard.url}"  
    else:
        pass
        
def generate_grafana_json(json_content: str, dashboard_title: str, sql_query: str) -> str:
    """Generate a Grafana JSON dashboard."""
    try:
        with open(json_content, 'r') as file:
            json_content = file.read()
 
        #print("Template content: ", json_content)
 
        replacements = {
            "{{TITLE}}": dashboard_title,
            "{{SQL_QUERY}}": sql_query
        }
 
        # Process the replacements directly on the content string
        #print("Template content: ", json_content)
        processed_content = json_content
        for placeholder, value in replacements.items():
            processed_content = processed_content.replace(placeholder, str(value))
        
        # Validate the result is valid JSON
        try:
            json.loads(processed_content)
        except json.JSONDecodeError as e:
            raise ValueError(f"Template processing resulted in invalid JSON: {str(e)}")
 
        #print("Processed content: ", processed_content)
 
        return processed_content
        
    except FileNotFoundError:
        print(f"Error: Dashboard file not found: {json_content}")
        sys.exit(1)
        
    except Exception as e:
        print(f"Error creating dashboard: {str(e)}")
        sys.exit(1)


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
        #Interactive mode takes precedence
        if args.interactive or args.query is None:
            await interactive_mode_async(table_schema, args.verbose, clickhouse_server, grafana_server)
        else:
            #Single query mode
            sqlquery = await process_query_async(args.query, table_schema, args.verbose, clickhouse_server, grafana_server)
        #sqlquery = "SELECT timestamp, storage_id FROM td_agg_rpz_combined limit 1000"
            jsonfile = "json-templates/createdashboarddemo.json"
            dashboard_url = await process_grafana_asynch(grafana_server, sqlquery, jsonfile)
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

###########
##Fast API routing for web page and getting natural language query from users

from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

app = FastAPI()

# Allow CORS for testing purposes
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Load schema (you can replace this with your actual schema file path)
#SCHEMA_FILE = "path/to/your/schema.json"
#table_schema = load_schema_from_file(SCHEMA_FILE)

# Set up MCP servers (ClickHouse and Grafana)
clickhouse_server, grafana_server = None, None


@app.on_event("startup")
async def startup_event():
    """Initialize MCP servers on startup."""
    global clickhouse_server, grafana_server
    clickhouse_server, grafana_server = await setup_mcp_servers()


@app.on_event("shutdown")
async def shutdown_event():
    """Clean up MCP servers on shutdown."""
    global clickhouse_server, grafana_server
    if clickhouse_server:
        print("Cleaning up ClickHouse MCP server...")
        # await clickhouse_server.cleanup()
    if grafana_server:
        print("Cleaning up Grafana MCP server...")
        # await grafana_server.cleanup()


@app.get("/", response_class=HTMLResponse)
async def index():
    """Serve the index page with a form to input the natural language query."""
    try:
        with open("index.html", "r") as file:
            html_content = file.read()
        return HTMLResponse(content=html_content)
    except FileNotFoundError:
        return HTMLResponse(content="<h1>Index page not found</h1>", status_code=404)

# Define the request body schema
class PromptRequest(BaseModel):
    prompt: str

@app.post("/prompt")
async def prompt(request: PromptRequest):
    """Process the user's natural language query and return the generated SQL."""
    global clickhouse_server, grafana_server
    table_schema = ""
    try:
        # Extract the prompt from the request
        prompt = request.prompt
        print(prompt)
        # Process the query using the AI agent
        jsonfile = "json-templates/createdashboarddemo.json"
        sql_query = await process_query_async(prompt, table_schema, verbose=False, clickhouse_server=clickhouse_server, grafana_server=grafana_server)
        dashboard_url = await process_grafana_asynch(grafana_server, sql_query, jsonfile)
        return {"status": "success", "query": prompt, "dashboard": dashboard_url}
    except Exception as e:
        return {"status": "error", "message": str(e)}
import json
from mcp_clickhouse import create_clickhouse_client, list_databases, list_tables, run_select_query

def getTableSchemaFromMCP(database, table_name):
    client = create_clickhouse_client()
    schema_query = f"SELECT COLUMN_NAME AS name, DATA_TYPE AS type FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '{database}' AND TABLE_NAME = '{table_name}';"
    schema_result = client.command(schema_query)

    actual_result = []
    for i in schema_result:
        if len(i.split("\n")) > 0:
            for j in i.split("\n"):
                actual_result.append(j)
        else:
            actual_result.append(i)
    
    count = 0
    parsed_result = []
    columnname = ""
    columntype = ""
    for i in actual_result:
        count +=1
        if count%2 != 0:
            columnname = i
        else:
            columntype = i
            parsed_result.append({"columnname":columnname, "columntype":columntype})

    json_result = []
    
    json_result.append({"tablename": table_name, "schema": parsed_result})
    output_json = {"tables": json_result}
    return output_json



if __name__ == "__main__":
    table_name = "td_agg_threat"
    result = getTableSchemaFromMCP(table_name)
    print(result)

import json

def get_table_schema_json(tablename, json_file_path="table_metadata.json"):
    """
    Parse the table schema from the JSON file and return the columnname and columntype for the given tablename.

    Args:
        tablename (str): The name of the table to retrieve the schema for.
        json_file_path (str): Path to the JSON file containing the table metadata.

    Returns:
        dict: A JSON payload containing the columnname and columntype for the given tablename.
    """
    try:
        # Load the JSON file
        with open(json_file_path, "r") as file:
            data = json.load(file)

        # Find the table with the given tablename
        for table in data.get("tables", []):
            if table.get("tablename") == tablename:
                # Return the schema as a JSON payload
                return {
                    "tablename": tablename,
                    "schema": table.get("schema", [])
                }

        # If the tablename is not found, return an error message
        return {"error": f"Table '{tablename}' not found in the schema."}

    except FileNotFoundError:
        return {"error": f"File '{json_file_path}' not found."}
    except json.JSONDecodeError:
        return {"error": "Invalid JSON format in the file."}

if __name__ == "__main__":
    table_name_from_json = "td_agg_threat"
    schema = get_table_schema_json(table_name_from_json)
    print(json.dumps(schema, indent=4))

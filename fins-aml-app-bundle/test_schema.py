import os
from databricks import sql

from backend import config

# Test the actual schema
def test_schema():
    warehouse_id = config.DATABRICKS_WAREHOUSE_ID
    creds = config.get_sql_credentials_provider()

    if not creds and not config.DATABRICKS_TOKEN:
        print("No credentials available for testing")
        return

    connect_kwargs = dict(
        server_hostname=config.DATABRICKS_HOSTNAME,
        http_path=f'/sql/1.0/warehouses/{warehouse_id}',
    )
    if creds:
        connect_kwargs["credentials_provider"] = creds
    else:
        connect_kwargs["access_token"] = config.DATABRICKS_TOKEN

    try:
        with sql.connect(**connect_kwargs) as connection:

            cursor = connection.cursor()

            # Test alerts table structure
            print("=== ALERTS TABLE ===")
            cursor.execute(f"DESCRIBE {config.table('alerts')}")
            alerts_schema = cursor.fetchall()
            for col in alerts_schema:
                print(f"{col[0]}: {col[1]}")

            # Sample a few rows
            print("\n=== ALERTS SAMPLE ===")
            cursor.execute(f"SELECT * FROM {config.table('alerts')} LIMIT 3")
            alerts_sample = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            print("Columns:", columns)

            print("\n=== CASES TABLE ===")
            cursor.execute(f"DESCRIBE {config.table('cases')}")
            cases_schema = cursor.fetchall()
            for col in cases_schema:
                print(f"{col[0]}: {col[1]}")

            print("\n=== GRAPH NODES TABLE ===")
            cursor.execute(f"DESCRIBE {config.table('graph_nodes')}")
            nodes_schema = cursor.fetchall()
            for col in nodes_schema:
                print(f"{col[0]}: {col[1]}")

            print("\n=== GRAPH EDGES TABLE ===")
            cursor.execute(f"DESCRIBE {config.table('graph_edges')}")
            edges_schema = cursor.fetchall()
            for col in edges_schema:
                print(f"{col[0]}: {col[1]}")

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    test_schema()

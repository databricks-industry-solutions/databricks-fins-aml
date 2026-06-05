#!/usr/bin/env python3
"""
Provision the Graph Explorer's semantic-search backing: a Vector Search Delta
Sync index over graph nodes. The app (backend/api/semantic_graph.py) queries
`<catalog>.<schema>.graph_node_embeddings_index` on the `fins-aml-vs-endpoint`
endpoint; without this the Graph Explorer search returns HTTP 500
("graph_node_embeddings_index does not exist").

Steps (idempotent):
  1. Build `<catalog>.<schema>.graph_node_embeddings` from `graph_nodes` — adds a
     unique composite primary key `node_key` (node_type + '_' + node_id; node_id
     alone is NOT unique across node types) and a per-type `text_description`,
     with Change Data Feed enabled (required for Delta Sync).
  2. Ensure the Vector Search endpoint (`fins-aml-vs-endpoint`).
  3. Ensure the Delta Sync index over `text_description` via the managed
     embedding model, then sync.

Dry-run by default; pass --apply to create resources.
"""

import argparse
import sys
import time

from pyspark.sql import SparkSession

VS_ENDPOINT_DEFAULT = "fins-aml-vs-endpoint"
EMBEDDING_MODEL_DEFAULT = "databricks-gte-large-en"
ENDPOINT_TIMEOUT_SEC = 30 * 60
INDEX_TIMEOUT_SEC = 45 * 60
POLL_SEC = 20


def log(msg: str) -> None:
    print(msg, flush=True)


# Per-node-type text built to match the existing reference deployment, derived
# from node_label, risk_*, and specific keys in the properties JSON.
TEXT_DESCRIPTION_SQL = """
CASE node_type
  WHEN 'customer' THEN concat_ws(' | ',
    concat('Customer: ', node_label),
    concat('Type: ', get_json_object(properties, '$.customer_type')),
    concat('Occupation: ', get_json_object(properties, '$.occupation')),
    concat('Location: ', concat_ws(' ',
      get_json_object(properties, '$.address_city'),
      get_json_object(properties, '$.address_state'),
      get_json_object(properties, '$.address_country'))),
    concat('Risk: ', risk_category, ' score ', cast(risk_score as string)),
    concat('PEP: ', get_json_object(properties, '$.pep_flag')),
    concat('KYC: ', get_json_object(properties, '$.kyc_status')))
  WHEN 'account' THEN concat_ws(' | ',
    concat('Bank Account: ', node_label),
    concat('Type: ', get_json_object(properties, '$.account_type')))
  WHEN 'alert' THEN concat_ws(' | ',
    concat('AML Alert: ', node_label),
    concat('Risk score: ', cast(risk_score as string)),
    concat('Risk category: ', risk_category))
  WHEN 'counterparty' THEN concat_ws(' | ',
    concat('Counterparty: ', node_label),
    concat('Risk: ', risk_category, ' score ', cast(risk_score as string)))
  WHEN 'watchlist' THEN concat_ws(' | ',
    concat('Watchlist Match: ', node_label),
    concat('Risk: ', get_json_object(properties, '$.list_type')))
  ELSE concat(node_type, ': ', node_label)
END
"""


def build_source_table(spark: SparkSession, catalog: str, schema: str, apply: bool) -> str:
    source = f"{catalog}.{schema}.graph_node_embeddings"
    graph_nodes = f"{catalog}.{schema}.graph_nodes"
    ddl = f"""
    CREATE OR REPLACE TABLE {source}
    TBLPROPERTIES (delta.enableChangeDataFeed = true)
    AS SELECT
        concat(node_type, '_', cast(node_id as string)) AS node_key,
        cast(node_id as string)                          AS node_id,
        node_type,
        node_label,
        risk_score,
        risk_category,
        properties,
        {TEXT_DESCRIPTION_SQL}                           AS text_description
    FROM {graph_nodes}
    """
    log(f"  [build] source table {source} (composite PK node_key, CDF on)")
    if not apply:
        log("         (dry-run: skipping CREATE TABLE)")
        return source
    spark.sql(ddl)
    n = spark.sql(f"SELECT count(*) c FROM {source}").collect()[0]["c"]
    log(f"         built {n} rows")
    return source


def ensure_endpoint(vsc, name: str, apply: bool) -> None:
    try:
        ep = vsc.get_endpoint(name)
        state = (ep.get("endpoint_status") or {}).get("state", "?")
        log(f"  [skip] VS endpoint '{name}' exists (state={state})")
        return
    except Exception:
        pass
    log(f"  [create] VS endpoint '{name}' (STANDARD)")
    if not apply:
        return
    vsc.create_endpoint(name=name, endpoint_type="STANDARD")
    deadline = time.time() + ENDPOINT_TIMEOUT_SEC
    while time.time() < deadline:
        state = (vsc.get_endpoint(name).get("endpoint_status") or {}).get("state", "")
        if state == "ONLINE":
            log(f"         endpoint ONLINE")
            return
        time.sleep(POLL_SEC)
    raise TimeoutError(f"VS endpoint '{name}' did not come ONLINE within timeout")


def ensure_index(vsc, endpoint: str, index_name: str, source_table: str,
                 embedding_model: str, apply: bool) -> None:
    try:
        idx = vsc.get_index(endpoint, index_name)
        log(f"  [skip] index '{index_name}' exists — triggering sync")
        if apply:
            idx.sync()
        return
    except Exception:
        pass
    log(f"  [create] Delta Sync index '{index_name}' (PK=node_key, embed=text_description via {embedding_model}, TRIGGERED)")
    if not apply:
        return
    vsc.create_delta_sync_index(
        endpoint_name=endpoint,
        index_name=index_name,
        source_table_name=source_table,
        pipeline_type="TRIGGERED",
        primary_key="node_key",
        embedding_source_column="text_description",
        embedding_model_endpoint_name=embedding_model,
    )
    # Wait for the index to finish its initial provisioning + sync.
    idx = vsc.get_index(endpoint, index_name)
    deadline = time.time() + INDEX_TIMEOUT_SEC
    while time.time() < deadline:
        try:
            status = idx.describe().get("status", {})
            if status.get("ready"):
                log(f"         index ready ({status.get('indexed_row_count', '?')} rows indexed)")
                return
        except Exception:
            pass
        time.sleep(POLL_SEC)
    raise TimeoutError(f"index '{index_name}' did not become ready within timeout")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--vs-endpoint", default=VS_ENDPOINT_DEFAULT)
    parser.add_argument("--embedding-model", default=EMBEDDING_MODEL_DEFAULT)
    parser.add_argument("--apply", action="store_true", help="Create resources (default: dry-run)")
    args = parser.parse_args()

    if not args.apply:
        log("=== DRY RUN === (pass --apply to create resources)\n")

    spark = SparkSession.builder.getOrCreate()
    index_name = f"{args.catalog}.{args.schema}.graph_node_embeddings_index"

    log("Step 1: Embeddings source table")
    source = build_source_table(spark, args.catalog, args.schema, args.apply)

    from databricks.vector_search.client import VectorSearchClient
    vsc = VectorSearchClient(disable_notice=True)

    log("\nStep 2: Vector Search endpoint")
    ensure_endpoint(vsc, args.vs_endpoint, args.apply)

    log("\nStep 3: Delta Sync index")
    ensure_index(vsc, args.vs_endpoint, index_name, source, args.embedding_model, args.apply)

    log("\nDone.")


if __name__ == "__main__":
    # Not sys.exit(main()) — see provision_agents.py: SystemExit is reported as a
    # task failure by the serverless runner even on success.
    main()

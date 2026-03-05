# Databricks notebook source
# MAGIC %md
# MAGIC ### DQX Structured Streaming Test
# MAGIC
# MAGIC DQX is installed at the cluster level. This notebook validates the structured streaming path.

# COMMAND ----------

# COMMAND ----------

# MAGIC %md
# MAGIC ### Define Data Quality Checks
# MAGIC
# MAGIC This cell defines a set of data quality checks using YAML syntax. The checks are designed to validate key columns in the open-source `tips` dataset, such as `total_bill`, `tip`, `day`, and `size`. Each check specifies a function, its arguments, a name, and a criticality level (`error` or `warn`). These checks will be used by the `DQEngine` to enforce data quality rules on the streaming data.

# COMMAND ----------

# Define Data Quality checks
import yaml


checks = yaml.safe_load("""
- check:
    function: is_not_null
    arguments:
      column: total_bill
  name: total_bill_is_null
  criticality: error
- check:
    function: is_not_null
    arguments:
      column: tip
  name: tip_is_null
  criticality: error

- check:
    function: is_in_range
    arguments:
      column: total_bill
      min_limit: 1
      max_limit: 100
  name: total_bill_not_in_range
  criticality: warn

- check:
    function: is_not_less_than
    arguments:
      column: tip
      limit: 0
  name: tip_not_less_than_zero
  criticality: warn

- check:
    function: is_in_list
    arguments:
      column: day
      allowed:
        - Sun
        - Sat
        - Thur
        - Fri
  name: day_is_not_valid
  criticality: warn

- check:
    function: is_in_range
    arguments:
      column: size
      min_limit: 1
      max_limit: 6
  name: party_size_not_in_range
  criticality: warn
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Inputs
# MAGIC Checkpoint and table locations are set via widgets.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <h3>Checkpoint and Table Location Options</h3>
# MAGIC <ul>
# MAGIC   <li>
# MAGIC     <b>Checkpoint Location can be specified as:</b>
# MAGIC     <ul>
# MAGIC       <li>Local path (e.g., <code>/tmp/checkpoints/...</code>)</li>
# MAGIC       <li>Workspace path (e.g., <code>/dbfs/mnt/...</code>)</li>
# MAGIC       <li>Unity Catalog (UC) volume path (e.g., <code>/Volumes/catalog/schema/volume/...</code>)</li>
# MAGIC     </ul>
# MAGIC   </li>
# MAGIC   <li>
# MAGIC     <b>Silver and Quarantine Table</b>:
# MAGIC     <ul>
# MAGIC       <li>Unity Catalog table (e.g., <code>catalog.schema.table</code>)</li>
# MAGIC     </ul>
# MAGIC   </li>
# MAGIC </ul>

# COMMAND ----------

# DBTITLE 1,Set Catalog,  Schema & checkpoint location for Demo Dataset
default_catalog_name = "main"
default_schema_name = "default"

dbutils.widgets.text("demo_catalog", default_catalog_name, "Catalog Name")
dbutils.widgets.text("demo_schema", default_schema_name, "Schema Name")

dbutils.widgets.text("silver_checkpoint", "/tmp/dq/structured/bronze_to_silver_checkpoint")
dbutils.widgets.text("quarantine_checkpoint", "/tmp/dq/structured/bronze_to_quarantine_checkpoint")

# COMMAND ----------

from uuid import uuid4

catalog = dbutils.widgets.get("demo_catalog")
schema = dbutils.widgets.get("demo_schema")

print(f"Selected Catalog for Demo Dataset: {catalog}")
print(f"Selected Schema for Demo Dataset: {schema}")

silver_checkpoint = dbutils.widgets.get("silver_checkpoint")
quarantine_checkpoint = dbutils.widgets.get("quarantine_checkpoint")

uuid = uuid4()
bronze_volume_name = f"bronze_{uuid}".replace("-", "_")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.{bronze_volume_name}")
bronze_path = f"/Volumes/{catalog}/{schema}/{bronze_volume_name}"
silver_table = f"{catalog}.{schema}.`silver_{uuid}`"
quarantine_table = f"{catalog}.{schema}.`quarantine_{uuid}`"

print(f"Demo Bronze Path: {bronze_path}")
print(f"Demo Silver Table: {silver_table}")
print(f"Demo Quarantine Table: {quarantine_table}")

# prepare sample test data
tips_url = "https://raw.githubusercontent.com/mwaskom/seaborn-data/master/tips.csv"
bronze_df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(tips_url)
)
bronze_df.write.mode("overwrite").format("parquet").save(f"{bronze_path}/data")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Apply data quality checks to streaming data using `DQEngine` Native method
# MAGIC This script performs data quality checks on a Parquet dataset, as follows:
# MAGIC 1. Reads an open-source tips dataset and writes it as Parquet files to a bronze location. This is needed for a sample parquet file(s) for streaming ingestion.
# MAGIC 2. Configures input for streaming ingestion using Auto Loader (cloudFiles) with schema tracking.
# MAGIC 3. Sets up output configurations for both the silver table and a quarantine table, specifying Delta format, checkpoint locations, and schema merging.
# MAGIC 4. Instantiates the `DQEngine`.
# MAGIC 5. Using `DQEngine` native method `apply_checks_by_metadata_and_save_in_table`, applies data quality checks to the input data stream, saving valid records to the silver table and invalid records to the quarantine table.

# COMMAND ----------

from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.config import InputConfig, OutputConfig
from databricks.sdk import WorkspaceClient

input_config=InputConfig(
      location=f"{bronze_path}/data",
      format="cloudFiles", 
      options={"cloudFiles.format": "parquet", "cloudFiles.schemaLocation": f"{bronze_path}/schema"},
      is_streaming=True
)

output_config=OutputConfig(
      location=silver_table,
      format="delta",
      mode="append",
      trigger={"availableNow": True},  # stop the stream once all data is processed
      options={"checkpointLocation": f"{silver_checkpoint}", "mergeSchema": "true"}
)

quarantine_config=OutputConfig(
      location=quarantine_table,
      format="delta",
      mode="append",
      trigger={"availableNow": True},  # stop the stream once all data is processed
      options={"checkpointLocation": f"{quarantine_checkpoint}", "mergeSchema": "true"}
)

dq_engine = DQEngine(WorkspaceClient())

dq_engine.apply_checks_by_metadata_and_save_in_table(
    checks=checks,
    input_config=input_config,
    output_config=output_config,
    quarantine_config=quarantine_config
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Display Results

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {silver_table}"))
display(spark.sql(f"SELECT * FROM {quarantine_table}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test Cleanup

# COMMAND ----------

dbutils.fs.rm(silver_checkpoint, True)
dbutils.fs.rm(quarantine_checkpoint, True)
spark.sql(f"DROP VOLUME IF EXISTS {catalog}.{schema}.{bronze_volume_name}")
spark.sql(f"DROP TABLE IF EXISTS {silver_table}")
spark.sql(f"DROP TABLE IF EXISTS {quarantine_table}")
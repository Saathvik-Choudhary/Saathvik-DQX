# Databricks notebook source
# MAGIC %md
# MAGIC # NB-02: DQX Installation & Validation
# MAGIC
# MAGIC **Purpose**
# MAGIC
# MAGIC Ensure DQX is correctly installed and compatible.
# MAGIC
# MAGIC **Responsibilities**
# MAGIC
# MAGIC - Validate Spark & DBR version
# MAGIC - Import DQX
# MAGIC - Run a sample check
# MAGIC - Fail fast if environment is misconfigured
# MAGIC
# MAGIC **Runs**
# MAGIC
# MAGIC Once per cluster / DBR upgrade.

# COMMAND ----------
# MAGIC %run ../_shared/env

# COMMAND ----------
print(f"Spark version: {spark.version}")

# COMMAND ----------
try:
    import databricks.labs.dqx  # noqa: F401
    print("DQX import: OK")
except Exception as exc:
    raise RuntimeError("DQX import failed") from exc

# COMMAND ----------
dbutils.widgets.text("validation_table", "", "Validation Table (catalog.schema.table)")
validation_table = dbutils.widgets.get("validation_table").strip()
if not validation_table:
    raise ValueError("validation_table is required to run a sample check")

# COMMAND ----------
from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient

ws = WorkspaceClient()
dq_engine = DQEngine(ws, spark)

# COMMAND ----------
df = spark.table(validation_table)
df.limit(1).count()

# COMMAND ----------
# TODO: Replace with a minimal DQX ruleset and run dq_engine.apply_checks.
# This is intentionally left for your exact rules/table conventions.
print("DQX engine initialized and validation table is readable.")


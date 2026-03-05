# Databricks notebook source
# MAGIC %md
# MAGIC # NB-04: Rule Generation (Non-AI)
# MAGIC
# MAGIC **Purpose**
# MAGIC
# MAGIC Convert profiles → deterministic rules.
# MAGIC
# MAGIC **Responsibilities**
# MAGIC
# MAGIC - Use DQGenerator
# MAGIC - Generate row / dataset rules
# MAGIC - Store rules in UC tables or YAML
# MAGIC
# MAGIC **Runs**
# MAGIC
# MAGIC After profiling; after schema change.

# COMMAND ----------
# MAGIC %run ../_shared/env

# COMMAND ----------
dbutils.widgets.text("input_table", "", "Input Table (catalog.schema.table)")
input_table = dbutils.widgets.get("input_table").strip()
if not input_table:
    raise ValueError("input_table is required")

# COMMAND ----------
from databricks.labs.dqx.profiler.generator import DQGenerator
from databricks.labs.dqx.profiler.profiler import DQProfiler
from databricks.labs.dqx.config import InputConfig
from databricks.sdk import WorkspaceClient

ws = WorkspaceClient()
profiler = DQProfiler(ws)
generator = DQGenerator(ws)

# COMMAND ----------
summary_stats, _profiles = profiler.profile_table(InputConfig(location=input_table))

# COMMAND ----------
# TODO: Replace with the exact non-AI generator method for your DQX version.
# Example (verify method name):
# rules_yaml = generator.generate_dq_rules(summary_stats=summary_stats)
rules_yaml = None

# COMMAND ----------
# TODO: Persist rules_yaml to your rules table or YAML storage.


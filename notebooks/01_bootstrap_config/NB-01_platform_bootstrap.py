# Databricks notebook source
# MAGIC %md
# MAGIC # NB-01: Platform Bootstrap
# MAGIC
# MAGIC **Purpose**
# MAGIC
# MAGIC One-time setup of the DQ platform.
# MAGIC
# MAGIC **Responsibilities**
# MAGIC
# MAGIC - Create Unity Catalog schemas
# MAGIC - Create rule tables
# MAGIC - Create metrics tables
# MAGIC - Create quarantine tables
# MAGIC - Define environment variables (dev/test/prod)
# MAGIC
# MAGIC **Runs**
# MAGIC
# MAGIC Once per environment; re-run only on infra change.

# COMMAND ----------
# MAGIC %run ../_shared/env

# COMMAND ----------
catalog = require_env_var("DQX_CATALOG")
rules_schema = require_env_var("DQX_RULES_SCHEMA")
metrics_schema = require_env_var("DQX_METRICS_SCHEMA")
quarantine_schema = require_env_var("DQX_QUARANTINE_SCHEMA")

# COMMAND ----------
for schema in (rules_schema, metrics_schema, quarantine_schema):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

# COMMAND ----------
# TODO: Create rules, metrics, and quarantine tables using your DQX conventions.
# Example env vars expected:
# - DQX_RULES_TABLE
# - DQX_METRICS_TABLE
# - DQX_QUARANTINE_TABLE
_ = get_rules_table()
_ = get_metrics_table()
_ = get_quarantine_table()


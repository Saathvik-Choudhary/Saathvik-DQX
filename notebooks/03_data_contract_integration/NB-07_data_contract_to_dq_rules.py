# Databricks notebook source
# MAGIC %md
# MAGIC # NB-07: Data Contract → DQ Rules
# MAGIC
# MAGIC **Purpose**
# MAGIC
# MAGIC Enforce ODCS contracts.
# MAGIC
# MAGIC **Responsibilities**
# MAGIC
# MAGIC - Read ODCS v3.x contract
# MAGIC - Generate schema & quality rules
# MAGIC - Merge with existing rule sets
# MAGIC
# MAGIC **Runs**
# MAGIC
# MAGIC When contract changes; before prod rollout.

# COMMAND ----------
# MAGIC %run ../_shared/env

# COMMAND ----------
dbutils.widgets.text("contract_path", "", "ODCS Contract Path")
contract_path = dbutils.widgets.get("contract_path").strip()
if not contract_path:
    raise ValueError("contract_path is required")

# COMMAND ----------
# TODO: Parse ODCS contract and convert to DQX rules.
# - Load contract from contract_path
# - Generate schema + quality rules
# - Merge with existing rules and persist
print(f"Ready to process contract: {contract_path}")


# Databricks notebook source
# MAGIC %md
# MAGIC # NB-06: AI Primary Key Detection
# MAGIC
# MAGIC **Purpose**
# MAGIC
# MAGIC Detect uniqueness constraints automatically.
# MAGIC
# MAGIC **Responsibilities**
# MAGIC
# MAGIC - Run PK detection
# MAGIC - Generate uniqueness rules
# MAGIC - Store as candidate rules
# MAGIC
# MAGIC **Runs**
# MAGIC
# MAGIC Once per dataset; re-run after schema expansion.

# COMMAND ----------
# MAGIC %run ../_shared/env

# COMMAND ----------
dbutils.widgets.text("input_table", "", "Input Table (catalog.schema.table)")
input_table = dbutils.widgets.get("input_table").strip()
if not input_table:
    raise ValueError("input_table is required")

# COMMAND ----------
# TODO: Replace with the exact AI PK detection API for your DQX version.
# Example outline:
# 1) Profile dataset
# 2) Detect candidate keys
# 3) Generate uniqueness rules

print(f"Ready to run PK detection for: {input_table}")


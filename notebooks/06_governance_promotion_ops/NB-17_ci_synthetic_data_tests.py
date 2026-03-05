# Databricks notebook source
# MAGIC %md
# MAGIC # NB-17: CI / Synthetic Data Tests
# MAGIC
# MAGIC **Purpose**
# MAGIC
# MAGIC Prevent bad rule deployments.
# MAGIC
# MAGIC **Responsibilities**
# MAGIC
# MAGIC - Generate synthetic datasets
# MAGIC - Test rule behavior
# MAGIC - Fail pipeline on regression
# MAGIC
# MAGIC **Runs**
# MAGIC
# MAGIC On PR / release.

# COMMAND ----------
# MAGIC %run ../_shared/env

# COMMAND ----------
# TODO: Implement CI-safe synthetic data generation for tests only.
# Ensure this notebook is only used in test/CI environments.
if ENV == "prod":
    raise RuntimeError("Synthetic tests must not run in prod")

# COMMAND ----------
# TODO: Execute rules against synthetic datasets and fail on regression.


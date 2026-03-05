# Databricks notebook source
# MAGIC %md
# MAGIC # NB-05: AI Rule Generation
# MAGIC
# MAGIC **Purpose**
# MAGIC
# MAGIC Human-friendly rule authoring.
# MAGIC
# MAGIC **Responsibilities**
# MAGIC
# MAGIC - Generate rules from natural language
# MAGIC - Suggest ranges, enums, nullability
# MAGIC - Flag rules as proposed
# MAGIC
# MAGIC **Runs**
# MAGIC
# MAGIC On demand (human-in-the-loop).

# COMMAND ----------
# MAGIC %run ../_shared/env

# COMMAND ----------
dbutils.widgets.text("model_name", "databricks/databricks-claude-sonnet-4-5", "Model Name")
dbutils.widgets.text("user_requirement", "", "User Requirement")
dbutils.widgets.text("table_name", "", "Table Name (optional)")

model_name = dbutils.widgets.get("model_name").strip()
user_requirement = dbutils.widgets.get("user_requirement").strip()
table_name = dbutils.widgets.get("table_name").strip()

if not user_requirement:
    raise ValueError("user_requirement is required")

# COMMAND ----------
from databricks.labs.dqx.profiler.generator import DQGenerator
from databricks.labs.dqx.config import LLMModelConfig, InputConfig
from databricks.sdk import WorkspaceClient

ws = WorkspaceClient()
llm_model_config = LLMModelConfig(model_name=model_name)
generator = DQGenerator(ws, llm_model_config=llm_model_config)

# COMMAND ----------
if table_name:
    checks = generator.generate_dq_rules_ai_assisted(
        user_input=user_requirement,
        input_config=InputConfig(location=table_name),
    )
else:
    checks = generator.generate_dq_rules_ai_assisted(user_input=user_requirement)

# COMMAND ----------
print(checks)

# COMMAND ----------
# TODO: Persist generated checks as "proposed" in your rules store.


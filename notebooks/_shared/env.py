# Databricks notebook source
# COMMAND ----------
import os


ENV = os.getenv("DQX_ENV", "dev")
if ENV not in {"dev", "test", "prod"}:
    raise ValueError(f"Unsupported DQX_ENV: {ENV}")


def require_env_var(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Missing required env var: {name}")
    return value


def get_rules_table() -> str:
    return require_env_var("DQX_RULES_TABLE")


def get_metrics_table() -> str:
    return require_env_var("DQX_METRICS_TABLE")


def get_quarantine_table() -> str:
    return require_env_var("DQX_QUARANTINE_TABLE")


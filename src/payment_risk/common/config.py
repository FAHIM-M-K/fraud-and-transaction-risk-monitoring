"""Lakehouse environment configuration and parameter resolution.

Resolves catalog, schema prefixes, volumes, and secret scopes dynamically
from Databricks widgets, falling back to environment variables or enterprise defaults.
"""

import os
from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class LakehouseConfig:
    """Immutable configuration resolving Unity Catalog identifiers and Volume paths."""

    catalog_name: str = "payment_risk"
    env_prefix: str = ""
    secret_scope: str = "grid_platform"

    @property
    def bronze_schema(self) -> str:
        """Fully qualified Bronze schema identifier (e.g. payment_risk.bronze)."""
        prefix = f"{self.env_prefix}_" if self.env_prefix else ""
        return f"{self.catalog_name}.{prefix}bronze"

    @property
    def silver_schema(self) -> str:
        """Fully qualified Silver schema identifier (e.g. payment_risk.silver)."""
        prefix = f"{self.env_prefix}_" if self.env_prefix else ""
        return f"{self.catalog_name}.{prefix}silver"

    @property
    def gold_schema(self) -> str:
        """Fully qualified Gold schema identifier (e.g. payment_risk.gold)."""
        prefix = f"{self.env_prefix}_" if self.env_prefix else ""
        return f"{self.catalog_name}.{prefix}gold"

    @property
    def landing_volume_path(self) -> str:
        """POSIX path to the Landing Zone Unity Catalog Volume."""
        prefix = f"{self.env_prefix}_" if self.env_prefix else ""
        return f"/Volumes/{self.catalog_name}/{prefix}bronze/landing_zone"

    @property
    def checkpoints_volume_path(self) -> str:
        """POSIX path to the RocksDB streaming checkpoints Volume."""
        prefix = f"{self.env_prefix}_" if self.env_prefix else ""
        return f"/Volumes/{self.catalog_name}/{prefix}bronze/checkpoints"


def resolve_lakehouse_config(dbutils_instance: Optional[object] = None) -> LakehouseConfig:
    """Dynamically resolves lakehouse configuration from Databricks widgets,

    falling back to environment variables or enterprise defaults.

    Args:
        dbutils_instance: Optional Databricks Utilities handle (dbutils) from notebook runtime.

    Returns:
        LakehouseConfig instance populated with resolved catalog and schema mappings.
    """
    catalog = "payment_risk"
    prefix = ""
    secret_scope = "grid_platform"

    if dbutils_instance is not None:
        try:
            dbutils_instance.widgets.text("catalog_name", "payment_risk", "Catalog Name")
            dbutils_instance.widgets.text("env_prefix", "", "Environment Prefix")
            dbutils_instance.widgets.text("secret_scope", "grid_platform", "Secret Scope")
            catalog = dbutils_instance.widgets.get("catalog_name").strip()
            prefix = dbutils_instance.widgets.get("env_prefix").strip()
            secret_scope = dbutils_instance.widgets.get("secret_scope").strip()
        except Exception:
            pass
    else:
        catalog = os.environ.get("LAKEHOUSE_CATALOG", "payment_risk")
        prefix = os.environ.get("LAKEHOUSE_ENV_PREFIX", "")
        secret_scope = os.environ.get("LAKEHOUSE_SECRET_SCOPE", "grid_platform")

    return LakehouseConfig(
        catalog_name=catalog,
        env_prefix=prefix,
        secret_scope=secret_scope,
    )

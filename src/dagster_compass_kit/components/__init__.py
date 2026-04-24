"""Dagster components exposed by this package.

Registered under the ``dagster_dg_cli.registry_modules`` entry point so
``dg`` commands and YAML scaffolding see them.
"""

from .daily_digest import DailyInsightDigest

__all__ = ["DailyInsightDigest"]

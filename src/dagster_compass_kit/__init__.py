"""Community-supported Dagster integration for Dagster+ Compass."""

from .client import CompassResponse, ToolCall, stream_ai_chat, stream_ai_summary_for_asset
from .hooks import compass_on_failure
from .resource import CompassResource
from .retry import (
    ExceptionAnalysis,
    compass_retry_advisor,
    exception_fingerprint,
    parse_analysis,
)

__all__ = [
    "CompassResource",
    "CompassResponse",
    "ExceptionAnalysis",
    "ToolCall",
    "compass_on_failure",
    "compass_retry_advisor",
    "exception_fingerprint",
    "parse_analysis",
    "stream_ai_chat",
    "stream_ai_summary_for_asset",
]

__version__ = "0.1.0"

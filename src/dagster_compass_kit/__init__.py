"""Community-supported Dagster+ Compass integration for Dagster pipelines.

Dagster+-Compass-specific: the kit hits the WebSocket subscription at your
``<org>.dagster.cloud`` tenant with a Dagster+ API token. Not a client for
standalone compass.dagster.io.
"""

from .checks import compass_asset_check
from .client import (
    CompassResponse,
    ToolCall,
    stream_ai_chat,
    stream_ai_summary_for_asset,
)
from .compass_client import CompassClient, CompassConversation
from .hooks import compass_on_failure
from .models import AnomalyVerdict, MonitoringDecision, RunbookSections
from .resource import CompassResource
from .retry import (
    ExceptionAnalysis,
    compass_retry_advisor,
    exception_fingerprint,
    heuristic_classify,
    parse_analysis,
)
from .runbook import (
    generate_asset_runbook,
    generate_job_runbook,
    generate_structured_runbook,
)
from .sensors import compass_sensor
from .structured import (
    CompassSchemaError,
    build_structured_prompt,
    extract_json,
    parse_structured,
)

__all__ = [
    # Core surfaces
    "CompassResource",
    "CompassClient",
    "CompassConversation",
    "CompassResponse",
    "ToolCall",
    # Hooks / checks / sensors
    "compass_on_failure",
    "compass_retry_advisor",
    "compass_asset_check",
    "compass_sensor",
    # Structured responses
    "build_structured_prompt",
    "parse_structured",
    "extract_json",
    "CompassSchemaError",
    # Pre-built Pydantic schemas
    "AnomalyVerdict",
    "MonitoringDecision",
    "RunbookSections",
    # Retry-advisor helpers
    "ExceptionAnalysis",
    "exception_fingerprint",
    "heuristic_classify",
    "parse_analysis",
    # Runbook generation
    "generate_job_runbook",
    "generate_asset_runbook",
    "generate_structured_runbook",
    # Low-level streaming (for real-time UI consumers)
    "stream_ai_chat",
    "stream_ai_summary_for_asset",
]

__version__ = "0.2.0"

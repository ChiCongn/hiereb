"""
Shared structlog configuration.
Call configure_logging() once at service startup.
"""
from __future__ import annotations

import logging
import structlog


def configure_logging(level: str = "INFO") -> None:
    """
    Configure structlog with JSON output.

    Args:
        level: Python log level string ("DEBUG", "INFO", "WARNING", "ERROR")
    """
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(message)s",
    )

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.stdlib.add_log_level,
            structlog.stdlib.add_logger_name,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.stdlib.BoundLogger,
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
    )
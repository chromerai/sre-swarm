"""
structlog configuration.
JSON output in production, colored console in dev.
Call configure_logging() once at process startup, before any logger is used.
"""

import logging
import sys
import structlog

from shared.config.settings import Settings

def configure_logging(settings: Settings | None = None) -> None:
    settings = settings or Settings()
    log_level = getattr(logging, settings.log_level.upper(), logging.INFO)

    shared_processors = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
    ]

    if settings.log_level.upper() == 'DEBUG' or sys.stderr.isatty():
        renderer = structlog.dev.ConsoleRenderer()
    else:
        renderer = structlog.processors.JSONRenderer()
    
    structlog.configure(
        processors=shared_processors + [renderer],
        wrapper_class=structlog.make_filtering_bound_logger(log_level),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )

def get_logger(name: str, **bind_kwargs):
    """
    Returns a BoundLogger with optional pre-bound context (e.g. agent name)
    """

    return structlog.get_logger(name).bind(**bind_kwargs)

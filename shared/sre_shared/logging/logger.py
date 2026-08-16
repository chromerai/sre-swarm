"""
structlog configuration.
JSON output in production, colored console in dev.
Call configure_logging() once at process startup, before any logger is used.
"""

import logging
import sys
from typing import Any

import structlog
from structlog.contextvars import bind_contextvars, clear_contextvars
from sre_shared.config.settings import Settings

def configure_logging(settings: Settings | None = None) -> None:
    settings = settings or Settings()
    log_level = getattr(logging, settings.log_level.upper(), logging.INFO)

    shared_processors = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.ExceptionRenderer(),
        structlog.stdlib.PositionalArgumentsFormatter(),
    ]

    if settings.log_level.upper() == 'DEBUG' or sys.stdout.isatty():
        renderer = structlog.dev.ConsoleRenderer(colors=True)
    else:
        renderer = structlog.processors.JSONRenderer()
    
    structlog.configure(
        processors=shared_processors + [renderer],
        wrapper_class=structlog.make_filtering_bound_logger(log_level),
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )

    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=log_level,
    )

def get_logger(name: str, **bind_kwargs):
    """
    Returns a BoundLogger with optional pre-bound context (e.g. agent name)
    """

    return structlog.get_logger(name).bind(**bind_kwargs)

def bind_context(
        correlation_id: str | None = None,
        incident_id: str | None = None,
        **extra: Any
) -> None:

    context: dict[str, Any] = {**extra}

    if correlation_id:
        context['correlation_id'] = correlation_id
    if incident_id:
        context['incident_id'] = incident_id
    bind_contextvars(**context)

def clear_context() -> None:
    clear_contextvars()

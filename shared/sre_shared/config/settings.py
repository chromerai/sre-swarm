"""
Pydantic BaseSettings — all configuration via environment variables.
Single import point for config across all agents.
"""

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]

class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=REPO_ROOT / ".env",
        extra="ignore")

    environment: str ="development"

    #core infra
    postgres_url: str = "postgresql+asyncpg://sre_swarm:simplepassword@localhost:5432/sre_agent"
    postgres_pool_size: int = 10
    postgres_max_overflow: int = 20

    nats_url: str = "nats://localhost:4222"
    nats_max_reconnect_attempts: int = 10
    nats_reconnect_time_wait: float =  2.00
    nats_ack_wait_seconds: int = 30
    nats_max_deliver: int = 5

    log_level: str = "INFO"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """
    Return a cached singleton Settings instance.

    Use this instead of instantiating Settings directly so that the .env file
    is only parsed once per process.
    """
    return Settings()


# Module-level singleton — import this directly in most cases.
settings: Settings = get_settings()
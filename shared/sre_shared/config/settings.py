"""
Pydantic BaseSettings — all configuration via environment variables.
Single import point for config across all agents.
"""

from functools import lru_cache
from typing import Literal

from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field, computed_field
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]

class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=REPO_ROOT / ".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
        )

    #environment
    environment: Literal["development", "production", "test"] = "development"
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = "INFO"

    #postgres - individual components, single source of truth
    #postgres_url: str = "postgresql+asyncpg://sre_swarm:simplepassword@localhost:5432/sre_agent"
    postgres_user: str = Field(default="sre_swarm")
    postgres_password: str = Field(default="simplepassword")
    postgres_db: str = Field(default="sre_agent")
    postgres_host: str = Field(default="localhost")
    postgres_port: int = Field(default=5432, gt=0, le=65535)
    postgres_pool_size: int =Field(default=10, ge=1)
    postgres_max_overflow: int = Field(default=20, ge=0)

    @computed_field
    @property
    def postgres_url(self) -> str:
        return (
            f"postgresql+asyncpg://{self.postgres_user}:{self.postgres_password}@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )

    #nats
    nats_url: str = Field(default="nats://localhost:4222", description="NATS connection url")
    nats_max_reconnect_attempts: int = Field(default=10, ge=0)
    nats_reconnect_time_wait: float =  Field(default=2.0, gt=0.0)
    nats_ack_wait_seconds: int = Field(default=30, gt=10)
    nats_max_deliver: int = Field(default=5, ge=1)
    agent_heartbeat_interval_seconds: int = Field(default=30, gt=0)
    agent_heartbeat_timeout_seconds: int = Field(default=90, gt=0)
    


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
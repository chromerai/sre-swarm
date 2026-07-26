"""
Pydantic BaseSettings — all configuration via environment variables.
Single import point for config across all agents.
"""

from pydantic_settings import BaseSettings, SettingsConfigDict
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]

class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=REPO_ROOT / ".env",
        extra="ignore")

    #core infra
    postgres_url: str = "postgresql+asyncpg://sre_swarm:simplepassword@localhost:5432/sre_agent"

    nats_url: str = "nats://localhost:4222"
    nats_max_reconnect_attempts: int = 10
    nats_reconnect_time_wait: float =  2.00
    nats_ack_wait_seconds: int = 30
    nats_max_deliver: int = 5

    log_level: str = "INFO"
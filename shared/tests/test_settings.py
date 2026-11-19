"""
Tests for sre_shared.config.settings.Settings

Covers: default values, env-var override loading, Field-level validation errors, and the computed
postgres_url property.

Note: More tests may be added as the Settings will grow!
"""

import pytest
from pydantic import ValidationError

from sre_shared.config.settings import Settings, get_settings

class TestDefaults:
    """
    Settings should be fully constructible with zero env vars set, using sensible dev-friendly defaults.
    """

    def test_constructs_with_no_env_vars(self, monkeypatch):
        monkeypatch.setitem(Settings.model_config, "env_file", None)
        for var in (
            "ENVIRONMENT", "LOG_LEVEL", 
            "POSTGRES_USER", "POSTGRES_PASSWORD", "POSTGRES_DB",
            "POSTGRES_HOST", "POSTGRES_PORT",
            "NATS_URL",
        ):
            monkeypatch.delenv(var, raising=False)

            s = Settings()

            assert s.environment == "development"
            assert s.log_level == "INFO"
            assert s.postgres_user == "sre_swarm"
            assert s.nats_url == "nats://localhost:4222"
            assert s.agent_heartbeat_interval_seconds == 30
            assert s.agent_heartbeat_timeout_seconds == 90

class TestEnvVarLoading:
    """
    Confirms env vars actually override field defaults, and that
    case-insensitivity works as configured.
    """

    def test_env_var_overrides_default(self, monkeypatch):
        get_settings.cache_clear()

        monkeypatch.setenv("POSTGRES_USER", "test_user")
        monkeypatch.setenv("NATS_URL", "nats://test-host:4222")

        s = get_settings()
        assert s.postgres_user == "test_user"
        assert s.nats_url == "nats://test-host:4222"

    def test_case_insensitive_env_var_matching(self, monkeypatch):
        # case_sensitive=False in model_config — lowercase env var
        # should still bind to the field.
        get_settings.cache_clear()
        monkeypatch.setenv("postgres_db", "lowercase_db")

        s = get_settings()
        assert s.postgres_db == "lowercase_db"

    def test_unrecognized_env_var_is_ignored_not_rejected(self, monkeypatch):
        get_settings.cache_clear()
        monkeypatch.setenv("SOME_UNRELATED_SERVICE_VAR", "whatever")

        s = get_settings()  # should not raise
        assert not hasattr(s, "some_unrelated_service_var")

class TestFieldValidation:
    """
    Confirms Field() constraints actually reject invalid values, rather than silently accepting them.
    """

    def test_invalid_environment_literal_raises(self, monkeypatch):
        get_settings.cache_clear()

        monkeypatch.setenv("ENVIRONMENT", "not_a_real_environment")
        with pytest.raises(ValidationError):
            get_settings()

    def test_invalid_log_level_literal_raises(self, monkeypatch):
        get_settings.cache_clear()

        monkeypatch.setenv("LOG_LEVEL", "SUPER_VERBOSE")
        with pytest.raises(ValidationError):
            get_settings()

    def test_negative_postgres_pool_size_raises(self, monkeypatch):
        get_settings.cache_clear()

        monkeypatch.setenv("POSTGRES_POOL_SIZE", "-1")
        with pytest.raises(ValidationError):
            get_settings()

    def test_zero_postgres_pool_size_raises(self, monkeypatch):
        get_settings.cache_clear()
        # ge=1 — zero is not a valid pool size
        monkeypatch.setenv("POSTGRES_POOL_SIZE", "0")
        with pytest.raises(ValidationError):
            get_settings()

    def test_negative_max_overflow_raises(self, monkeypatch):
        get_settings.cache_clear()

        monkeypatch.setenv("POSTGRES_MAX_OVERFLOW", "-5")
        with pytest.raises(ValidationError):
            get_settings()

    def test_postgres_port_out_of_range_raises(self, monkeypatch):
        get_settings.cache_clear()

        monkeypatch.setenv("POSTGRES_PORT", "70000")  # > 65535
        with pytest.raises(ValidationError):
            get_settings()

    def test_zero_nats_reconnect_time_wait_raises(self, monkeypatch):
        get_settings.cache_clear()
        # gt=0 — zero is invalid, must be strictly positive
        monkeypatch.setenv("NATS_RECONNECT_TIME_WAIT", "0")
        with pytest.raises(ValidationError):
            get_settings()

    def test_zero_heartbeat_interval_raises(self, monkeypatch):
        get_settings.cache_clear()

        monkeypatch.setenv("AGENT_HEARTBEAT_INTERVAL_SECONDS", "0")
        with pytest.raises(ValidationError):
            get_settings()

    def test_zero_max_deliver_raises(self, monkeypatch):
        get_settings.cache_clear()
        # ge=1 — a message must be deliverable at least once
        monkeypatch.setenv("NATS_MAX_DELIVER", "0")
        with pytest.raises(ValidationError):
            get_settings()

    def test_non_integer_pool_size_raises(self, monkeypatch):
        get_settings.cache_clear()

        monkeypatch.setenv("POSTGRES_POOL_SIZE", "not_a_number")
        with pytest.raises(ValidationError):
            get_settings()

class TestPostgresUrlComputedField:
    """
    Confirms postgres_url is correctly assembled from components, with no drift
    between the individual fields and the derived URL
    """

    def test_postgres_url_reflects_defaults(self,monkeypatch):
        monkeypatch.setitem(Settings.model_config, "enf_file", None)

        for var in (
            "POSTGRES_USER", "POSTGRES_PASSWORD", "POSTGRES_DB",
            "POSTGRES_HOST", "POSTGRES_PORT",
        ):
            monkeypatch.delenv(var, raising=False)

        s = Settings()

        assert s.postgres_url == (
            "postgresql+asyncpg://sre_swarm:simplepassword@localhost:5432/sre_agent"
        )

    def test_postgres_url_reflects_env_overrides(self, monkeypatch):
        get_settings.cache_clear()

        monkeypatch.setenv("POSTGRES_USER", "test_user")
        monkeypatch.setenv("POSTGRES_PASSWORD", "test_pass")
        monkeypatch.setenv("POSTGRES_DB", "test_db")
        monkeypatch.setenv("POSTGRES_HOST", "db.internal")
        monkeypatch.setenv("POSTGRES_PORT", "5433")

        s = get_settings()
        assert s.postgres_url == (
            "postgresql+asyncpg://test_user:test_pass@db.internal:5433/test_db"
        )

    def test_postgres_url_updates_when_only_password_changes(self, monkeypatch):
        get_settings.cache_clear()

        monkeypatch.setenv("POSTGRES_PASSWORD", "rotated_password")

        s = get_settings()
        assert "rotated_password" in s.postgres_url
        assert "simplepassword" not in s.postgres_url

    

class TestSingletonCaching:
    """Confirms get_settings() actually caches, and that the fixture-level
    cache_clear() is doing what it claims."""

    def test_get_settings_returns_same_instance(self):
        s1 = get_settings()
        s2 = get_settings()
        assert s1 is s2

    def test_cache_clear_produces_new_instance(self, monkeypatch):
        get_settings.cache_clear()

        s1 = get_settings()

        get_settings.cache_clear()
        
        monkeypatch.setenv("POSTGRES_DB", "different_db")
        s2 = get_settings()

        assert s1 is not s2
        assert s2.postgres_db == "different_db"
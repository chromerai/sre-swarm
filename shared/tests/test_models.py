"""
Unit tests for sre_shared.db.models — SQLAlchemy ORM model definitions.

"""



from __future__ import annotations

import uuid
from datetime import datetime

import pytest
from sqlalchemy import inspect, Table, UniqueConstraint, Integer, CheckConstraint
from sqlalchemy.dialects.postgresql import JSONB, UUID as PG_UUID
from sqlalchemy.exc import IntegrityError

from sre_shared.db.models import (
    Incident,
    RateLimitRecord,
    RemediationState,
    RunbookStats,
    TimelineEvent,
    AuditLog,
    DetectionMode
)

# ========================================================================== #
# Reusable helpers                                                           #
# ========================================================================== #

def assert_no_relationships(model_cls) -> None:
    mapper = inspect(model_cls)
    assert len(mapper.relationships.keys()) == 0, (
        f"{model_cls.__name__} has relationship(s) defined - "
        f"violates the project's no-model-coupling decision."
    )

def assert_columns_present(model_cls, required_fields: set[str]) -> None:
    mapper = inspect(model_cls)
    col_names = {c.key for c in mapper.columns}
    missing = required_fields - col_names
    assert not missing, (
        f"{model_cls.__name__} missing required columns: {sorted(missing)}"
    )

def assert_column_is_jsonb(model_cls, column_name: str) -> None:
    mapper = inspect(model_cls)
    col_type = mapper.columns[column_name].type
    assert isinstance(col_type, JSONB), (
        f"{model_cls.__name__}.{column_name} should be JSONB, "
        f"got {type(col_type).__name__}"
    )

def assert_column_is_uuid_as_uuid(model_cls, column_name: str) -> None:
    mapper = inspect(model_cls)
    col = mapper.columns[column_name]
    assert isinstance(col.type, PG_UUID), (
        f"{model_cls.__name__}.{column_name} should be postgresql.UUID. "
    )

    assert col.type.as_uuid is True, (
        f"{model_cls.__name__}.{column_name} UUID column has as_uuid=False"
    )

# ========================================================================== #
# Incident                                                                   #
# ========================================================================== #

class TestIncidentModel:
    def test_table_name(self):
        assert Incident.__tablename__ == "incidents"

    def test_id_is_uuid_as_uuid(self):
        assert_column_is_uuid_as_uuid(Incident, "id")

    def test_incident_detection_mode_nullable_is_false(self):
        mapper = inspect(Incident)
        assert mapper.columns["detection_mode"].nullable is False

    def test_incident_columns_present(self):
        assert_columns_present(Incident, {
            "id", "status", "detection_mode", "detected_at",
            "resolved_at", "updated_at", "closed_at",
            "diagnosis", "diagnosis_confidence", "raw_event",
            "proposed_action", "action_type",
            "verification_result", "blast_radius_score",
            "root_cause_service", "root_cause_category",
            "operator_decision", "resolution_summary", "postmortem",
        })

    def test_check_constraint(self):
        mapper = inspect(Incident)
        assert isinstance(mapper.local_table, Table)

        checks= [c for c in mapper.local_table.constraints if isinstance(c, CheckConstraint)]

        assert len(checks) == 1

        name, sqltext = checks[0].name, str(checks[0].sqltext)

        assert name == "ck_incidents_diagnosis_confidence_range"
        assert sqltext == "diagnosis_confidence IS NULL OR (diagnosis_confidence >= 0 AND diagnosis_confidence <= 100)"




# ---------------------------------------------------------------------------
# Incident — DB-enforced constraints (NOT run by default — needs a real
# session). Un-skip once a DB session fixture exists.
# ---------------------------------------------------------------------------


@pytest.mark.skip(reason="Requires an async DB session fixture (`db_session`) "
                          "against a real Postgres — constraints and defaults "
                          "only fire at flush() time. Wire up the fixture, "
                          "then remove this marker.")
class TestIncidentPersistence:
    """Behavior only the database layer enforces: defaults, constraints."""

    # --- defaults ---

    async def test_defaults_populate_on_flush(self, db_session):
        inc = Incident(detection_mode=DetectionMode.FULL)
        db_session.add(inc)
        await db_session.flush()

        assert type(inc.id) is uuid.UUID
        assert inc.status == "detecting"
        assert isinstance(inc.detected_at, datetime)
        assert isinstance(inc.updated_at, datetime)
        assert inc.resolved_at is None
        assert inc.closed_at is None

    # --- nullability ---

    async def test_missing_detection_mode_rejected(self, db_session):
        db_session.add(Incident(status="testing"))
        with pytest.raises(IntegrityError):
            await db_session.flush()

    # --- diagnosis_confidence CheckConstraint (0-100) ---

    @pytest.mark.parametrize("bad_value", [-1, -10, 101, 150, None])
    async def test_diagnosis_confidence_rejects_out_of_range(
        self, db_session, bad_value
    ):
        db_session.add(
            Incident(detection_mode=DetectionMode.FULL,
                     diagnosis_confidence=bad_value)
        )
        with pytest.raises(IntegrityError):
            await db_session.flush()

    @pytest.mark.parametrize("ok_value", [0, 50, 100])
    async def test_diagnosis_confidence_accepts_valid_range(
        self, db_session, ok_value
    ):
        db_session.add(
            Incident(detection_mode=DetectionMode.FULL,
                     diagnosis_confidence=ok_value)
        )
        await db_session.flush()  # must not raise


# ---------------------------------------------------------------------------
# TimelineEvent
# ---------------------------------------------------------------------------

class TestTimelineEventModel:
    def test_table_name(self):
        assert TimelineEvent.__tablename__ == "timeline_events"

    def test_id_is_integer_primary_key(self):
        mapper = inspect(TimelineEvent)
        assert [c.key for c in mapper.primary_key] == ["id"]
        assert isinstance(mapper.columns["id"].type, Integer)

    def test_incident_id_is_uuid_as_uuid(self):
        assert_column_is_uuid_as_uuid(TimelineEvent, "incident_id")

    def test_columns_present(self):
        assert_columns_present(TimelineEvent, {
            "id", "incident_id", "from_state", "to_state",
            "agent_type", "agent_instance_id", "payload", "timestamp",
        })

    def test_jsonb_payload(self):
        assert_column_is_jsonb(TimelineEvent, "payload")

    def test_composite_index_on_incident_id_and_timestamp(self):
        # Confirms the documented composite index exists — dashboard
        # timeline fetch depends on this being indexed, not a full scan.
        mapper = inspect(TimelineEvent)
        assert isinstance(mapper.local_table, Table)

        index_column_sets = [
            frozenset(col.name for col in idx.columns)
            for idx in mapper.local_table.indexes
        ]
        assert frozenset({"incident_id", "timestamp"}) in index_column_sets, (
            "Missing composite index on (incident_id, timestamp) — "
            "documented as required for the dashboard timeline fetch pattern."
        )


# ---------------------------------------------------------------------------
# AuditLog
# ---------------------------------------------------------------------------

class TestAuditLogModel:
    def test_table_name(self):
        assert AuditLog.__tablename__ == "audit_logs"

    def test_detection_mode_nullable_is_false(self):
        mapper = inspect(AuditLog)
        assert mapper.columns["detection_mode"].nullable is False

    def test_columns_present(self):
        assert_columns_present(AuditLog, {
            "id", "incident_id", "decision_type", "agent_type",
            "agent_instance_id", "action", "blast_radius",
            "policy_result", "operator_decision", "topology_source",
            "detection_mode", "payload", "timestamp",
        })

    def test_jsonb_payload(self):
        assert_column_is_jsonb(AuditLog, "payload")

    def test_index_on_incident_id(self):
        mapper = inspect(AuditLog)

        assert isinstance(mapper.local_table, Table)
        indexed_columns = {
            col.name
            for idx in mapper.local_table.indexes
            for col in idx.columns
        }
        assert "incident_id" in indexed_columns, (
            "Missing index on incident_id — required for the audit "
            "drill-down query pattern."
        )


# ---------------------------------------------------------------------------
# RemediationState
# ---------------------------------------------------------------------------

class TestRemediationStateModel:
    def test_table_name(self):
        assert RemediationState.__tablename__ == "remediation_states"

    def test_columns_present(self):
        assert_columns_present(RemediationState, {
            "id", "incident_id", "runbook_id", "rendered_action",
            "rollback_action", "cluster_snapshot", "status", "created_at",
        })

    def test_jsonb_cluster_snapshot(self):
        assert_column_is_jsonb(RemediationState, "cluster_snapshot")

    def test_index_on_incident_id(self):
        mapper = inspect(RemediationState)

        assert isinstance(mapper.local_table, Table)
        indexed_columns = {
            col.name
            for idx in mapper.local_table.indexes
            for col in idx.columns
        }
        assert "incident_id" in indexed_columns, (
            "Missing index on incident_id — required for rollback "
            "lookup under failure conditions."
        )


# ---------------------------------------------------------------------------
# RateLimitRecord
# ---------------------------------------------------------------------------

class TestRateLimitRecordModel:
    def test_table_name(self):
        assert RateLimitRecord.__tablename__ == "rate_limit_records"

    def test_columns_present(self):
        assert_columns_present(RateLimitRecord, {
            "id", "action_fingerprint", "namespace", "count", "timestamp",
        })

    def test_unique_constraint_on_fingerprint_and_namespace(self):
        # Required as the ON CONFLICT target for the increment-in-place
        # upsert design — a plain index cannot serve as an ON CONFLICT
        # target, this must be a real UniqueConstraint.
        mapper = inspect(RateLimitRecord)
        assert isinstance(mapper.local_table, Table)
        unique_constraint_column_sets = [
            frozenset(col.name for col in uc.columns)
            for uc in mapper.local_table.constraints
            if isinstance(uc, UniqueConstraint)
        ]
        assert frozenset({"action_fingerprint", "namespace"}) in unique_constraint_column_sets, (
            "Missing UniqueConstraint(action_fingerprint, namespace) — "
            "required as the ON CONFLICT DO UPDATE target."
        )


# ---------------------------------------------------------------------------
# RunbookStats
# ---------------------------------------------------------------------------

class TestRunbookStatsModel:
    def test_table_name(self):
        assert RunbookStats.__tablename__ == "runbook_stats"

    def test_runbook_id_is_primary_key_no_surrogate(self):
        mapper = inspect(RunbookStats)
        pk_columns = [c.key for c in mapper.primary_key]
        assert pk_columns == ["runbook_id"], (
            "RunbookStats should use runbook_id as the natural primary "
            "key — no surrogate 'id' column, per the documented decision "
            "(always exactly one row per runbook, always upserted)."
        )

    def test_columns_present(self):
        assert_columns_present(RunbookStats, {
            "runbook_id", "total_attempts", "successes",
            "total_mttr_sec", "last_updated_at",
        })
# ---------------------------------------------------------------------------
# Cross-model — protects the project-wide convention itself, not any
# one model. Add new models to this list as they're built.
# ---------------------------------------------------------------------------

class TestCrossModelConventions:
    ALL_MODELS = [
        Incident, TimelineEvent, AuditLog,
        RemediationState, RateLimitRecord, RunbookStats,
    ]

    @pytest.mark.parametrize("model_cls", ALL_MODELS)
    def test_no_model_has_relationships(self, model_cls):
        assert_no_relationships(model_cls)
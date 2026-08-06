"""
SQLAlchemy ORM models for the agent incident store (sre_shared DB).

Tables:
  - incidents:           Full lifecycle record of each detected incident.
  - timeline_events:      Append-only log of every FSM state transition for
                          an incident. Source for the dashboard's FSM timeline.
  - audit_logs:           Immutable record of every safety decision — blast
                          radius, policy result, topology_source, detection
                          mode, and full decision payload. Never updated
                          after insert.
  - remediation_states:   Pre-execution snapshot and rollback plan for a
                          remediation attempt, enabling immediate rollback and crash
                          recovery.
  - rate_limit_records:   Per-action-fingerprint rate limiting counters,
                          incremented in place. Persisted so limits survive
                          agent restarts.
  - runbook_stats:        Aggregate success/failure/MTTR counters per
                          runbook, upserted after every resolved incident.
                          
  - agent_heartbeats:     Latest heartbeat per running agent instance — used
                          for dead-agent detection and liveness monitoring.

All timestamps are timezone-aware, stored in UTC.
"""

from __future__ import annotations

import enum
import uuid
from datetime import datetime, timezone
from typing import Any, Optional

from sqlalchemy import (
    BigInteger,
    DateTime,
    Enum as SQLEnum,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
    Index,
)

from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


def utcnow() -> datetime:
    return datetime.now(timezone.utc)

class Base(DeclarativeBase):
    """Shared declarative base for all sre_shared ORM models"""
    pass

class DetectionMode(str, enum.Enum):
    K8S_ONLY = "k8s_only"
    OBSERVABILITY_ONLY = "observability_only"
    FULL = "full"

class Incident(Base):
    __tablename__ = "incidents"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )

    status: Mapped[str] = mapped_column(
        String,
        nullable=False,
        default="detecting",
        comment="Current FSM state - validated upstream by incident_fsm.py. not here"
    )

    detection_mode: Mapped[DetectionMode] = mapped_column(
        SQLEnum(DetectionMode, name="detection_mode_enum"),
        nullable=False,
    )

    detected_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), 
        nullable=False, 
        default=utcnow,
    )

    resolved_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )

    raw_event: Mapped[Optional[dict]] = mapped_column(
        JSONB,
        nullable=True,
        comment="Original Observer payload"
    )

    diagnosis: Mapped[Optional[str]] = mapped_column(
        Text,
        nullable=True,
    )

    proposed_action: Mapped[Optional[str]] = mapped_column(
        Text,
        nullable=True,
    )

    action_type: Mapped[Optional[str]] = mapped_column(
        String,
        nullable=True,
        comment="config_change | live_action"
    )

    verification_result: Mapped[Optional[dict]] = mapped_column(
        JSONB,
        nullable=True,
        comment="{passed, reason, checks_run, warnings} - mirrrors VerificationResult"
    )

    blast_radius_score: Mapped[Optional[int]] = mapped_column(
        Integer,
        nullable=True
    )

    operator_decision: Mapped[Optional[str]] = mapped_column(
        String,
        nullable=True,
        comment="approved | rejected"
    )

    resolution_summary: Mapped[Optional[str]] = mapped_column(
        Text,
        nullable=True
    )

    postmortem: Mapped[Optional[dict]] = mapped_column(
        JSONB,
        nullable=True
    )

class TimelineEvent(Base):
    """
    One row per FSM transition. Kept deliberately lean — payload holds only
    a small summary dict, never the full diagnosis/audit detail. When a user
    drills into a timeline entry on the dashboard, the heavier context comes
    from AuditLog (for safety_review entries) or the current Incident row
    (for diagnosing/verifying entries), not from here.
    """

    __tablename__ = "timeline_events"

    id: Mapped[int] = mapped_column(
        Integer,
        primary_key=True,
        autoincrement=True,
    )

    incident_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("incidents.id"),
        nullable=False,
    )

    from_state: Mapped[str] = mapped_column(
        String,
        nullable=False,
    )

    to_state: Mapped[str] = mapped_column(
        String,
        nullable=False
    )

    agent_type: Mapped[str] = mapped_column(
        String,
        nullable=False,
        comment="e.g. 'remediator'"
    )

    agent_instance_id: Mapped[str] = mapped_column(
        String,
        nullable=False,
        comment='e.g. "remediator-2" -specific replica'
    )

    payload: Mapped[Optional[dict]] = mapped_column(
        JSONB,
        nullable=True,
        comment="Small summary only"
    )

    timestamp: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=utcnow
    )

    __table_args__ = (
        Index("ix_timeline_events_incident_timestamp", "incident_id", "timestamp"),
    )

class AuditLog(Base):
    """
    Immutable record of every safety decision. Never updated after insert —
    only ever INSERTed. Holds the heavier context (full approval/rejection
    payload) that TimelineEvent deliberately omits.
    """

    __tablename__ = "audit_logs"

    id: Mapped[int] = mapped_column(
        Integer, 
        primary_key=True,
        autoincrement=True,
    )

    incident_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("incidents.id"),
        nullable=False,
    )

    decision_type: Mapped[str] = mapped_column(
        String,
        nullable=False,
    )

    agent_type: Mapped[str] = mapped_column(String, nullable=False)
    agent_instance_id: Mapped[str] = mapped_column(String, nullable=False)

    action: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    blast_radius: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    policy_result: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    operator_decision: Mapped[Optional[str]] = mapped_column(String, nullable=True)

    topology_source: Mapped[Optional[str]] = mapped_column(
        String,
        nullable=True,
        comment="discovered | static | unavailable"
    )

    detection_mode: Mapped[DetectionMode] = mapped_column(
        SQLEnum(DetectionMode, name="detection_mode_enum"),
        nullable=False
    )

    payload: Mapped[Optional[dict]] = mapped_column(
        JSONB,
        nullable=True,
        comment="Full context - approval payload, rejection reason, etc"
    )

    timestamp: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=utcnow
    )

    __table_args__ = (
        Index("ix_audit_logs_incident_id", "incident_id"),
    )

class RemediationState(Base):


    __tablename__ = "remediation_states"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    incident_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("incidents.id"),
        nullable=False
    )

    runbook_id: Mapped[str] = mapped_column(String, nullable=False)

    rendered_action: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    rollback_action: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    cluster_snapshot: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
 
    status: Mapped[str] = mapped_column(
        String, nullable=False, default="planned",
        comment="planned | executing | rolled_back | complete",
    )
 
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utcnow
    )
 
    __table_args__ = (
        Index("ix_remediation_states_incident_id", "incident_id"),
    )

class RateLimitRecord(Base):

    __tablename__ = "rate_limit_records"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
 
    action_fingerprint: Mapped[str] = mapped_column(String, nullable=False)
    namespace: Mapped[str] = mapped_column(String, nullable=False)
 
    count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
 
    timestamp: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utcnow,
        comment="Last time this fingerprint fired — updated on every increment",
    )
 
    __table_args__ = (
        UniqueConstraint(
            "action_fingerprint", "namespace",
            name="uq_rate_limit_fingerprint_namespace",
        ),
    )

class RunbookStats(Base):

    __tablename__ = "runbook_stats"
 
    runbook_id: Mapped[str] = mapped_column(String, primary_key=True)
 
    total_attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    successes: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    total_mttr_sec: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
 
    last_updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utcnow, onupdate=utcnow
    )

class AgentHeartbeat(Base):

    __tablename__ = "agent_heartbeats"
 
    agent_instance_id: Mapped[str] = mapped_column(String, primary_key=True)
    agent_type: Mapped[str] = mapped_column(String, nullable=False)
 
    last_seen: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utcnow
    )
    status: Mapped[str] = mapped_column(
        String, nullable=False, default="healthy",
        comment="healthy | degraded | dead",
    )
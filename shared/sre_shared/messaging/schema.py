"""
AgentMessage — the universal envelope for all inter-agent NATS messages.

Large Payload Rule:
    `payload` must contain only structured, typed, small fields (IDs, enums,
    scores, short strings). It must NEVER carry raw Kubernetes pod logs,
    `kubectl describe` output, full cluster snapshots, or any other large
    blob. NATS JetStream has a 1MB default message size limit, and stuffing
    large data into every hop degrades bus performance for all consumers.

    Agents needing full incident context (logs, describe output, cluster
    state, prior diagnosis) must fetch it from Postgres using `incident_id`.
    Postgres is the single source of truth for accumulated incident state;
    NATS messages exist only to signal that state has changed and where to
    look it up.
"""


from pydantic import BaseModel, Field, ConfigDict, model_validator

from typing import Any, Dict
from uuid import uuid4, UUID
from datetime import datetime, UTC

HEARTBEAT_TYPE = "heartbeat"
class AgentMessage(BaseModel):
    id: UUID = Field(
        default_factory=uuid4, 
        description="unique message identifier for this specific message."
    )
    incident_id: UUID | None  = Field(
        default=None, 
        description=(
            "ID linking this message to specific incident."
            "Required - for all message types except heartbeats"
            "never auti-generated, since it must match a real Incident row in database."
        )
    )
    correlation_id: UUID = Field(
        default_factory=uuid4, 
        description=(
            "Links all messages that belong to the same processing chain (logic flow)"
            "Distinct from "
            "incident_id: a single incident can span multiple chains over its lifetime"
            " (retries, escalations)"
        )
    )
    type: str = Field(
        ..., 
        description="the event or message type eg: 'diagnosis_complete'"
    )
    
    source_agent: str = Field(
        ..., 
        description="Name or identifier of the producer Agent"
    )
    
    payload: Dict[str, Any] = Field(
        default_factory=dict, 
        description=(
            "Structured message data."
            "See Large Payload Rule in module "
        )
    )
    
    timestamp: datetime = Field(
        default_factory=lambda: datetime.now(UTC),
        description="Timestamp when the message was generated"
    )

    @model_validator(mode="after")
    def check_incident_id_required_for_non_heartbeat(self):
        if self.type != HEARTBEAT_TYPE and self.incident_id is None:
            raise ValueError(f"incident_id is required for message type '{self.type}'")
        return self

    model_config = ConfigDict(frozen=True)

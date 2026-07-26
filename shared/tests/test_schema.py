"""
Unit tests for 'AgentMessage' schema

Tests -
- Minimal valid construction produces correct defaults
- incident_id coerces a valid UUID string into a UUID object
- incident_id is required for non-heartbeat types (enforced via model_validator)
- incident_id may be omitted for heartbeat messages
- payload accepts arbitrary structured dicts
- Serialization round-trips through JSON unchanged
- correlation_id is independently generated per message
- correlation_id/incident_id can be explicitly carried forward across messages
- incident_id, type, and source_agent are all required fields
- incident_id rejects malformed (non-UUID) strings
- payload rejects non-dict input

"""

from uuid import UUID, uuid4

import pytest
from pydantic import ValidationError

from typing import Any

from sre_shared.messaging.schema import AgentMessage
HEARTBEAT_TYPE = "heartbeat"

def make_message(**overrides) -> AgentMessage:
    """
    Builds a minimal valid AgentMessage with sensible defaults for all
    required fields. Pass keyword overrides to replace any field's value
    for a specific test case (e.g. make_message(payload={...})).

    Note: cannot be used to omit a required field entirely — tests needing
    that construct AgentMessage(...) directly instead.

    """
    defaults = {
        "incident_id": uuid4(),
        "type": "diagnosis_complete",
        "source_agent": "diagnoser",
    }

    defaults.update(overrides)
    return AgentMessage(**defaults)

def make_heartbeat(**overrides) -> AgentMessage:
    """
    Builds a minimal valid heartbeat AgentMessage — the one message type
    that legitimately omits incident_id.
    """
    defaults: dict[str, Any] = {
        "msg_type": HEARTBEAT_TYPE,
        "source_agent": "observer",
    }
    defaults.update(overrides)
    return AgentMessage(**defaults)

# ===================================SUCCESS TESTS======================================

def test_minimal_valid_construction():
    """
    Defaults fire correctly on the smallest valid input: auto-generated
    id/correlation_id as UUIDs, empty payload, UTC-aware timestamp.
    """

    msg = make_message()
    assert isinstance(msg.id, UUID)
    assert isinstance(msg.correlation_id, UUID)
    assert msg.id != msg.correlation_id
    assert msg.payload == {}
    assert msg.timestamp.tzinfo is not None

def test_incident_id_accepts_valid_uuid_string():
    """
    A valid UUID-formatted string is coerced into an actual UUID object,
    not merely accepted as-is.
    """

    valid_str = str(uuid4())
    msg = make_message(incident_id=valid_str)
    assert isinstance(msg.incident_id, UUID)

def test_heartbeat_allows_missing_incident_id():
    """
    type='heartbeat' is the one case where incident_id may be omitted —
    the model_validator's conditional check must not fire here.
    """
    msg = make_heartbeat()
    assert msg.incident_id is None

def test_heartbeat_still_generates_correlation_id():
    """
    correlation_id is unconditional (default_factory=uuid4) regardless of
    message type — a heartbeat still gets a real, valid, if unused, id.
    """
    msg = make_heartbeat()
    assert isinstance(msg.correlation_id, UUID)


def test_payload_accepts_arbitrary_dict():
    """
    payload stores nested/arbitrary structured data unchanged.
    """

    data = {"confidence": 0.87, "hypotheses": ["oom", "config_drift"]}
    msg = make_message(payload=data)
    assert msg.payload == data

def test_serialization_round_trip():
    """
    model_dump_json() -> model_validate_json() reproduces an identical
    object — the exact operation nats_client.py will rely on for publish
    and subscribe.
    """

    original = make_message(payload={"foo": "bar"})
    dumped = original.model_dump_json()
    restored = AgentMessage.model_validate_json(dumped)
    assert restored == original

def test_heartbeat_serialization_round_trip():
    """
    Same round-trip guarantee, specifically for the incident_id=None case —
    confirms None survives JSON serialization rather than being dropped
    or coerced into something else.
    """
    original = make_heartbeat()
    dumped = original.model_dump_json()
    restored = AgentMessage.model_validate_json(dumped)
    assert restored == original
    assert restored.incident_id is None


def test_two_messages_have_independent_correlation_ids():
    """
    default_factory=uuid4 generates a fresh value per instance, not a
    cached or shared one.
    """

    msg1 = make_message()
    msg2 = make_message()
    assert msg1.correlation_id != msg2.correlation_id


def test_correlation_id_can_be_explicitly_carried_forward():
    """
    Simulates an agent propagating correlation_id/incident_id from a
    received message into a new one it publishes — the real-world pattern.
    """
    incoming = make_message()
    outgoing = make_message(
        incident_id=incoming.incident_id,
        correlation_id=incoming.correlation_id,
        type="diagnosis_complete",
        source_agent="diagnoser",
    )
    assert outgoing.incident_id == incoming.incident_id
    assert outgoing.correlation_id == incoming.correlation_id
    assert outgoing.id != incoming.id  # id is always unique per message

def test_message_is_immutable():
    """
    model_config = ConfigDict(frozen=True) — reassigning any field after
    construction must raise, not silently succeed.
    """
    msg = make_message()
    with pytest.raises(ValidationError):
        msg.type = "something_else"

#===================================FAILURE TESTS=======================================

def test_incident_id_required_for_non_heartbeat_types():
    """
    Omitting incident_id for any type other than 'heartbeat' raises
    ValidationError via the model_validator — a diagnosis/remediation/etc
    message with no incident_id is a real bug (it's a Postgres FK), never
    silently accepted.
    """
    with pytest.raises(ValidationError):
        AgentMessage(type="diagnosis_complete", source_agent="diagnoser")  # type: ignore[call-arg]

def test_type_is_required():
    """
    Omitting type raises ValidationError — confirms it has no default and
    cannot be silently skipped.
    """

    with pytest.raises(ValidationError):
        AgentMessage(incident_id=uuid4(), source_agent="y")  # type: ignore[call-arg]

def test_source_agent_is_required():
    """
    Omitting source_agent raises ValidationError — confirms it has no
    default and cannot be silently skipped.
    """

    with pytest.raises(ValidationError):
        AgentMessage(incident_id=uuid4(), type="x")  # type: ignore[call-arg]


def test_incident_id_rejects_invalid_uuid():
    """
    A malformed, non-UUID-parseable string raises ValidationError instead
    of being silently accepted.
    """

    with pytest.raises(ValidationError):
        make_message(incident_id="not-a-uuid")

def test_payload_rejects_non_dict():
    """
    Passing a non-dict (e.g. a list) to payload raises ValidationError.
    """

    with pytest.raises(ValidationError):
        make_message(payload=["not", "a", "dict"])

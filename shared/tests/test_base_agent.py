"""
Unit-test for BaseAgent

====================================================================
HOW to RUN
====================================================================

    pytest test_base_agent.py -v
    pytest test_base_agent.py -k heartbeat
====================================================================

====================================================================
TEST SUITE OVERVIEW
====================================================================

TestConstruction
    Verifies __init__ wires injected state correctly, and that
    agent_instance_id falls back to "{agent_type}-{hostname}" only when
    not explicitly supplied.
 
TestAbstractEnforcement
    Confirms BaseAgent cannot be instantiated directly — handle_message
    must be implemented by a concrete subclass.
 
TestStart
    Validates the startup sequence: connect, subscribe with the correct
    callback/durable_name, and spawn of the heartbeat background task.
    Bucket creation is explicitly NOT this class's job (lives in
    init_nats.py) — asserted as a negative case.
 
TestStop
    Covers graceful shutdown: heartbeat task cancellation, NATS close,
    and safe no-op behavior when stop() is called before start().
 
TestPublish
    Exercises the publish() wrapper: correct AgentMessage construction
    via build_message(), in_reply_to/incident_id forwarding, delegation
    to nats_client.publish(), and failure propagation with logging.
 
TestHeartbeatLoop
    Verifies each heartbeat tick publishes to AGENT_HEARTBEAT and writes
    to the KV bucket independently — including the specific design
    requirement that one channel failing does not stop the other.
====================================================================
"""

import asyncio
import json
import socket
from datetime import datetime, timezone
from uuid import uuid4
from typing import AsyncIterator

import pytest
from pytest_mock import MockerFixture

from sre_shared.agents.base import BaseAgent, AGENT_HEARTBEAT_BUCKET
from sre_shared.config.settings import Settings
from sre_shared.messaging.nats_client import NatsClient, build_message
from sre_shared.messaging.schema import AgentMessage, HEARTBEAT_TYPE
from sre_shared.messaging.subjects import AGENT_HEARTBEAT

INPUT_SUBJECT = "sre.test.diagnosis"
DURABLE_NAME = "test-agent-durable"

# ================================================================== #
# TestAgent - BaseAgent is abstract, tests need something            #
# instantiable                                                       #
# ================================================================== #

class _TestAgent(BaseAgent):
    agent_type = "test-agent"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.received: list[AgentMessage] = []

    async def handle_message(self, message: AgentMessage) -> None:
        self.received.append(message)

# ================================================================== #
# Fixtures and helpers                                               #
# ================================================================== #

@pytest.fixture
def settings() -> Settings:
    return Settings(
        agent_heartbeat_interval_seconds=10,
        agent_heartbeat_timeout_seconds=30,
    )

@pytest.fixture
def mock_nats_client(mocker: MockerFixture) -> NatsClient:
    nc = mocker.MagicMock(spec=NatsClient)
    nc.connect = mocker.AsyncMock()
    nc.close = mocker.AsyncMock()
    nc.subscribe = mocker.AsyncMock()
    nc.publish = mocker.AsyncMock()
    nc.kv_put = mocker.AsyncMock()
    nc.kv_get = mocker.AsyncMock()
    return nc

@pytest.fixture
def mock_logger(mocker: MockerFixture):
    logger = mocker.MagicMock()
    return logger

@pytest.fixture
def agent(settings, mock_nats_client, mock_logger) -> _TestAgent:
    return _TestAgent(
        settings=settings,
        nats_client=mock_nats_client,
        logger=mock_logger,
        input_suibject=INPUT_SUBJECT,
        durable_name=DURABLE_NAME,
        agent_instance_id="test-agent-1",
    )

@pytest.fixture
async def started_agent(agent: _TestAgent) -> AsyncIterator[_TestAgent]:

    await agent.start()
    yield agent

    if agent._heartbeat_task is not None and not agent._heartbeat_task.done():
        agent._heartbeat_task.cancel()
        try:
            await agent._heartbeat_task
        except asyncio.CancelledError:
            pass

@pytest.fixture
def agent_message() -> AgentMessage:

    return build_message(
        source_agent="test-agent",
        type="diagnosis_requested",
        incident_id=uuid4(),
        payload={"detail": "something_happened"}
    )


# ================================================================== #
# Construction                                                       #
# ================================================================== #

class TestConstruction:
    def test_wires_injected_state(self, settings, mock_nats_client, mock_logger):
        t = _TestAgent(
            settings=settings,
            nats_client=mock_nats_client,
            logger=mock_logger,
            input_subject=INPUT_SUBJECT,
            durable_name=DURABLE_NAME,
            agent_instance_id="explicit-id-1",
        )

        assert t.settings is settings
        assert t.nats_client is mock_nats_client
        assert t.logger is mock_logger
        assert t.input_subject == INPUT_SUBJECT
        assert t.durable_name == DURABLE_NAME
        assert t.agent_instance_id == "explicit-id-1"
        assert t._running is False
        assert t._heartbeat_task is None

    def test_agent_instance_id_falls_back_to_type_and_hostname(
            self, settings, mock_nats_client, mock_logger
    ):
        t = _TestAgent(
            settings=settings,
            nats_client=mock_nats_client,
            logger=mock_logger,
            input_subject=INPUT_SUBJECT,
            durable_name=DURABLE_NAME,
            # agent_instance_id="explicit-id-1",
        )

        assert t.agent_instance_id == f"test-agent-{socket.gethostname()}"

    def test_explicit_instance_id_is_not_overriden(
            self, settings, mock_nats_client, mock_logger
    ):
        t = _TestAgent(
                settings=settings,
                nats_client=mock_nats_client,
                logger=mock_logger,
                input_subject=INPUT_SUBJECT,
                durable_name=DURABLE_NAME,
                agent_instance_id="pod-diagnoser-7213Df",
            )

        assert t.agent_instance_id == "pod-diagnoser-7213Df"

# ================================================================== #
# Abstract enforcement                                               #
# ================================================================== #

class TestAbstractEnforcement:
    def test_cannot_instantiate_base_agent_directly(
            self, settings, mock_nats_client, mock_logger
    ):
        with pytest.raises(TypeError):
            BaseAgent( # type: ignore
                settings=settings,
                nats_client=mock_nats_client,
                logger=mock_logger,
                input_subject=INPUT_SUBJECT,
                durable_name=DURABLE_NAME,
                agent_instance_id="base-agent-1"
            )

    def test_subclass_missing_handle_message_cannot_instantiate(
                self, settings, mock_nats_client, mock_logger
        ):
            class _Incomplete(BaseAgent):
                agent_type = "incomplete"

            with pytest.raises(TypeError):
                _Incomplete( # type: ignore
                    settings=settings,
                    nats_client=mock_nats_client,
                    logger=mock_logger,
                    input_subject=INPUT_SUBJECT,
                    durable_name=DURABLE_NAME,
                    agent_instance_id="base-agent-1"
                )


# ================================================================== #
# Start                                                              #
# ================================================================== #

class TestStart:
    async def test_start_connect_nats(self, agent, mock_nats_client):
        await agent.start()
        mock_nats_client.connect.assert_awaited_once()

    async def test_start_does_not_create_kv_bucket(self, agent, mock_nats_client):
        await agent.start()
        assert not hasattr(mock_nats_client, "ensure_kv_bucket") or not mock_nats_client.ensure_kv_bucket.called

    async def test_start_subscribes_with_correct_args(self, agent, mock_nats_client):
        await agent.start()

        mock_nats_client.subscribe.assert_awaited_once()
        call_kwargs = mock_nats_client.subscribe.call_args.kwargs
        assert call_kwargs["subject"] == INPUT_SUBJECT
        assert call_kwargs["durable_name"] == DURABLE_NAME
        assert call_kwargs["handler"] == agent.handle_message

    async def test_start_spawns_heartbeat_task(self, started_agent):
        assert started_agent._heartbeat_task is not None
        assert isinstance(started_agent._heartbeat_task, asyncio.Task)
        assert not started_agent._heartbeat_task.done()
        assert started_agent._running is True

    async def test_start_connects_before_subscribing(self,agent, mock_nats_client):
        call_order = []
        mock_nats_client.connect.side_effect = lambda: call_order.append("connect")
        mock_nats_client.subscribe.side_effect = lambda **_: call_order.append("subscribe")

        await agent.start()

        assert call_order == ["connect", "subscribe"]

# ================================================================== #
# Stop                                                               #
# ================================================================== #

class TestStop:
    async def test_start_without_start_is_safe(self, agent, mock_nats_client):
        await agent.stop()
        mock_nats_client.close.assert_awaited_once()

    async def test_stop_cancels_heartbeat_task(self, started_agent, mock_nats_client):
        task = started_agent._heartbeat_task
        assert not task.done()

        await started_agent.stop()

        assert task.cancelled() or task.done()
        assert started_agent._running is False

    async def test_stop_nats_connection(self, started_agent, mock_nats_client):
        await started_agent.stop()
        mock_nats_client.close.assert_awaited_once()

    async def test_stop_does_not_raise_when_heartbeat_already_cancelled(self, started_agent):
        await started_agent.stop()

        await started_agent.stop()

# ================================================================== #
# Publish                                                            #
# ================================================================== #

class TestPublish:
    async def test_publish_builds_agent_message_and_delegates(self, agent, mock_nats_client):
        await agent.publsh(
            "sre.diagnosis.complete",
            "diagnosis_complete",
            {"diagnosis": "OOMKilled"},
        )

        mock_nats_client.publsh.assert_awaited_once()
        subject_arg = mock_nats_client.publish.call_args.args[0]
        message_arg = mock_nats_client.publish.call_args.args[1]

        assert subject_arg == "sre.diagnosis.complete"
        assert isinstance(message_arg, AgentMessage)
        assert message_arg.type == "diagnosis_complete"
        assert message_arg.payload == {"diagnosis": "OOMKilled"}
        assert message_arg.source_agent == agent.agent_instance_id

    async def test_publish_carries_forward_correlation_id_and_incident_id(
            self, agent, mock_nats_client, agent_message):
        await agent.publish(
            "sre.diagnosis.complete",
            "diagnosis_complete",
            {"diagnosis": "OOMKilled"},
            in_reply_to=agent_message,
        )

        message_arg = mock_nats_client.publish.call_args.args[1]
        assert message_arg.correlation_id == agent_message.correlation_id
        assert message_arg.incident_id == agent_message.incident_id

    async def test_publish_explicit_incident_id_overrides_in_reply_to(
        self, agent, mock_nats_client, agent_message
    ):
        override_id = uuid4()
        await agent.publish(
            "sre.diagnosis.complete",
            "diagnosis_complete",
            {},
            incident_id=override_id,
            in_reply_to=agent_message,
        )
 
        message_arg = mock_nats_client.publish.call_args.args[1]
        assert message_arg.incident_id == override_id
 
    async def test_publish_without_in_reply_to_has_no_incident_id(
        self, agent, mock_nats_client
    ):
        await agent.publish("sre.agent.heartbeat", "heartbeat", {})
 
        message_arg = mock_nats_client.publish.call_args.args[1]
        assert message_arg.incident_id is None
 
    async def test_publish_failure_logs_and_reraises(
        self, agent, mock_nats_client, mock_logger
    ):
        mock_nats_client.publish.side_effect = OSError("nats down")
 
        with pytest.raises(OSError, match="nats down"):
            await agent.publish("sre.diagnosis.complete", "diagnosis_complete", {})
 
        mock_logger.error.assert_called_once()
        assert mock_logger.error.call_args.args[0] == "publish_failed"


# ================================================================== #
# HeartBeat                                                          #
# ================================================================== #

class TestHeartbeatLoop:
    async def test_single_tick_publishes_heartbeat_message(
        self, agent, mock_nats_client, mocker: MockerFixture
    ):
        # Force exactly one loop iteration then stop
        mocker.patch(
            "asyncio.sleep",
            new=mocker.AsyncMock(side_effect=lambda *_: setattr(agent, "_running", False)),
        )
        agent._running = True
 
        await agent._heartbeat_loop()
 
        mock_nats_client.publish.assert_awaited_once()
        subject_arg = mock_nats_client.publish.call_args.args[0]
        message_arg = mock_nats_client.publish.call_args.args[1]
        assert subject_arg == AGENT_HEARTBEAT
        assert message_arg.type == HEARTBEAT_TYPE
        assert message_arg.payload["agent_instance_id"] == agent.agent_instance_id
        assert message_arg.payload["agent_type"] == agent.agent_type
        assert message_arg.payload["status"] == "healthy"
 
    async def test_single_tick_writes_to_kv_bucket(
        self, agent, mock_nats_client, mocker: MockerFixture
    ):
        mocker.patch(
            "asyncio.sleep",
            new=mocker.AsyncMock(side_effect=lambda *_: setattr(agent, "_running", False)),
        )
        agent._running = True
 
        await agent._heartbeat_loop()
 
        mock_nats_client.kv_put.assert_awaited_once()
        call_args = mock_nats_client.kv_put.call_args.args
        assert call_args[0] == AGENT_HEARTBEAT_BUCKET
        assert call_args[1] == agent.agent_instance_id
 
        stored_payload = json.loads(call_args[2])
        assert stored_payload["agent_instance_id"] == agent.agent_instance_id
        assert stored_payload["status"] == "healthy"
 
    async def test_nats_publish_failure_does_not_block_kv_write(
        self, agent, mock_nats_client, mock_logger, mocker: MockerFixture
    ):
        mocker.patch(
            "asyncio.sleep",
            new=mocker.AsyncMock(side_effect=lambda *_: setattr(agent, "_running", False)),
        )
        agent._running = True
        mock_nats_client.publish.side_effect = OSError("nats down")
 
        await agent._heartbeat_loop()
 
        mock_logger.warning.assert_any_call("heartbeat_publish_failed", exc_info=True)
        mock_nats_client.kv_put.assert_awaited_once()
 
    async def test_kv_write_failure_does_not_block_nats_publish(
        self, agent, mock_nats_client, mock_logger, mocker: MockerFixture
    ):
        mocker.patch(
            "asyncio.sleep",
            new=mocker.AsyncMock(side_effect=lambda *_: setattr(agent, "_running", False)),
        )
        agent._running = True
        mock_nats_client.kv_put.side_effect = OSError("nats down")
 
        await agent._heartbeat_loop()
 
        mock_nats_client.publish.assert_awaited_once()
        mock_logger.warning.assert_any_call("heartbeat_kv_write_failed", exc_info=True)
 
    async def test_loop_sleeps_for_configured_interval(
        self, agent, settings, mocker: MockerFixture
    ):
        mock_sleep = mocker.patch(
            "asyncio.sleep",
            new=mocker.AsyncMock(side_effect=lambda *_: setattr(agent, "_running", False)),
        )
        agent._running = True
 
        await agent._heartbeat_loop()
 
        mock_sleep.assert_awaited_once_with(settings.agent_heartbeat_interval_seconds)
 
    async def test_loop_does_not_run_when_not_started(
        self, agent, mock_nats_client
    ):
        """_running defaults to False — a loop entered without start() should
        exit immediately without publishing anything."""
        assert agent._running is False
        await agent._heartbeat_loop()
        mock_nats_client.publish.assert_not_awaited()
        mock_nats_client.kv_put.assert_not_awaited()
    
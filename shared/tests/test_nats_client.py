"""
Unit test suite for NatsClient (pytest-mock edition).

=============================================================================
HOW TO RUN
=============================================================================
    pytest test_nats_client.py -v            # run everything
    pytest test_nats_client.py -k publish    # run only tests with "publish" in the name

Ensure your pyproject.toml or pytest.ini has:
    [tool.pytest.ini_options]
    asyncio_mode = "auto"
=============================================================================

=============================================================================
TEST SUITE OVERVIEW
=============================================================================

TestInit
    Verifies that a fresh NatsClient starts in a disconnected, uninitialized
    state with no partial connection artifacts.

TestEnsureConnectedGuard
    Acts as a gatekeeper suite, ensuring every public operation (publish,
    subscribe, request) rejects calls when the client is disconnected or has
    lost its connection.

TestConnect
    Validates successful connection state setup, proper forwarding of
    settings/callbacks to nats.connect, graceful failure handling, and cleanup
    of the closing flag.

TestClose
    Covers orderly shutdown logic: draining open connections, safe no-op
    behavior when already closed or never connected, and idempotency.

TestPublish
    Exercises the publish retry policy with exponential backoff, serialization
    of AgentMessage payloads, and boundary cases like zero retries or exhausted
    attempts.

TestSubscribe
    Confirms that consumer registration passes the correct JetStream
    configuration—including durable names, delivery policies, and safe
    defaults—through to the underlying subscription.

TestOnMessage
    Defines the message-processing contract: valid messages are acked after
    handler success, handler failures trigger naks, malformed or schema-invalid
    payloads are terminated, and contextvars are always cleaned up.

TestRequest
    Tests the request-reply pattern: successful round-trip parsing, timeout and
    malformed-reply degradation to None, and propagation of unexpected transport
    errors.

TestReplayFrom
    Ensures replay_from() is a thin, correct delegate to subscribe() with
    DeliverPolicy.BY_START_TIME and the supplied start time.

TestConnectionCallbacks
    Validates the client's event-handler hooks: quiet intentional shutdowns,
    noisy unexpected closures, and non-raising logging-only callbacks for
    disconnects, reconnects, and errors.

TestBuildMessage
    Pure-function suite verifying that build_message() mints fresh identities,
    preserves UTC timestamps, and correctly inherits correlation IDs for reply
    messages while generating new ones.
=============================================================================
"""

import json
from datetime import datetime, UTC
from uuid import UUID, uuid4
from typing import AsyncIterator

import pytest
from pytest_mock import MockerFixture
from nats.js.api import AckPolicy, DeliverPolicy
from nats.errors import TimeoutError as NatsTimeoutError

from sre_shared.config.settings import Settings
from sre_shared.messaging.nats_client import NatsClient, build_message
from sre_shared.messaging.schema import AgentMessage

SUBJECT = "test.check.message"

# ============================================================================= #
# Fixtures and Helpers                                                          #
# ============================================================================= #

@pytest.fixture
def settings() -> Settings:
    "Real settings object with small, deterministic test values."
    return Settings(
        nats_url="nats://127.0.0.1:4222",
        nats_max_reconnect_attempts=3,
        nats_reconnect_time_wait=0.5,
        nats_ack_wait_seconds=30,
        nats_max_deliver=5,
    )

@pytest.fixture
def agent_message() -> AgentMessage:
    """A valid message, built through the same factory production code uses."""
    return build_message(
        source_agent="test-agent",
        type="test.event",
        incident_id=uuid4(),
        payload={"detail": "something_happened"}
    )

@pytest.fixture
def client(settings:Settings) -> NatsClient:
    """A client that has NOT connected yet."""
    return NatsClient(settings=settings)

@pytest.fixture
def mock_js(mocker: MockerFixture):
    """Fake JetStream context. publish/subscribe are awaited -> AsyncMock."""
    js = mocker.MagicMock()
    js.publish = mocker.AsyncMock()
    js.subscribe = mocker.AsyncMock()
    return js

@pytest.fixture
def mock_nc(mocker: MockerFixture, mock_js):
    """Fake NATS connection in the 'healthy, connected' state."""
    nc = mocker.MagicMock()
    nc.is_connected = True
    nc.is_closed = False
    nc.jetstream = mocker.MagicMock(return_value=mock_js)
    nc.request = mocker.AsyncMock()
    nc.drain = mocker.AsyncMock()
    return nc

@pytest.fixture
async def connected_client(client, mock_nc, mocker) -> AsyncIterator[NatsClient]:
    """A client that has gone through connect() against a mocked nats.connect."""
    mocker.patch("nats.connect", new=mocker.AsyncMock(return_value=mock_nc))
    await client.connect()
    yield client

@pytest.fixture
def make_msg(mocker: MockerFixture):
    """Factory to build a fake nats.aio.msg.Msg using pytest-mock."""
    def _make_msg(data: bytes):
        msg = mocker.MagicMock()
        msg.data = data
        msg.ack = mocker.AsyncMock()
        msg.nak = mocker.AsyncMock()
        msg.term = mocker.AsyncMock()
        return msg
    return _make_msg


# ============================================================================= #
# Construction & the _ensure_connected guard                                    #
# ============================================================================= #

class TestInit:
    async def test_init_starts_disconnected(self, client, settings):
        assert client.settings is settings
        assert client._nc is None
        assert client._js is None
        assert client._closing is False

class TestEnsureConnectionGuard:
    async def test_publish_before_connect_raises(self, client, agent_message):
        with pytest.raises(RuntimeError, match="connect"):
            await client.publish(SUBJECT, agent_message)
    
    async def test_subscribe_before_connect_raises(self, client, mocker):
        handler = mocker.AsyncMock()
        with pytest.raises(RuntimeError, match="connect"):
            await client.subscribe(SUBJECT, handler)
    
    async def test_request_before_connect_raises(self, client, agent_message):
        with pytest.raises(RuntimeError, match="connect"):
            await client.request(SUBJECT, agent_message, timeout=1.0)
    
    async def test_publish_after_connection_lost_raises(self, connected_client, mock_nc, mock_js, agent_message):
        mock_nc.is_connected = False
        mock_nc.is_closed = True
        with pytest.raises(RuntimeError, match="connect"):
            await connected_client.publish(SUBJECT, agent_message)
        mock_js.publish.assert_not_awaited()

# ============================================================================== #
# connect() / close()                                                            #
# ============================================================================== #

class TestConnect:
    async def test_connect_success_stores_state(self, client, mock_nc, mock_js, mocker: MockerFixture):
        mocker.patch("nats.connect", new=mocker.AsyncMock(return_value=mock_nc))

        await client.connect()

        assert client._nc is mock_nc
        assert client._js is mock_js
        mock_nc.jetstream.assert_called_once()
    
    async def test_connect_pases_settings_and_callbacks(self, client, mock_nc, settings: Settings, mocker:MockerFixture):
        mock_connect = mocker.patch("nats.connect", new=mocker.AsyncMock(return_value=mock_nc))

        await client.connect()

        mock_connect.assert_awaited_once()
        assert mock_connect.call_args.args[0] == settings.nats_url
        assert mock_connect.call_args.kwargs["max_reconnect_attempts"] == settings.nats_max_reconnect_attempts
        assert mock_connect.call_args.kwargs["reconnect_time_wait"] == settings.nats_reconnect_time_wait
        assert mock_connect.call_args.kwargs["error_cb"] == client._on_error
        assert mock_connect.call_args.kwargs["disconnected_cb"] == client._on_disconnect
        assert mock_connect.call_args.kwargs["reconnected_cb"] == client._on_reconnect
        assert mock_connect.call_args.kwargs["closed_cb"] == client._on_close
    
    async def test_connect_failure_reraises(self, client, mocker):
        mocker.patch("nats.connect", new=mocker.AsyncMock(side_effect=OSError("refused")))

        with pytest.raises(OSError, match="refused"):
            await client.connect()
        
        assert client._nc is None

    async def test_connect_reset_closing_flag(self, client, mock_nc, mocker):
        
        client._closing = True

        mocker.patch("nats.connect", new=mocker.AsyncMock(return_value=mock_nc))
        await client.connect()

        assert client._closing is False

class TestClose:
    async def test_close_drain_open_connection(self, connected_client, mock_nc):

        await connected_client.close()

        mock_nc.drain.assert_awaited_once()
        assert connected_client._closing is True
    
    async def test_close_without_connection_is_safe(self, client):
        await client.close()

        assert client._closing is True
    
    async def test_close_already_closed_skips_drain(self, connected_client, mock_nc):

        mock_nc.is_closed = True
        mock_nc.is_connected = False

        await connected_client.close()

        mock_nc.drain.assert_not_awaited()

# ============================================================================== #
# publish()                                                                      #
# ============================================================================== #

class TestPublish:
    async def test_publish_sends_serialized_payload(self, connected_client, mock_js, agent_message):
        await connected_client.publish(SUBJECT, agent_message)

        mock_js.publish.assert_awaited_once()
        assert mock_js.publish.call_args.kwargs["subject"] == SUBJECT
        assert mock_js.publish.call_args.kwargs["payload"] == agent_message.model_dump_json().encode("utf-8")
    
    async def test_publish_retries_with_exponential_backoffs(
            self,
            connected_client: NatsClient,
            mock_js,
            agent_message:AgentMessage,
            settings:Settings,
            mocker:MockerFixture,
    ):
        mock_js.publish.side_effect = [OSError("boom"), OSError("boom"), None]
        mock_sleep = mocker.patch("asyncio.sleep", new=mocker.AsyncMock())
        await connected_client.publish(SUBJECT, agent_message)

        assert mock_js.publish.await_count == 3
        assert mock_sleep.await_args_list[0].args == (settings.nats_reconnect_time_wait,)
        assert mock_sleep.await_args_list[1].args == (settings.nats_reconnect_time_wait * 2,)
    
    async def test_publish_raises_after_retries_exhausted(
            self,
            connected_client,
            mock_js,
            agent_message,
            settings,
            mocker: MockerFixture,
    ):
        mock_js.publish.side_effect = OSError("persistent")
        mock_sleep = mocker.patch("asyncio.sleep", new=mocker.AsyncMock())
        with pytest.raises(OSError, match="persistent"):
            await connected_client.publish(SUBJECT, agent_message)
        
        assert mock_js.publish.await_count == 4
        assert mock_sleep.await_count == 3
    
    async def test_publish_max_retries_zero_makes_single_attempt(
            self,
            connected_client,
            mock_js,
            agent_message,
            mocker: MockerFixture,
    ):
        mock_js.publish.side_effect = OSError("persistent")
        mock_sleep = mocker.patch("asyncio.sleep", new=mocker.AsyncMock())

        with pytest.raises(OSError, match="persistent"):
            await connected_client.publish(SUBJECT, agent_message, max_retries=0)
        
        assert mock_js.publish.await_count == 1
        mock_sleep.assert_not_awaited()

# =========================================================================== #
# subscribe() - consumer - wiring                                             #
# =========================================================================== #

class TestSubscribe:
    async def test_subscribe_registers_consum_and_returns_subscription(
            self,
            connected_client,
            mock_js,
            settings,
            mocker: MockerFixture
    ):
        sentinel_object = object()
        handler = mocker.AsyncMock()
        mock_js.subscribe.return_value = sentinel_object
        result = await connected_client.subscribe(
            SUBJECT,
            handler,
            durable_name="durable-1",
            deliver_policy=DeliverPolicy.LAST,
        )

        assert result is sentinel_object
        mock_js.subscribe.assert_awaited_once()
        config = mock_js.subscribe.call_args.kwargs["config"]
        assert config.durable_name == "durable-1"
        assert config.deliver_policy == DeliverPolicy.LAST
        assert config.ack_policy == AckPolicy.EXPLICIT
    
    async def test_subscribe_uses_safe_defaults(
            self,
            connected_client,
            mock_js,
            mocker: MockerFixture,
    ):
        handler = mocker.AsyncMock()

        await connected_client.subscribe(SUBJECT, handler)

        config = mock_js.subscribe.call_args.kwargs["config"]
        
        assert config.durable_name is None
        assert config.deliver_policy == DeliverPolicy.ALL
        assert config.opt_start_time is None

# ================================================================================ #
# The _on_message closure - ack / nak / term policy                                #
# ================================================================================ #

class TestOnMessage:
    async def test_valid_message_calls_handler_and_acks(
            self,
            connected_client,
            mock_js,
            agent_message,
            mocker: MockerFixture,
            make_msg,
    ):
        handler = mocker.AsyncMock()
        await connected_client.subscribe(SUBJECT, handler)
        cb = mock_js.subscribe.call_args.kwargs["cb"]
        msg = make_msg(agent_message.model_dump_json().encode("utf-8"))

        await cb(msg)

        handler.assert_awaited_once()
        assert handler.await_args is not None

        received = handler.await_args.args[0]

        assert isinstance(received, AgentMessage)
        assert str(received.id) == str(agent_message.id)
        msg.ack.assert_awaited_once()
        msg.nak.assert_not_awaited()
        msg.term.assert_not_awaited()
    
    async def test_handler_failure_naks_and_does_not_raise(
            self,
            connected_client,
            mock_js,
            agent_message,
            mocker: MockerFixture,
            make_msg,
    ):
        handler = mocker.AsyncMock(side_effect=ValueError("boom"))
        await connected_client.subscribe(SUBJECT, handler)
        cb = mock_js.subscribe.call_args.kwargs["cb"]
        msg = make_msg(agent_message.model_dump_json().encode("utf-8"))
        await cb(msg)

        msg.nak.assert_awaited_once()
        msg.ack.assert_not_awaited()
        msg.term.assert_not_awaited()
    
    async def test_malformed_json_is_terminated(
            self,
            connected_client,
            mock_js,
            mocker: MockerFixture,
            make_msg
    ):
        handler = mocker.AsyncMock(side_effect=ValueError("boom"))
        await connected_client.subscribe(SUBJECT, handler)
        cb = mock_js.subscribe.call_args.kwargs["cb"]
        msg = make_msg(b"{not valid json}")
        await cb(msg)

        msg.term.assert_awaited_once()
        handler.assert_not_awaited()
        msg.ack.assert_not_awaited()
        msg.nak.assert_not_awaited()
    
    async def test_schema_invalid_is_terminated(
            self,
            connected_client,
            mock_js,
            mocker: MockerFixture,
            make_msg,
    ): 
        handler = mocker.AsyncMock()
        await connected_client.subscribe(SUBJECT, handler)
        cb = mock_js.subscribe.call_args.kwargs["cb"]
        msg = make_msg(json.dumps({"unexpected": "shape"}).encode("utf-8"))
        await cb(msg)

        msg.term.assert_awaited_once()
        handler.assert_not_awaited()
        msg.ack.assert_not_awaited()
        msg.nak.assert_not_awaited()

    async def test_contextvars_bound_then_unbound_even_on_failure(
            self,
            connected_client,
            mock_js,
            agent_message,
            mocker: MockerFixture,
            make_msg,
    ):
        handler = mocker.AsyncMock(side_effect=ValueError("boom"))
        mock_bind = mocker.patch("structlog.contextvars.bind_contextvars")
        mock_unbind = mocker.patch("structlog.contextvars.unbind_contextvars")

        await connected_client.subscribe(SUBJECT, handler)
        cb = mock_js.subscribe.call_args.kwargs["cb"]
        msg = make_msg(agent_message.model_dump_json().encode("utf-8"))
        await cb(msg)

        mock_bind.assert_called_once_with(correlation_id=str(agent_message.correlation_id), incident_id=str(agent_message.incident_id))
        mock_unbind.assert_called_once_with("correlation_id", "incident_id")
        msg.nak.assert_awaited_once()
    
    async def test_contextvars_bind_omits_incident_id_for_heartbeat(
        self,
        connected_client,
        mock_js,
        mocker: MockerFixture,
        make_msg,
    ):
        """
        A heartbeat message (incident_id=None) must not have incident_id
        bound into contextvars at all — binding str(None) would pollute logs
        with a fake-looking value instead of correctly omitting the key.
        """
        heartbeat_msg = build_message(
            source_agent="test-agent",
            type="heartbeat",
            payload={"status": "healthy"},
        )
        handler = mocker.AsyncMock()
        mock_bind = mocker.patch("structlog.contextvars.bind_contextvars")
        mock_unbind = mocker.patch("structlog.contextvars.unbind_contextvars")

        await connected_client.subscribe(SUBJECT, handler)
        cb = mock_js.subscribe.call_args.kwargs["cb"]
        msg = make_msg(heartbeat_msg.model_dump_json().encode("utf-8"))
        await cb(msg)

        mock_bind.assert_called_once_with(correlation_id=str(heartbeat_msg.correlation_id))
        # note: incident_id key must be ABSENT from the call, not present-as-None
        mock_unbind.assert_called_once_with("correlation_id", "incident_id")
        msg.ack.assert_awaited_once()

# ========================================================================================== #
# request() - request-reply                                                                  #
# ========================================================================================== #

class TestRequest:
    async def test_request_returns_parsed_reply(
            self,
            connected_client,
            mock_nc,
            agent_message,
            make_msg,
            mocker: MockerFixture
    ):
        reply = build_message(source_agent="test", type="reply", incident_id=uuid4(), payload={})
        mock_nc.request = mocker.AsyncMock(return_value=make_msg(reply.model_dump_json().encode("utf-8")))

        result = await connected_client.request(SUBJECT, agent_message, timeout=1.5)

        assert isinstance(result, AgentMessage) and str(result.id) == str(reply.id)
        assert mock_nc.request.call_args.args[0] == SUBJECT
        assert mock_nc.request.call_args.args[1] == agent_message.model_dump_json().encode("utf-8")
        assert mock_nc.request.call_args.kwargs["timeout"] == 1.5
    
    async def test_request_timeout_returns_none(
            self,
            connected_client,
            mock_nc,
            agent_message,
            mocker: MockerFixture
    ):
        mock_nc.request = mocker.AsyncMock(side_effect=NatsTimeoutError)
        result = await connected_client.request(SUBJECT, agent_message, timeout=1.5)

        assert result is None
    
    @pytest.mark.parametrize("bad_payload", [b"not_json", json.dumps({"wrong": "schema"}).encode("utf-8")], ids=["invalid-json","invalid-schema"])
    async def test_request_malformed_reply_returns_none(
        self,
        connected_client,
        mock_nc,
        agent_message,
        mocker: MockerFixture,
        make_msg,
        bad_payload
    ):
        mock_nc.request = mocker.AsyncMock(return_value=make_msg(bad_payload))
        result = await connected_client.request(SUBJECT, agent_message, timeout=1.5)

        assert result is None
    
    async def test_request_unexoected_error_propogate(
            self,
            connected_client,
            mock_nc,
            agent_message,
            mocker: MockerFixture
    ): 
        mock_nc.request = mocker.AsyncMock(side_effect=OSError("socket died"))

        with pytest.raises(OSError, match="socket died"):
            await connected_client.request(SUBJECT, agent_message, timeout=1.0)

# ======================================================================================================= #
# replay_from()                                                                                           #
# ======================================================================================================= #

class TestReplayFrom:
    async def test_replay_from_delegates_to_subscribe(
            self,
            connected_client,
            mocker: MockerFixture
    ):
        sentinel_object = object()

        start_time = datetime.now(UTC)
        handler = mocker.AsyncMock()

        mock_subscribe = mocker.patch.object(connected_client, "subscribe", new=mocker.AsyncMock(return_value=sentinel_object))

        result = await connected_client.replay_from(SUBJECT, start_time, handler)

        assert result is sentinel_object
        mock_subscribe.assert_awaited_once_with(subject=SUBJECT, handler=handler, durable_name=None, deliver_policy=DeliverPolicy.BY_START_TIME, opt_start_time=start_time)

# ================================================================================== #
# Connection event callbacks                                                         #
# ================================================================================== #

class TestConnectionCallbacks:
    async def test_on_close_intentional_shutdown_is_quiet(
            self,
            client,
            mocker: MockerFixture
    ):
        client._closing = True
        mock_logger = mocker.patch("sre_shared.messaging.nats_client.logger")
        await client._on_close()

        mock_logger.error.assert_not_called()
    
    async def test_on_close_unexpected_logs_error(
            self,
            client,
            mocker: MockerFixture
    ):
        client._closing = False
        mock_logger = mocker.patch("sre_shared.messaging.nats_client.logger")
        await client._on_close()

        mock_logger.error.assert_called_once_with("nats_connection_closed_unexpectedly")
    
    async def test_log_only_callbacks_do_not_raise(
            self,
            client: NatsClient,
    ):
        await client._on_disconnect()
        await client._on_reconnect()
        await client._on_error(Exception("test"))


# ================================================================================== #
# build_message() - pure function, no mocks at all                                   #
# ================================================================================== #

class TestBuildMessage:
    def test_creates_message_with_fresh_identity(self):
        m1 = build_message(source_agent="obs", type="evt", incident_id=uuid4(), payload={})
        m2 = build_message(source_agent="obs", type="evt", incident_id=m1.incident_id, payload={})

        assert m1.type == "evt"
        assert m1.payload == {}

        assert isinstance(m1.id, UUID)
        assert isinstance(m1.correlation_id, UUID)
        assert m1.timestamp.tzinfo is not None
        assert m1.id != m2.id and m1.correlation_id != m2.correlation_id
    
    def test_reply_inherits_correlation_but_not_id(self):
        
        parent = build_message(source_agent="obs", type="evt", incident_id=uuid4(), payload={})
        reply = build_message(source_agent="res", type="reply", incident_id=uuid4(), payload={}, in_reply_to=parent)

        assert reply.correlation_id == parent.correlation_id
        assert reply.id != parent.id
        assert reply.timestamp >= parent.timestamp

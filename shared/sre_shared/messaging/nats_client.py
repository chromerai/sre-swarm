
import nats
import asyncio
import math
import json
import structlog

from sre_shared.logging.logger import get_logger, bind_context
from sre_shared.messaging.schema import AgentMessage
from nats.js.api import ConsumerConfig, DeliverPolicy, AckPolicy, KeyValueConfig
from nats.js.kv import KeyValue
from nats.js.errors import KeyNotFoundError, KeyDeletedError
from nats.aio.client import Client as NatsConnection
from nats.aio.msg import Msg
from nats.js import JetStreamContext
from nats.errors import TimeoutError as NatsTimeoutError
from uuid import uuid4, UUID
from sre_shared.config.settings import Settings

from collections.abc import Callable, Awaitable
from datetime import datetime, UTC
from typing import Any

from pydantic import ValidationError

logger = get_logger(__name__)

MessageHandler = Callable[[AgentMessage], Awaitable[None]]

class NatsClient: 
    """
    High-level async wrapper around nats-py with JetStream support.

    Usage::
        # Settings
        settings = Settings()

        #Log configuration
        configure_logging(settings)

        #Setting up the client
        client = NatsClient(settings)
        await client.connect()

        # Publish
        await client.publish("agents.observer.anomalies", message)

        # Subscribe (durable consumer)
        await client.subscribe("agents.observer.anomalies", handler, durable_name="agents-observer-anomalies")

        # KV (e.g. agent heartbeats) — bucket must exist before put/get.
        # Bucket creation lives in scripts/init_nats.py, not here — same
        # centralized-creation pattern as STREAM_CONFIGS.
        await client.kv_put("agent_heartbeats", "diagnoser-1", b'{"status": "healthy"}')
        value = await client.kv_get("agent_heartbeats", "diagnoser-1")  # None if expired/missing

        await client.close()
    """

    def __init__(self, settings: Settings) -> None:
        self.settings = settings

        self._nc: NatsConnection | None = None
        self._js: JetStreamContext | None = None
        self._kv_buckets: dict[str, KeyValue] = {}

        self._closing = False
    
    # ============================================================= #
    # Connection Management                                         #
    # ============================================================= #

    async def connect(self) -> None:
        """
        Establish connection to NATS and obtain a JetStream context.

        Reconnection behavior (max attempts, wait interval) is driven by
        settings. Raises on initial connection failure — callers should
        treat this as fatal at startup.
        """

        self._closing = False

        try:
            self._nc = await nats.connect(
                self.settings.nats_url,
                max_reconnect_attempts =    self.settings.nats_max_reconnect_attempts,
                reconnect_time_wait =       self.settings.nats_reconnect_time_wait,
                error_cb =                  self._on_error,
                disconnected_cb =           self._on_disconnect,
                reconnected_cb =            self._on_reconnect,
                closed_cb =                 self._on_close,
            )

            self._js = self._nc.jetstream()
            logger.info("nats_connected", url=self.settings.nats_url)

        except Exception as exc:
            logger.error(
                "nats_connection_establishment_failed",
                error=str(exc),
                error_type=type(exc).__name__,
            )
            raise
    
    async def close(self) -> None:
        """
        Gracefully drain and close the NATS connection.
        
        Drains in-flight messages before closing. Sets an internal flag so
        the closed_cb callback can distinguish this intentional shutdown
        from an unexpected connection loss.
        """

        self._closing = True
        if self._nc and not self._nc.is_closed:
            await self._nc.drain()
            logger.info("NATS connection closed")
    
    @property
    def _is_connected(self):
        return self._nc is not None and self._nc.is_connected
    
    # ============================================================== #
    # Publish                                                        #
    # ============================================================== #

    async def publish(
            self,
            subject: str,
            message: AgentMessage,
            *,
            max_retries: int = 3,
    ):
        
        """
        Publish an AgentMessage to a JetStream subject.

        Retries with exponential backoff on transient failures.
        """
        
        self._ensure_connected()
        payload_bytes = message.model_dump_json().encode("utf-8")

        for attempt in range(max_retries + 1):
            try:
                await self._js.publish(subject=subject, payload=payload_bytes) # type: ignore[union-attr]
                logger.info(
                    "message_published",
                    subject=subject,
                    message_type=message.type,
                    incident_id=message.incident_id if message.incident_id is not None else None,
                    correlation_id=message.correlation_id,
                )
                return
            except Exception as exc:
                if attempt == max_retries:
                    logger.error(
                        "failed_to_publish_message",
                        subject=subject,
                        max_retries=max_retries + 1,
                        error=str(exc),
                        error_type=type(exc).__name__,
                    )
                    raise
                wait = self.settings.nats_reconnect_time_wait * math.pow(2, attempt)
                logger.warning(
                    "publish_retry",
                    attempt=attempt+1,
                    wait_seconds=wait,
                    error=str(exc)
                )
                await asyncio.sleep(wait)

    # ============================================================== #
    # Subscribe                                                      #
    # ============================================================== #

    async def subscribe(
            self,
            subject: str,
            handler: MessageHandler,
            *,
            durable_name: str | None = None,
            deliver_policy: DeliverPolicy = DeliverPolicy.ALL,
            opt_start_time: datetime | None = None
            
    ) -> JetStreamContext.PushSubscription:
        
        """
        Subscribe to a JetStream subject with a push consumer.

        Args:
            subject:        NATS subject string.
            handler:        Async callback receiving an AgentMessage.
            durable_name:        Durable consumer name (enables at-least-once delivery).
            deliver_policy: 'new', 'all', 'last', etc. Default: 'all'
            opt_start_time: To provide optional start time

        Returns:
            The underlying NATS push subscription (call `.unsubscribe()` to stop).
        """
        
        self._ensure_connected()

        async def _on_message(msg: Msg) -> None:
            try:
                data = json.loads(msg.data)
                agent_message = AgentMessage.model_validate(data)
    
            except (json.JSONDecodeError, ValidationError) as exc:
                #malformed payload - this will never succeed in redelivery

                logger.error(
                    "message_parse_failed",
                    subject=subject,
                    error=str(exc),
                    error_type=type(exc).__name__,
                )

                await msg.term()
                return

            ctx_kwargs = {"correlation_id": str(agent_message.correlation_id)}
            if agent_message.incident_id is not None:
                ctx_kwargs["incident_id"] = str(agent_message.incident_id)

            bind_context(**ctx_kwargs)
                        
            try:
                await handler(agent_message)
                await msg.ack()
            except Exception as exc:
                logger.error(
                    "handler_failed",
                    subject=subject,
                    incident_id=str(agent_message.incident_id) if agent_message.incident_id is not None else None,
                    correlation_id=str(agent_message.correlation_id),
                    error=str(exc)
                )
                await msg.nak()
            finally:
                structlog.contextvars.unbind_contextvars("correlation_id", "incident_id")
        
        consumer_config = ConsumerConfig(
            durable_name=durable_name,
            deliver_policy=deliver_policy,
            opt_start_time=opt_start_time,
            ack_policy=AckPolicy.EXPLICIT,
            ack_wait=self.settings.nats_ack_wait_seconds,
            max_deliver=self.settings.nats_max_deliver,
            filter_subject=subject,
        )

        subcription = await self._js.subscribe(  # type: ignore[union-attr]
            subject,
            cb=_on_message,
            config=consumer_config,
        )
        logger.info(
            "subscribed",
            subject=subject,
            durable=durable_name,
        )

        return subcription

    # ============================================================== #
    # Key-Value Store (agent heartbeats / liveness)                  #
    # ============================================================== #
    

    async def ensure_kv_bucket(
            self,
            bucket: str,
            *,
            ttl_seconds: float,
    ) -> KeyValue:
        """
        Get or create a JetStream KV bucket with the given TTL.
 
        Idempotent — safe to call on every connect(). Caches the KeyValue
        handle per bucket name so repeated calls don't re-hit JetStream.

        Caches the KeyValue handle in-process so repeated calls (or calls
        from multiple methods) don't re-hit JetStream every time.
        """

        self._ensure_connected()

        if bucket in self._kv_buckets:
            return self._kv_buckets[bucket]

        config = KeyValueConfig(bucket=bucket, ttl=ttl_seconds)
        kv = await self._js.create_key_value(config) # type: ignore[union-attr]
        self._kv_buckets[bucket] = kv

        logger.info("kv_bucket_ready", bucket=bucket, ttl=ttl_seconds)
        return kv

    async def kv_put(self, bucket: str, key: str, value: bytes) -> None:
        """
        Write/refresh a key in the given KV bucket. Each put resets that
        key's TTL clock.

        Requires the bucket to already exist (created via init_nats.py at
        cluster startup, not lazily here)
        """

        self._ensure_connected()

        kv = await self._get_kv_handle(bucket)
        await kv.put(key, value)

    async def kv_get(self, bucket: str, key: str) -> bytes | None:
        """
        Read a key from the given KV bucket.
 
        Returns None if the key doesn't exist, has expired (TTL elapsed),
        or was explicitly deleted (KeyDeletedError).
        """

        self._ensure_connected()

        kv = await self._get_kv_handle(bucket)

        try:
            entry = await kv.get(key)
            return entry.value
        except (KeyDeletedError, KeyNotFoundError):
            return None

    async def _get_kv_handle(self, bucket: str) -> KeyValue:
        """
        Return the cached KeyValue handle for a bucket, attaching to an
        already-existing bucket on first use if not yet cached in this
        process.
        """

        if bucket in self._kv_buckets:
            return self._kv_buckets[bucket]

        kv = await self._js.key_value(bucket) # type: ignore[union-attr]
        self._kv_buckets[bucket] = kv
        return kv

    
    # ============================================================== #
    # Request-Reply                                                  #
    # ============================================================== #

    async def request(
            self,
            subject: str,
            message: AgentMessage,
            timeout: float,
    ) -> AgentMessage | None:
        """
        Publish an AgentMessage and wait for a single reply (core NATS
        request-reply, not JetStream — no persistence, no redelivery).

        Returns None on timeout or on a malformed reply — callers should
        treat both identically as "no answer available" and proceed
        without it.
        """
        self._ensure_connected()

        payload_bytes = message.model_dump_json().encode("utf-8")

        try:
            raw_response = await self._nc.request(subject, payload_bytes, timeout=timeout) # type: ignore[union-attr]
        except NatsTimeoutError:
            logger.warning(
                "request_timed_out", 
                subject=subject,
                correlation_id=str(message.correlation_id), 
                timeout=timeout
            )
            return None
        
        try:
            data = json.loads(raw_response.data) 
            response_message = AgentMessage.model_validate(data)
        except (json.JSONDecodeError, ValidationError) as exc:
            logger.error(
                    "message_parse_failed",
                    subject=subject,
                    error=str(exc),
                    error_type=type(exc).__name__,
                )
            return None
        
        logger.info(
            "request_completed",
            subject=subject,
            correlation_id=str(message.correlation_id)
        )
        
        return response_message
    
    # ============================================================== #
    # Replay Helper                                                  #
    # ============================================================== #

    async def replay_from(
            self,
            subject: str,
            start_time: datetime,
            handler: MessageHandler,
    ) -> JetStreamContext.PushSubscription:
        
        """
        Wrapper over the subscribe call to include message replay from the
        start time and appropriate deliver policy.
        """
        
        return await self.subscribe(
            subject=subject,
            handler=handler,
            durable_name=None,
            deliver_policy=DeliverPolicy.BY_START_TIME,
            opt_start_time=start_time
        )


    # ============================================================== #
    # Guard Helper                                                   #
    # ============================================================== #

    def _ensure_connected(self):
        """Raise if publish/subscribe/request is called before connect()."""
        if self._js is None or not self._is_connected:
            raise RuntimeError("call await connect() before using this client")
    
    # ============================================================== #
    # Connection event callbacks                                     #
    # ============================================================== #
    
    async def _on_disconnect(self):
        """Transient disconnect — nats-py will attempt to reconnect automatically."""
        logger.warning("nats_disconnected")
    
    async def _on_reconnect(self):
        """Connection recovered after a transient disconnect."""
        logger.info("nats_reconnected")
    
    async def _on_error(self, exc):
        """Async-level NATS error not tied to a specific connection-state change."""
        logger.error("nats_error", error=str(exc))
    
    async def _on_close(self):
        """
        Fires when the connection is permanently closed — either via our
        own close() (self._closing=True, expected, no-op here) or because
        reconnection attempts were exhausted (unexpected, logged as error).
        """

        if self._closing:
            return
        else:
            logger.error("nats_connection_closed_unexpectedly")


def build_message(
        *,
        source_agent: str,
        type: str,
        incident_id: UUID | None = None,
        payload: dict[str, Any],
        in_reply_to: AgentMessage | None = None,
) -> AgentMessage:
    
    correlation_id = in_reply_to.correlation_id if in_reply_to else uuid4()
    resolved_incident_id = incident_id if incident_id is not None else (
        in_reply_to.incident_id if in_reply_to else None
    ) 

    return AgentMessage(
        id=uuid4(),
        type=type,
        incident_id=resolved_incident_id,
        payload=payload,
        source_agent=source_agent,
        timestamp=datetime.now(UTC),
        correlation_id=correlation_id,
    )
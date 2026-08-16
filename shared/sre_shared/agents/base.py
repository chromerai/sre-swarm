"""
BaseAgent -> base class for SRE Agents

All agents (Observer, Diagnoser, Remediator, Safety, Orchestrator, Learning)
inherit from BaseAgent to get:


"""

from __future__ import annotations

import abc
import asyncio
import socket
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

import json

from sre_shared.config.settings import Settings
from sre_shared.messaging.schema import AgentMessage, HEARTBEAT_TYPE
from sre_shared.messaging.subjects import AGENT_HEARTBEAT
from sre_shared.messaging.nats_client import NatsClient, build_message
from sre_shared.messaging.streams import AGENT_HEARTBEAT_BUCKET

class BaseAgent(abc.ABC):
    """
    Shared Lifecycle for all sre-swarm agents.

    Owns: Nats connect/subscribe/publish/disconnect, heartbeat loop, structured logging convention, graceful shutdown.

    Does not own: any domain knowledge. No agent-specific business rules belong here.
    """

    # Set by each concrete subclass.
    agent_type: str = "agents.base"

    def __init__(
            self,
            settings: Settings,
            nats_client: NatsClient,
            logger: Any,
            input_subject: str,
            durable_name: str,
            agent_instance_id: str | None = None,
    ) -> None:
        """
        Args:
            settings: already-constructed Settings instance (injected).
            nats_client: NatsClient instance — connection is owned by this
                object, but start()/stop() here drive connect/close on it.
            logger: structlog BoundLogger, ideally already bound with
                agent_type context by main.py before injection.
            input_subject: the NATS subject this agent subscribes to
                (resolved via agent_router.py upstream, not decided here).
            durable_name: explicit JetStream durable consumer name for this
                agent's subscription. Never derived from Settings.
            agent_instance_id: stable identity for this replica. Falls back
                to "{agent_type}-{hostname}" only for local/dev convenience —
                in a real deployment this should be passed explicitly
                (e.g. K8s pod name) so restarts keep the same identity.
        """

        self.settings = settings
        self.nats_client = nats_client
        self.logger = logger
        self.input_subject = input_subject
        self.durable_name = durable_name
        self.agent_instance_id = agent_instance_id or f"{self.agent_type}-{socket.gethostname()}"

        self._heartbeat_task: asyncio.Task | None = None
        self._running: bool = False

    # ------------------------------------------------------------------ #
    # Lifecycle                                                          #
    # ------------------------------------------------------------------ #

    async def start(self) -> None:
        """
        Bring the agent online:
            1. connect Nats.
            2. subscribe to input_subject with handle_message as the callback
            3. spawn the heartbeat loop as a background task

        Returns once setup is complete - does not block. The actual message processing
        happens via Nats-driven callbacks, not a loop here.
        """

        self.logger.info(
            "agent_starting",
            agent_type=self.agent_type,
            agent_instance_id = self.agent_instance_id,
        )

        await self.nats_client.connect()

        await self.nats_client.subscribe(
            subject=self.input_subject,
            durable_name=self.durable_name,
            handler = self.handle_message,
        )

        self._running = True

        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())

        self.logger.info("agent_started", input_subject=self.input_subject)

    async def stop(self) -> None:
        """
        Graceful shutdown: stop the heartbeat loop, drain Nats ( not a hard close - drain lets in-flight messages finish; see nats_client.py's _closing flag ordering).
        """

        self.logger.info("agent_stopping", agent_instance_id=self.agent_instance_id)
        self._running = False

        if self._heartbeat_task is not None:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass

        await self.nats_client.close()
        self.logger.info("agent_stopped", agent_instance_id=self.agent_instance_id)

    # ------------------------------------------------------------------ #
    # Abstract interface — implemented by each concrete agent            #
    # ------------------------------------------------------------------ #

    @abc.abstractmethod
    async def handle_message(self, message: AgentMessage) -> None:
        """
        Each agent implements its own handle_message. Called once per inbound message on input_subject, with correlation_id/incident_id already bound to context by nats_client - do not rebind here.

        Side-effect only: use self.publish() to emit results. No return value is consumed by the caller.
        """

        raise NotImplementedError

    # ------------------------------------------------------------------ #
    # Publishing                                                         #
    # ------------------------------------------------------------------ #
 
    async def publish(
            self, 
            subject: str, 
            payload: dict[str, Any],
            message_type: str,
            *,
            incident_id: UUID | None = None,
            in_reply_to: AgentMessage | None = None,
        ) -> None:
        """
        Thin wrapper around nats_client.publish() adding consistent logging
        and error handling across all agents. Does not add retry logic —
        that already lives in nats_client.publish()'s own backoff.
        """
        try:
            message = build_message(
                source_agent=self.agent_instance_id,
                type=message_type,
                incident_id=incident_id,
                payload=payload,
                in_reply_to=in_reply_to,
            )
            await self.nats_client.publish(subject, message)
            self.logger.info("message_published", subject=subject)
        except Exception:
            self.logger.error("publish_failed", subject=subject, exc_info=True)
            raise

    # ------------------------------------------------------------------ #
    # Heartbeat                                                          #
    # ------------------------------------------------------------------ #

    async def _heartbeat_loop(self) -> None:
        """
        Runs until cancelled by stop(). Every agent_hearbeat_interval)seconds (default 30s):

            1. publish a heartbeat AgentMessage to AGENT_HEARTBEAT (NATS —
             real-time liveness signal)
            2. put a key into the agent_heartbeats KV bucket, keyed by
             agent_instance_id, with value = last-seen payload. Each put
             resets that key's TTL clock (bucket TTL = timeout_seconds, set
             once in ensure_kv_bucket() at start()).
 
        """

        interval = self.settings.agent_heartbeat_interval_seconds

        while self._running:
            now = datetime.now(timezone.utc)
            status_payload: dict[str, Any] = {
                "agent_instance_id": self.agent_instance_id,
                "agent_type": self.agent_type,
                "status": "healthy",
                "timestamp": now.isoformat()
            }
            try:
                heartbeat_message = build_message(
                    source_agent=self.agent_instance_id,
                    type=HEARTBEAT_TYPE,
                    payload=status_payload,
                )
                await self.nats_client.publish(AGENT_HEARTBEAT, heartbeat_message)
            except Exception:
                self.logger.warning("heartbeat_publish_failed", exc_info=True)


            try:
                await self.nats_client.kv_put(
                    AGENT_HEARTBEAT_BUCKET,
                    self.agent_instance_id,
                    json.dumps(status_payload).encode("utf-8")
                )
            except Exception:
                self.logger.warning("heartbeat_kv_write_failed", exc_info=True)

            await asyncio.sleep(interval)


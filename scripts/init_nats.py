"""
Idempotent JetStream stream initializer.
Run via: make init ( after make dev, before starting any agent)
"""

import asyncio
import nats
from nats.js.errors import NotFoundError
from nats.js import JetStreamContext

from sre_shared.messaging.streams import STREAM_CONFIGS
from sre_shared.config.settings import Settings
from sre_shared.logging.logger import configure_logging, get_logger

logger = get_logger(__name__)

async def init_streams(js: JetStreamContext) -> None:
    """
    Create or update all JetStream streams defined in streams.py˘
    """

    for stream_config in STREAM_CONFIGS:
        name = stream_config.name
        subjects = stream_config.subjects
        if name is None:
            logger.error("stream_config_missing_name", config=stream_config)
        
            raise ValueError(f"StreamConfig missing name: {stream_config}")
        
        if subjects is None:
            logger.error("stream_config_missing_subjects",config=stream_config)

            raise ValueError(f"StreamConfig missing subjects: {stream_config}")
        
        try:
            await js.stream_info(name)
            logger.info("stream_exists", name=name)

            try:
                await js.update_stream(stream_config)
                print(f"  ✓ Updated stream: {name} ({len(subjects)} subjects)")

                logger.info("stream_updated", name=name, subjects=subjects)

            except Exception as exc:
                logger.error(
                    "stream_update_failed",
                    name=name,
                    error=str(exc),
                    error_type=type(exc).__name__,
                )
                raise

        except NotFoundError:
            logger.info("stream_not_found", name=name)

            try:
                await js.add_stream(config=stream_config)
                print(f"  ✓ Created stream: {name} ({len(subjects)} subjects)")

                logger.info("stream_created", name=name, subjects=subjects)
            
            except Exception as exc:
                logger.error(
                    "stream_create_failed",
                    name=name,
                    error=str(exc),
                    error_type=type(exc).__name__,
                )
                raise

async def main() -> None:
    settings = Settings()
    configure_logging(settings)

    logger.info("nats_connecting", url=settings.nats_url)
    print(f"Connecting to NATS at {settings.nats_url}...")
    try:
        nc = await nats.connect(settings.nats_url)
    except Exception as exc:
        logger.error("nats_connect_failed", url=settings.nats_url, error=str(exc))
        raise

    js = nc.jetstream()
    logger.info("nats_connected", url=settings.nats_url)

    try:
        await init_streams(js)
        logger.info("stream_init_complete", total_streams=len(STREAM_CONFIGS))
    finally:
        await nc.close()
        logger.info("nats_closed")

if __name__ == "__main__":
    asyncio.run(main())
    
    
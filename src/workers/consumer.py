import asyncio
import json
import signal
from uuid import UUID

from loguru import logger

from src.app.commands import ProcessModerationCommand
from src.app.interfaces import MessageConsumer
from src.app.services import ModerationConsumerService
from src.core.config import settings
from src.infrastructure.database.config import Database
from src.infrastructure.database.uow import SqlAlchemyUnitOfWork
from src.infrastructure.kafka.consumer import AioKafkaConsumer


class ModerationConsumerWorker:
    """Worker that consumes moderation events from Kafka and processes them."""

    def __init__(
        self,
        database: Database,
        consumer: MessageConsumer,
        poll_timeout_ms: int,
    ) -> None:
        self._database = database
        self._consumer = consumer
        self._poll_timeout_ms = poll_timeout_ms
        self._running = False

    async def start(self) -> None:
        """Start the worker and subscribe to the Kafka topic."""
        await self._consumer.start()
        self._running = True

    async def stop(self) -> None:
        """Stop the worker gracefully."""
        if not self._running:
            return
        self._running = False
        await self._consumer.stop()
        await self._database.close()

    async def run(self) -> None:
        """Run the moderation consumer loop until a shutdown signal is received."""
        await self.start()

        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, lambda: asyncio.create_task(self.stop()))

        while self._running:
            messages = await self._consumer.poll(self._poll_timeout_ms)
            if not messages:
                logger.info("No messages received")
                continue
            all_success = True
            for msg in messages:
                try:
                    if not msg.value:
                        all_success = False
                        logger.warning(f"Empty message at offset {msg.offset}")
                        continue
                    data = json.loads(msg.value)
                    command = ProcessModerationCommand(
                        event_id=UUID(data["event_id"]),
                        event_type=data["event_type"],
                        aggregate_id=UUID(data["aggregate_id"]),
                        payload=data["payload"],
                    )
                    async for session in self._database.get_session():
                        uow = SqlAlchemyUnitOfWork(session)
                        service = ModerationConsumerService(uow)
                        processed = await service.process(command)
                        if processed:
                            logger.info(f"Processed message {command.event_id}")
                        else:
                            logger.debug(
                                f"Skipped duplicate message {command.event_id}"
                            )
                except Exception:
                    all_success = False
                    logger.exception(
                        f"Failed to process message at offset {msg.offset}"
                    )

            if all_success:
                await self._consumer.commit()
                logger.info(f"Committed offset after processing {len(messages)}")
            else:
                logger.warning("Skipping commit due to errors")
        logger.info("Consumer stopped")


def main() -> None:
    """Entry point for the moderation consumer worker."""
    database = Database(
        database_url=settings.database_url,
        pool_size=settings.DB_POOL_SIZE,
        max_overflow=settings.DB_MAX_OVERFLOW,
        pool_pre_ping=settings.DB_POOL_PRE_PING,
        pool_recycle=settings.DB_POOL_RECYCLE,
    )
    consumer = AioKafkaConsumer(
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        group_id=settings.KAFKA_CONSUMER_GROUP_ID,
        topic=settings.KAFKA_CONSUMER_TOPIC,
    )
    worker = ModerationConsumerWorker(
        database=database,
        consumer=consumer,
        poll_timeout_ms=settings.KAFKA_POLL_TIMEOUT_MS,
    )
    asyncio.run(worker.run())


if __name__ == "__main__":
    main()

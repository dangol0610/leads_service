import asyncio

from loguru import logger

from src.app.interfaces import MessageProducer
from src.app.services import OutboxPublisherService
from src.core.config import settings
from src.infrastructure.database.config import Database
from src.infrastructure.database.uow import SqlAlchemyUnitOfWork
from src.infrastructure.kafka.producer import AiokafkaProducer


class OutboxPublisherWorker:
    """Worker that publishes unpublished outbox events to Kafka in a loop."""

    def __init__(
        self,
        database: Database,
        producer: MessageProducer,
        topic: str,
        poll_interval: int,
    ) -> None:
        self._database = database
        self._producer = producer
        self._topic = topic
        self._poll_interval = poll_interval
        self._running = False

    async def start(self) -> None:
        """Start the worker and connect to Kafka."""
        await self._producer.start()
        self._running = True

    async def stop(self) -> None:
        """Stop the worker gracefully."""
        if not self._running:
            return
        self._running = False
        await self._producer.stop()
        await self._database.close()

    async def run(self) -> None:
        """Run the outbox publishing loop until a shutdown signal is received."""
        await self.start()

        try:
            while self._running:
                async for session in self._database.get_session():
                    uow = SqlAlchemyUnitOfWork(session=session)
                    service = OutboxPublisherService(uow=uow, producer=self._producer)
                    count = await service.execute(topic=self._topic)

                if count == 0:
                    logger.debug(f"No events to publish, sleep {self._poll_interval}s")
                    await asyncio.sleep(self._poll_interval)
                else:
                    logger.info(f"Published {count} events to topic {self._topic}")
        except Exception:
            logger.exception("Unexpected worker error")
        finally:
            await self.stop()
            logger.info("Worker stopped")


def main() -> None:
    """Entry point for the outbox publisher worker."""
    database = Database(
        database_url=settings.database_url,
        pool_size=settings.DB_POOL_SIZE,
        max_overflow=settings.DB_MAX_OVERFLOW,
        pool_pre_ping=settings.DB_POOL_PRE_PING,
        pool_recycle=settings.DB_POOL_RECYCLE,
    )
    producer = AiokafkaProducer(bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS)
    worker = OutboxPublisherWorker(
        database=database,
        producer=producer,
        topic=settings.KAFKA_OUTBOX_TOPIC,
        poll_interval=settings.KAFKA_POLL_INTERVAL,
    )
    try:
        asyncio.run(worker.run())
    except KeyboardInterrupt:
        logger.info("Shutting down publisher")


if __name__ == "__main__":
    main()

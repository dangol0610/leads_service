from aiokafka import AIOKafkaConsumer

from src.app.interfaces import ConsumerMessage


class AioKafkaConsumer:
    """Kafka message consumer wrapping aiokafka.

    Implements the MessageConsumer protocol with manual offset commit.
    """

    def __init__(
        self,
        bootstrap_servers: str,
        group_id: str,
        topic: str,
    ) -> None:
        self._bootstrap_servers = bootstrap_servers
        self._group_id = group_id
        self._topic = topic
        self._consumer: AIOKafkaConsumer | None = None

    async def start(self) -> None:
        """Start the consumer and subscribe to the topic."""
        self._consumer = AIOKafkaConsumer(
            self._topic,
            bootstrap_servers=self._bootstrap_servers,
            group_id=self._group_id,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
        )
        await self._consumer.start()

    async def stop(self) -> None:
        """Stop the consumer and release all resources."""
        if self._consumer is not None:
            await self._consumer.stop()

    async def poll(self, timeout_ms: int) -> list[ConsumerMessage]:
        """Poll messages from Kafka and wrap them as ConsumerMessage DTOs."""
        if not self._consumer:
            raise RuntimeError("Consumer is not started")
        raw = await self._consumer.getmany(timeout_ms=timeout_ms)
        messages: list[ConsumerMessage] = []
        for tp, records in raw.items():
            for record in records:
                messages.append(
                    ConsumerMessage(
                        topic=record.topic,
                        key=record.key,
                        value=record.value,
                        offset=record.offset,
                        partition=record.partition,
                    )
                )
        return messages

    async def commit(self) -> None:
        """Commit the current offset to Kafka."""
        if not self._consumer:
            raise RuntimeError("Consumer is not started")
        await self._consumer.commit()

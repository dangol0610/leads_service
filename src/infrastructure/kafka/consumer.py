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
        self._consumer = AIOKafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
        )

    async def start(self) -> None:
        """Start the consumer and subscribe to the topic."""
        await self._consumer.start()

    async def stop(self) -> None:
        """Stop the consumer and release all resources."""
        await self._consumer.stop()

    async def poll(self, timeout_ms: int) -> list[ConsumerMessage]:
        """Poll messages from Kafka and wrap them as ConsumerMessage DTOs."""
        raw = await self._consumer.getmany(timeout_ms)
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
        await self._consumer.commit()

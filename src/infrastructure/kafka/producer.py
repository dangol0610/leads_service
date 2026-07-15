from aiokafka.producer import AIOKafkaProducer


class AiokafkaProducer:
    """Kafka message producer wrapping aiokafka.

    Implements the MessageProducer protocol.
    """

    def __init__(self, bootstrap_servers: str):
        self._bootstrap_servers = bootstrap_servers
        self._producer: AIOKafkaProducer | None = None

    async def start(self) -> None:
        """Start the producer and connect to the Kafka broker."""
        self._producer = AIOKafkaProducer(bootstrap_servers=self._bootstrap_servers)
        await self._producer.start()

    async def stop(self) -> None:
        """Stop the producer and release all resources."""
        if self._producer is not None:
            await self._producer.stop()

    async def send(self, topic: str, key: str, value: bytes) -> None:
        """Send a message and wait for acknowledgement from Kafka."""
        if not self._producer:
            raise RuntimeError("Consumer is not started")
        await self._producer.send_and_wait(
            topic=topic,
            key=key.encode(),
            value=value,
        )

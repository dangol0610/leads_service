from aiokafka.producer import AIOKafkaProducer


class AiokafkaProducer:
    """Kafka message producer wrapping aiokafka.

    Implements the MessageProducer protocol.
    """

    def __init__(self, bootstrap_servers: str):
        self._producer = AIOKafkaProducer(bootstrap_servers=bootstrap_servers)

    async def start(self) -> None:
        """Start the producer and connect to the Kafka broker."""
        await self._producer.start()

    async def stop(self) -> None:
        """Stop the producer and release all resources."""
        await self._producer.stop()

    async def send(self, topic: str, key: str, value: bytes) -> None:
        """Send a message and wait for acknowledgement from Kafka."""
        await self._producer.send_and_wait(
            topic=topic,
            key=key.encode(),
            value=value,
        )

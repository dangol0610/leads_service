from aiokafka.producer import AIOKafkaProducer


class AiokafkaProducer:
    def __init__(self, bootstrap_servers: str):
        self._producer = AIOKafkaProducer(bootstrap_servers=bootstrap_servers)

    async def start(self) -> None:
        await self._producer.start()

    async def stop(self) -> None:
        await self._producer.stop()

    async def send(self, topic: str, key: str, value: bytes) -> None:
        await self._producer.send_and_wait(
            topic=topic,
            key=key.encode(),
            value=value,
        )

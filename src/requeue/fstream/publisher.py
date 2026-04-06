from dataclasses import dataclass

from faststream.rabbit import RabbitBroker, RabbitExchange


from requeue.fstream.models import FQueueMessage


@dataclass
class Publisher:
    exchange: RabbitExchange
    broker: RabbitBroker
    connected: bool = False

    async def publish(self, message: FQueueMessage):
        if not self.connected:
            await self.broker.connect()
            self.connected = True
        await self.broker.publish(message=message, exchange=self.exchange)

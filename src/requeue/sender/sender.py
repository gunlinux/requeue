import typing
import logging

from abc import ABC, abstractmethod

from requeue.requeue import Queue
from requeue.rredis import Connection
from requeue.schemas import QueueMessageSchema


if typing.TYPE_CHECKING:
    from requeue.models import QueueMessage

logger = logging.getLogger(__name__)


class SenderAbc(ABC):
    @abstractmethod
    def __init__(
        self,
        queue_name: str,
        connection: Connection,
        source: str = "",
    ) -> None: ...

    @abstractmethod
    async def send_message(
        self,
        message: str,
        source: str = "",
        queue_name: str = "",
    ) -> None: ...


class Sender(SenderAbc):
    def __init__(
        self,
        queue_name: str,
        connection: Connection,
        source: str = "",
    ) -> None:
        self.connection: Connection = connection
        self.queue = Queue(name=queue_name, connection=connection)
        self.source = source

    async def send_message(
        self,
        message: str,
        source: str = "",
        queue_name: str = "",
    ) -> None:
        payload = {
            "event": "mssg",
            "data": {
                "message": message,
                "event_type": "mssg",
            },
            "source": source or self.source,
        }
        new_message: QueueMessage = typing.cast(
            "QueueMessage", QueueMessageSchema().load(payload)
        )
        if not queue_name:
            await self.queue.push(new_message)
            return
        await Queue(name=queue_name, connection=self.connection).push(new_message)


class DummySender(SenderAbc):
    def __init__(
        self,
        queue_name: str,
        connection: Connection,
        source: str = "",
    ) -> None:
        self.connection: Connection = connection
        self.queue_name = queue_name
        self.source = source

    async def send_message(
        self,
        message: str,
        source: str = "",
        queue_name: str = "",
    ) -> None:
        _ = source, queue_name
        logger.debug('send_message to %s "%s"', self.queue_name, message)

import typing
from typing import TYPE_CHECKING

from requeue.models import QueueMessageStatus
from requeue.requeue import Queue
from requeue.rredis import Connection
from requeue.schemas import QueueMessageSchema

if TYPE_CHECKING:
    from requeue.models import QueueMessage


def make_event_payload(event_name: str, _: dict | None = None):
    return {
        "event": event_name,
        "data": {
            "user_name": "tester",
            "amount": 10.5,
            "billing_system": None,
            "currency": "USD",
            "message": "hello world",
            "event_type": "test_event",
            "event": None,
        },
        "source": "test_queue",
        "retry": 0,
        "status": QueueMessageStatus.WAITING.value,
    }


async def test_queue(mock_redis: Connection):
    async with mock_redis as connection:
        queue = Queue(name="test_queue", connection=connection)

        payload1 = make_event_payload("Test event 1", {"kinda": 1})
        payload2 = make_event_payload("Test event 2", {"kinda": 2})

        message_one: QueueMessage = typing.cast(
            "QueueMessage", QueueMessageSchema().load(payload1)
        )
        message_two: QueueMessage = typing.cast(
            "QueueMessage", QueueMessageSchema().load(payload2)
        )

        await queue.push(message_one)
        assert await queue.llen() == 1
        await queue.push(message_two)
        assert await queue.llen() == 2  # noqa: PLR2004

        data_from_queue_1: QueueMessage | None = await queue.pop()
        assert data_from_queue_1 is not None
        payload1["status"] = QueueMessageStatus.PROCESSING.value
        assert data_from_queue_1.to_serializable_dict() == payload1
        assert await queue.llen() == 1

        data_from_queue_2: QueueMessage | None = await queue.pop()
        assert data_from_queue_2 is not None
        payload2["status"] = QueueMessageStatus.PROCESSING.value
        assert data_from_queue_2.to_serializable_dict() == payload2
        assert await queue.llen() == 0
        assert await queue.pop() is None


async def test_queue_opts(mock_redis: Connection):
    async with mock_redis as connection:
        queue = Queue(name="test_queue", connection=connection)

        payload1 = make_event_payload("Test event 1", {"kinda": 1})
        payload2 = make_event_payload("Test event 2", {"kinda": 2})

        message_one: QueueMessage = typing.cast(
            "QueueMessage", QueueMessageSchema().load(payload1)
        )
        message_two: QueueMessage = typing.cast(
            "QueueMessage", QueueMessageSchema().load(payload2)
        )

        await queue.push(message_one)
        assert await queue.llen() == 1
        await queue.push(message_two)
        assert await queue.llen() == 2  # noqa: PLR2004
        await queue.clean()
        assert await queue.llen() == 0
        assert await queue.pop() is None


async def test_retry(mock_redis: Connection):
    async with mock_redis as connection:
        queue = Queue(name="test_queue", connection=connection)

        payload1 = make_event_payload("Test event 1", {"kinda": 1})
        message_one: QueueMessage = typing.cast(
            "QueueMessage", QueueMessageSchema().load(payload1)
        )
        await queue.push(message_one)
        assert await queue.llen() == 1
        for _ in range(10):
            data_from_queue_1 = await queue.pop()
            if data_from_queue_1:
                await queue.push(data_from_queue_1)
        # after too many retries, it should go into failed queue
        assert await queue.llen() == 0
        assert await queue.pop() is None
        # failed queue should contain 1
        assert await queue.failed.llen() == 1

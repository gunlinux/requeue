import typing
from dataclasses import asdict, dataclass
from enum import Enum, auto


class QueueMessageStatus(Enum):
    WAITING = auto()
    PROCESSING = auto()
    FINISHED = auto()
    DROPPED = auto()
    FAILED = auto()


@dataclass
class QueueEvent:
    event_type: str
    user_name: str
    amount: float
    currency: str
    message: str
    event: dict[str, typing.Any]


@dataclass
class QueueMessage:
    event: str
    data: QueueEvent
    source: str = ""
    retry: int = 0
    status: QueueMessageStatus = QueueMessageStatus.WAITING

    def to_serializable_dict(self) -> dict[str, typing.Any]:
        return {
            field: (value.value if isinstance(value, Enum) else value)
            for field, value in asdict(self).items()
        }

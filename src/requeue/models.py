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
    billing_system: str | None = None
    user_name: str | None = None
    amount: float | None = None
    currency: str | None = None
    message: str | None = None
    event: dict[str, typing.Any] | None = None

    def recal_amount(self, currencies: dict[str, float], currency: str = "RUB"):
        if self.currency == currency or not self.amount:
            return
        if point := currencies.get(currency):
            self.amount = point * self.amount
            self.currency = currency
            return
        raise ValueError


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

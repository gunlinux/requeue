import typing
from enum import StrEnum

from marshmallow import Schema, fields, post_load
from marshmallow_enum import EnumField

from requeue.models import QueueMessage, QueueMessageStatus, QueueEvent


class EventType(StrEnum):
    DONATION = "donation"
    SUBSCRIBE = "subscribe"
    FOLLOW = "follow"
    MESSAGE = "message"
    RAID = "raid"


class QueueEventSchema(Schema):
    event_type = fields.String(required=True)
    billing_system = fields.String(required=True, allow_none=True)
    user_name = fields.String(required=True, allow_none=True)
    amount = fields.Float(required=True, allow_none=True)
    currency = fields.String(required=True, allow_none=True)
    message = fields.String(required=True, allow_none=True)
    event = fields.Dict(keys=fields.String(), values=fields.Raw(), allow_none=True)

    @post_load
    def make(self, data: dict[str, typing.Any], **_: typing.Any) -> QueueEvent:
        return QueueEvent(**data)


class QueueMessageSchema(Schema):
    event = fields.Str(required=True)
    data = fields.Nested(QueueEventSchema, required=True)
    source = fields.Str(required=False)
    retry = fields.Int(required=False)
    status = EnumField(
        QueueMessageStatus, by_value=True, dump_default=QueueMessageStatus.WAITING
    )

    @post_load
    def make(self, data: dict[str, typing.Any], **_: typing.Any) -> QueueMessage:
        return QueueMessage(**data)

import typing

from pydantic import BaseModel, UUID4


class MsgPayloadModel(BaseModel):
    meter_power: float
    id: UUID4
    delivery_tag: typing.Optional[int]

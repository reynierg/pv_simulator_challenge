import typing

from typing_extensions import Protocol


class MQReceiverProtocol(Protocol):
    DELIVERY_TAG: str
    def get_message(self, ack_on_receive: bool = False) -> typing.Optional[typing.Dict[str, str]]: ...

    def ack_message(self, msg_delivery_tag: str) -> None: ...

    def close_connection(self) -> None: ...


class PVPowerValueCalculatorProtocol(Protocol):
    def get_pv_power_value(self, minute: int) -> float: ...

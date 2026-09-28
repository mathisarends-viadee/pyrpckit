from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from types import MappingProxyType
from typing import Protocol


class RpcRejection(StrEnum):
    UNAUTHORIZED = "unauthorized"
    FORBIDDEN = "forbidden"
    NOT_FOUND = "not_found"
    PROTOCOL_ERROR = "protocol_error"
    UNAVAILABLE = "unavailable"
    INTERNAL_ERROR = "internal_error"


class RpcConnectionClose(StrEnum):
    NORMAL = "normal"
    SHUTDOWN = "shutdown"
    PROTOCOL_ERROR = "protocol_error"
    POLICY_VIOLATION = "policy_violation"
    MESSAGE_TOO_BIG = "message_too_big"
    INTERNAL_ERROR = "internal_error"
    TRY_AGAIN_LATER = "try_again_later"
    OTHER = "other"


class RpcDisconnect(Exception):
    def __init__(
        self,
        close: RpcConnectionClose | int = RpcConnectionClose.NORMAL,
        reason: str = "",
    ) -> None:
        if isinstance(close, RpcConnectionClose):
            self.code = close
            self.raw_close_code = None
        else:
            self.code = {
                1000: RpcConnectionClose.NORMAL,
                1001: RpcConnectionClose.SHUTDOWN,
                1002: RpcConnectionClose.PROTOCOL_ERROR,
                1008: RpcConnectionClose.POLICY_VIOLATION,
                1009: RpcConnectionClose.MESSAGE_TOO_BIG,
                1011: RpcConnectionClose.INTERNAL_ERROR,
                1013: RpcConnectionClose.TRY_AGAIN_LATER,
            }.get(close, RpcConnectionClose.OTHER)
            self.raw_close_code = close
        self.reason = reason
        super().__init__(reason)


@dataclass(frozen=True, slots=True)
class RpcHandshake:
    path: str
    headers: Mapping[str, str] = field(default_factory=lambda: MappingProxyType({}))
    query_params: Mapping[str, str] = field(
        default_factory=lambda: MappingProxyType({})
    )
    path_params: Mapping[str, str] = field(default_factory=lambda: MappingProxyType({}))
    subprotocols: tuple[str, ...] = ()
    client: tuple[str, int] | None = None


type RpcBeforeAccept = Callable[[RpcHandshake], Awaitable[Mapping[type, object] | None]]


class RpcReject(Exception):
    def __init__(
        self,
        rejection: RpcRejection,
        reason: str = "",
        *,
        headers: Mapping[str, str] | None = None,
    ) -> None:
        self.rejection = rejection
        self.reason = reason
        self.headers = headers
        super().__init__(reason)


type RpcRejectionMapper = Callable[[Exception], RpcReject | None]
type RpcRejections = Mapping[type[Exception], RpcRejection] | RpcRejectionMapper

REJECTION_CLOSES = MappingProxyType(
    {
        RpcRejection.UNAUTHORIZED: RpcConnectionClose.POLICY_VIOLATION,
        RpcRejection.FORBIDDEN: RpcConnectionClose.POLICY_VIOLATION,
        RpcRejection.NOT_FOUND: RpcConnectionClose.POLICY_VIOLATION,
        RpcRejection.PROTOCOL_ERROR: RpcConnectionClose.PROTOCOL_ERROR,
        RpcRejection.UNAVAILABLE: RpcConnectionClose.TRY_AGAIN_LATER,
        RpcRejection.INTERNAL_ERROR: RpcConnectionClose.INTERNAL_ERROR,
    }
)


def rejection_for(
    error: Exception, rejections: RpcRejections | None
) -> RpcReject | None:
    """Map a failure to a rejection; mapped entries use the error text as reason."""
    if isinstance(error, RpcReject):
        return error
    if rejections is None:
        return None
    if isinstance(rejections, Mapping):
        return next(
            (
                RpcReject(rejection, str(error))
                for exception, rejection in rejections.items()
                if isinstance(error, exception)
            ),
            None,
        )
    return rejections(error)


def chain_rejections(*levels: RpcRejections | None) -> RpcRejections | None:
    """Consult ``levels`` in order; the first one that maps a failure wins."""
    present = [level for level in levels if level is not None]
    if len(present) <= 1:
        return present[0] if present else None

    def reject(error: Exception) -> RpcReject | None:
        for level in present:
            rejected = rejection_for(error, level)
            if rejected is not None:
                return rejected
        return None

    return reject


class RpcSocket(Protocol):
    @property
    def handshake(self) -> RpcHandshake: ...
    async def accept(self, subprotocol: str | None = None) -> None: ...
    async def reject(
        self,
        rejection: RpcRejection,
        reason: str,
        *,
        headers: Mapping[str, str] | None = None,
    ) -> None: ...
    async def receive(self) -> str | bytes: ...
    async def send(self, message: str) -> None: ...
    async def send_bytes(self, data: bytes) -> None: ...
    async def close(self, close: RpcConnectionClose, reason: str) -> None: ...


@dataclass(frozen=True, slots=True)
class RpcLimits:
    max_concurrency: int = 32
    max_queue_size: int = 128
    max_message_bytes: int | None = 1_048_576
    client_method_timeout: float | None = 30.0
    send_timeout: float | None = 10.0
    max_batch_size: int = 32
    max_subscriptions: int = 100
    max_pending_requests: int = 1024

    def __post_init__(self) -> None:
        if self.max_concurrency < 1 or self.max_queue_size < 1:
            raise ValueError("RPC limits must be at least 1")
        if self.max_message_bytes is not None and self.max_message_bytes < 1:
            raise ValueError("max_message_bytes must be at least 1 or None")
        if self.client_method_timeout is not None and self.client_method_timeout <= 0:
            raise ValueError("client_method_timeout must be positive or None")
        if self.send_timeout is not None and self.send_timeout <= 0:
            raise ValueError("send_timeout must be positive or None")
        if self.max_batch_size < 0:
            raise ValueError("max_batch_size must be at least 0")
        if self.max_subscriptions < 1:
            raise ValueError("max_subscriptions must be at least 1")
        if self.max_pending_requests < 1:
            raise ValueError("max_pending_requests must be at least 1")


class _Headers(Mapping[str, str]):
    def __init__(self, values: Mapping[str, str]) -> None:
        self._values = {key.lower(): value for key, value in values.items()}

    def __getitem__(self, key: str) -> str:
        return self._values[key.lower()]

    def __iter__(self):
        return iter(self._values)

    def __len__(self) -> int:
        return len(self._values)


class RpcConnection:
    __slots__ = (
        "_accepted",
        "_closed",
        "_endpoint",
        "_handshake",
        "_close_code",
        "_close_reason",
        "_raw_close_code",
        "_on_close",
        "_path_params",
    )

    def __init__(self) -> None:
        raise TypeError("RpcConnection instances are created by rpckit")

    @classmethod
    def _create(
        cls, endpoint, socket: RpcSocket, path_params: Mapping[str, str] | None = None
    ):
        self = cls.__new__(cls)
        self._endpoint = endpoint.name
        self._handshake = socket.handshake
        self._path_params = MappingProxyType(
            dict(path_params or socket.handshake.path_params)
        )
        self._accepted = False
        self._closed = False
        self._close_code = None
        self._close_reason = ""
        self._raw_close_code = None
        self._on_close = None
        return self

    @property
    def endpoint(self) -> str:
        return self._endpoint

    @property
    def path(self) -> str:
        return self._handshake.path

    @property
    def path_params(self) -> Mapping[str, str]:
        return self._path_params

    @property
    def query_params(self) -> Mapping[str, str]:
        return MappingProxyType(dict(self._handshake.query_params))

    @property
    def headers(self) -> Mapping[str, str]:
        """Handshake headers with case-insensitive lookup."""
        return _Headers(self._handshake.headers)

    @property
    def subprotocols(self) -> tuple[str, ...]:
        return self._handshake.subprotocols

    @property
    def client(self) -> tuple[str, int] | None:
        return self._handshake.client

    @property
    def closed(self) -> bool:
        return self._closed

    @property
    def close_code(self) -> RpcConnectionClose | None:
        return self._close_code

    @property
    def close_reason(self) -> str:
        return self._close_reason

    @property
    def raw_close_code(self) -> int | None:
        return self._raw_close_code

    async def close(
        self, close: RpcConnectionClose = RpcConnectionClose.NORMAL, *, reason: str = ""
    ) -> None:
        if not self._accepted:
            raise RuntimeError(
                "RpcConnection.close() is only valid after the connection was accepted"
            )
        if self._on_close is not None:
            self._on_close(close, reason)

import asyncio
import inspect
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping, Sequence
from typing import (
    Annotated,
    Any,
    Protocol,
    get_type_hints,
    overload,
    runtime_checkable,
)

try:
    from fastapi import (
        APIRouter,
        Depends,
        Response,
        WebSocket,
        WebSocketDisconnect,
    )
    from fastapi.websockets import WebSocketState
except ImportError as error:
    raise ModuleNotFoundError(
        "rpckit.fastapi requires the 'fastapi' extra; install pyrpckit[fastapi]",
        name="fastapi",
    ) from error

from rpckit.connection import (
    REJECTION_CLOSES,
    RpcBeforeAccept,
    RpcConnectionClose,
    RpcDisconnect,
    RpcHandshake,
    RpcLimits,
    RpcRejection,
    RpcRejections,
    chain_rejections,
)
from rpckit.dependencies import RpcResolverLike
from rpckit.errors import RpcErrorBinding, contract_rejections, rejecting_contracts
from rpckit.runtime import _rejection
from rpckit.server import RpcErrorMapper
from rpckit.service import (
    RpcEndpoint,
    RpcService,
    RpcStreamEndpoint,
    endpoint_rejections,
)
from rpckit.websocket import CLOSE_CODES, REJECTION_CLOSE_CODES, close_reason

type FastApiResolverFactory = Callable[[WebSocket], RpcResolverLike]

_HTTP_STATUS = {
    RpcRejection.UNAUTHORIZED: 401,
    RpcRejection.FORBIDDEN: 403,
    RpcRejection.NOT_FOUND: 404,
    RpcRejection.PROTOCOL_ERROR: 400,
    RpcRejection.UNAVAILABLE: 503,
    RpcRejection.INTERNAL_ERROR: 500,
}


class FastApiSocket:
    def __init__(self, websocket: WebSocket) -> None:
        self._websocket = websocket
        client = websocket.client
        self._handshake = RpcHandshake(
            path=websocket.url.path,
            headers=dict(websocket.headers),
            query_params=dict(websocket.query_params),
            path_params=dict(websocket.path_params),
            subprotocols=tuple(websocket.scope.get("subprotocols", ())),
            client=None if client is None else (client.host, client.port),
        )

    @property
    def handshake(self) -> RpcHandshake:
        return self._handshake

    async def accept(self, subprotocol: str | None = None) -> None:
        await self._websocket.accept(subprotocol=subprotocol)

    async def reject(
        self,
        rejection: RpcRejection,
        reason: str,
        *,
        headers: Mapping[str, str] | None = None,
    ) -> None:
        await _reject(self._websocket, rejection, reason, headers=headers)

    async def receive(self) -> str | bytes:
        message = await self._websocket.receive()
        if message["type"] == "websocket.disconnect":
            raise RpcDisconnect(
                message.get("code") or 1000, str(message.get("reason", ""))
            )
        value = message.get("text")
        return value if value is not None else message.get("bytes", b"")

    async def send(self, message: str) -> None:
        try:
            await self._websocket.send_text(message)
        except WebSocketDisconnect as error:
            raise RpcDisconnect(error.code, error.reason or "") from error

    async def send_bytes(self, data: bytes) -> None:
        try:
            await self._websocket.send_bytes(data)
        except WebSocketDisconnect as error:
            raise RpcDisconnect(error.code, error.reason or "") from error

    async def close(self, close: RpcConnectionClose, reason: str) -> None:
        await self._websocket.close(CLOSE_CODES[close], close_reason(reason))


async def _reject(
    websocket: WebSocket,
    rejection: RpcRejection,
    reason: str,
    *,
    headers: Mapping[str, str] | None = None,
) -> None:
    if "websocket.http.response" in websocket.scope.get("extensions", {}):
        await websocket.send_denial_response(
            Response(
                reason,
                status_code=_HTTP_STATUS[rejection],
                media_type="text/plain",
                headers=dict(headers or {}),
            )
        )
    else:
        await websocket.close(REJECTION_CLOSE_CODES[rejection], close_reason(reason))


def create_router(
    service: RpcService,
    *,
    resolver: RpcResolverLike | None = None,
    resolver_factory: FastApiResolverFactory | None = None,
    context: object | Mapping[type[Any], object] | None = None,
    error_mapper: RpcErrorMapper | None = None,
    limits: RpcLimits | None = None,
    before_accept: RpcBeforeAccept | None = None,
    rejections: RpcRejections | None = None,
) -> APIRouter:
    if resolver is not None and resolver_factory is not None:
        raise ValueError("resolver and resolver_factory are mutually exclusive")
    service.freeze()
    router = APIRouter()
    for endpoint in service.endpoints:
        handler = _create_handler(
            endpoint,
            resolver=resolver,
            resolver_factory=resolver_factory,
            context=context,
            error_mapper=error_mapper,
            limits=limits,
            before_accept=before_accept,
            rejections=rejections,
        )
        router.add_api_websocket_route(endpoint.path, handler, name=endpoint.name)
    return router


def _create_handler(
    endpoint: RpcEndpoint | RpcStreamEndpoint,
    *,
    resolver: RpcResolverLike | None,
    resolver_factory: FastApiResolverFactory | None,
    context: object | Mapping[type[Any], object] | None,
    error_mapper: RpcErrorMapper | None,
    limits: RpcLimits | None,
    before_accept: RpcBeforeAccept | None,
    rejections: RpcRejections | None,
):
    async def handler(websocket: WebSocket) -> None:
        connection_resolver = (
            resolver_factory(websocket) if resolver_factory is not None else resolver
        )
        await serve_websocket(
            endpoint,
            websocket,
            resolver=connection_resolver,
            context=context,
            error_mapper=error_mapper,
            limits=limits,
            before_accept=before_accept,
            rejections=rejections,
        )

    return handler


async def serve_websocket(
    endpoint: RpcEndpoint | RpcStreamEndpoint,
    websocket: WebSocket,
    *,
    resolver: RpcResolverLike | None = None,
    context: object | Mapping[type[Any], object] | None = None,
    error_mapper: RpcErrorMapper | None = None,
    limits: RpcLimits | None = None,
    before_accept: RpcBeforeAccept | None = None,
    rejections: RpcRejections | None = None,
) -> None:
    socket = FastApiSocket(websocket)
    try:
        if isinstance(endpoint, RpcEndpoint):
            await endpoint.serve(
                socket,
                resolver=resolver,
                context=context,
                error_mapper=error_mapper,
                limits=limits,
                before_accept=before_accept,
                rejections=rejections,
            )
        else:
            await endpoint.serve(
                socket,
                resolver=resolver,
                context=context,
                limits=limits,
                before_accept=before_accept,
                error_mapper=error_mapper,
                rejections=rejections,
            )
    except asyncio.CancelledError:
        # Starlette cancels its WebSocket task after websocket.disconnect.
        return


@runtime_checkable
class FastApiResolver(Protocol):
    """Resolve RPC dependencies per WebSocket and prepare the context function.

    Pass an implementation, such as ``rpckit.dishka.Dishka``, as ``resolver=``
    to ``RpcRoutes`` to integrate a DI library.
    """

    def for_websocket(self, websocket: WebSocket) -> RpcResolverLike: ...

    def dependency[FunctionT: Callable[..., Any]](
        self, function: FunctionT
    ) -> FunctionT: ...


class RpcRoutes[ContextT]:
    """Mount rpckit endpoints as WebSocket routes on an existing FastAPI router.

    ``context`` runs as a FastAPI dependency per connection, path parameters
    included. Its result reaches handlers as ``Inject[T]``, where T is the
    context type the mounted endpoint declares. Failures covered by ``rejects``
    or ``rejections`` reject the handshake or close the socket, depending on
    whether it was already accepted.
    """

    @overload
    def __init__(
        self: "RpcRoutes[None]",
        router: APIRouter,
        *,
        context: None = None,
        resolver: RpcResolverLike | FastApiResolver | None = None,
        rejects: Sequence[RpcErrorBinding[Any]] = (),
        rejections: RpcRejections | None = None,
        error_mapper: RpcErrorMapper | None = None,
        limits: RpcLimits | None = None,
    ) -> None: ...

    @overload
    def __init__(
        self,
        router: APIRouter,
        *,
        context: Callable[..., Awaitable[ContextT]],
        resolver: RpcResolverLike | FastApiResolver | None = None,
        rejects: Sequence[RpcErrorBinding[Any]] = (),
        rejections: RpcRejections | None = None,
        error_mapper: RpcErrorMapper | None = None,
        limits: RpcLimits | None = None,
    ) -> None: ...

    def __init__(
        self,
        router: APIRouter,
        *,
        context: Callable[..., Awaitable[Any]] | None = None,
        resolver: RpcResolverLike | FastApiResolver | None = None,
        rejects: Sequence[RpcErrorBinding[Any]] = (),
        rejections: RpcRejections | None = None,
        error_mapper: RpcErrorMapper | None = None,
        limits: RpcLimits | None = None,
    ) -> None:
        self._router = router
        self._resolver: RpcResolverLike | None = None
        self._websocket_resolver: FastApiResolver | None = None
        if isinstance(resolver, FastApiResolver):
            self._websocket_resolver = resolver
        else:
            self._resolver = resolver
        self._context_type = None if context is None else _context_type(context)
        self._context = context
        if context is not None and self._websocket_resolver is not None:
            self._context = self._websocket_resolver.dependency(context)
        self._rejections = chain_rejections(
            rejections,
            contract_rejections(rejecting_contracts(rejects, "RpcRoutes")),
        )
        self._error_mapper = error_mapper
        self._limits = limits
        self._mounted: set[RpcEndpoint | RpcStreamEndpoint] = set()

    def mount(
        self, endpoint: RpcEndpoint[ContextT] | RpcStreamEndpoint[ContextT]
    ) -> None:
        """Add a WebSocket route for ``endpoint`` at its declared path."""
        if endpoint in self._mounted:
            raise ValueError(f"RPC endpoint {endpoint.name!r} is already mounted")
        self._check_context(endpoint)
        self._router.add_api_websocket_route(
            self._route_path(endpoint),
            self._handler(endpoint),
            name=endpoint.name,
            dependencies=[
                Depends(
                    _reject_failures(
                        chain_rejections(
                            self._rejections, endpoint_rejections(endpoint)
                        )
                    )
                )
            ],
        )
        self._mounted.add(endpoint)

    def _check_context(self, endpoint: RpcEndpoint | RpcStreamEndpoint) -> None:
        declared = endpoint.context
        provided = self._context_type
        if declared is None and provided is None:
            return
        if declared is None or provided is None or not issubclass(provided, declared):
            raise TypeError(
                f"RPC endpoint {endpoint.name!r} declares context "
                f"{_type_name(declared)}, but these routes provide "
                f"{_type_name(provided)}"
            )

    def _route_path(self, endpoint: RpcEndpoint | RpcStreamEndpoint) -> str:
        prefix = self._router.prefix
        path = endpoint.path
        if not path.startswith(prefix) or path[len(prefix) :][:1] not in ("", "/"):
            raise ValueError(
                f"RPC endpoint path {path!r} is outside the router prefix {prefix!r}"
            )
        return path[len(prefix) :]

    def _handler(
        self, endpoint: RpcEndpoint | RpcStreamEndpoint
    ) -> Callable[..., Awaitable[None]]:
        context_type = endpoint.context

        async def handler(websocket: WebSocket, context: Any = None) -> None:
            await serve_websocket(
                endpoint,
                websocket,
                resolver=(
                    self._websocket_resolver.for_websocket(websocket)
                    if self._websocket_resolver is not None
                    else self._resolver
                ),
                context=None if context_type is None else {context_type: context},
                error_mapper=self._error_mapper,
                limits=self._limits,
                rejections=self._rejections,
            )

        parameters = [
            inspect.Parameter(
                "websocket",
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
                annotation=WebSocket,
            )
        ]
        if self._context is not None:
            parameters.append(
                inspect.Parameter(
                    "context",
                    inspect.Parameter.KEYWORD_ONLY,
                    annotation=Annotated[Any, Depends(self._context)],
                )
            )
        handler.__signature__ = inspect.Signature(parameters)  # type: ignore[attr-defined]
        return handler


def _context_type(function: Callable[..., Any]) -> type[Any]:
    if not callable(function) or inspect.isclass(function):
        raise TypeError(f"context= expects an async function, got {function!r}")
    context_type = get_type_hints(function).get("return")
    if context_type in (Any, object, type(None)) or not inspect.isclass(context_type):
        raise TypeError(
            f"context= needs a concrete class as return annotation of "
            f"{getattr(function, '__qualname__', function)!r}, got {context_type!r}"
        )
    return context_type


def _type_name(value: type[Any] | None) -> str:
    return "none" if value is None else value.__name__


def _reject_failures(
    rejections: RpcRejections | None,
) -> Callable[[WebSocket], AsyncIterator[None]]:
    # Registered as the route's first dependency, so its exit sees failures of
    # later dependencies and of the handler.
    async def reject_failures(websocket: WebSocket) -> AsyncIterator[None]:
        try:
            yield
        except Exception as error:
            rejected = _rejection(error, rejections)
            if rejected is None:
                raise
            if websocket.application_state is WebSocketState.CONNECTING:
                await _reject(
                    websocket,
                    rejected.rejection,
                    rejected.reason,
                    headers=rejected.headers,
                )
            elif websocket.application_state is WebSocketState.CONNECTED:
                await websocket.close(
                    CLOSE_CODES[REJECTION_CLOSES[rejected.rejection]],
                    close_reason(rejected.reason),
                )

    return reject_failures

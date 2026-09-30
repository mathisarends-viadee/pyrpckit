import asyncio
import logging
import time
from collections.abc import Callable

from rpckit.codec import RpcCodec
from rpckit.connection import RpcConnection, RpcLimits
from rpckit.constants import LOGGER_NAME
from rpckit.dependencies import RpcResolver
from rpckit.dispatch import RpcDispatcher
from rpckit.envelopes import RpcFailure, RpcRequestId, RpcSuccess
from rpckit.errors import (
    RpcError,
    RpcInternalError,
    RpcInvalidRequestError,
    RpcParseError,
    bind_contracts,
    contract_for,
    contract_of,
)
from rpckit.observer import (
    RpcObserverLike,
    RpcRequestContext,
    RpcResponseContext,
    notify_observer,
)
from rpckit.protocol import RpcProtocol

type RpcErrorMapper = Callable[[Exception], RpcError | None]
type RpcResponse = RpcSuccess | RpcFailure
type RpcResponseMessage = RpcResponse | list[RpcResponse] | None

logger = logging.getLogger(LOGGER_NAME)


class RpcServer:
    """Serve decoded or encoded JSON-RPC messages over any transport."""

    def __init__(self) -> None:
        raise TypeError("RpcServer instances are created by RpcChannel.create_server()")

    @classmethod
    def _from_channel(
        cls,
        protocol: RpcProtocol,
        *,
        resolver: RpcResolver | None = None,
        error_mapper: RpcErrorMapper | None = None,
        observer: RpcObserverLike | None = None,
        connection: RpcConnection | None = None,
        limits: RpcLimits | None = None,
        strict_errors: bool = False,
    ) -> "RpcServer":
        server = cls.__new__(cls)
        server._protocol = protocol
        server._dispatcher = RpcDispatcher(protocol, resolver=resolver)
        server._error_mapper = error_mapper
        server._observer = observer
        server._connection = connection
        server._codec = RpcCodec()
        server._limits = limits or RpcLimits()
        server._semaphore = asyncio.Semaphore(server._limits.max_concurrency)
        server._strict_errors = strict_errors
        return server

    @property
    def protocol(self) -> RpcProtocol:
        return self._protocol

    async def handle(self, raw_request: object) -> RpcResponseMessage:
        """Serve one decoded request or batch."""
        if isinstance(raw_request, list):
            if not raw_request or len(raw_request) > self._limits.max_batch_size:
                return self.failure(None, RpcInvalidRequestError())
            responses = await asyncio.gather(
                *(self._handle_one_bounded(item) for item in raw_request)
            )
            return [response for response in responses if response is not None] or None
        return await self._handle_one_bounded(raw_request)

    async def _handle_one_bounded(self, raw_request: object) -> RpcResponse | None:
        async with self._semaphore:
            return await self._handle_one(raw_request)

    async def handle_json(self, message: str | bytes | bytearray) -> str | None:
        """Decode, serve, and encode one JSON-RPC message."""
        try:
            request = self._codec.decode(message)
        except RpcParseError as error:
            return self._codec.encode(self.failure(None, error))
        response = await self.handle(request)
        return None if response is None else self._codec.encode(response)

    async def _handle_one(self, raw_request: object) -> RpcResponse | None:
        request_context = RpcRequestContext(
            raw_request=raw_request,
            method=_request_method(raw_request),
            request_id=_request_id(raw_request),
            notification=_looks_like_notification(raw_request),
            connection=self._connection,
        )
        started = time.perf_counter()
        caught_error: Exception | None = None
        invocation = None
        await notify_observer(self._observer, "request_started", request_context)
        try:
            invocation = self._dispatcher.parse_request(raw_request)
            result = await self._dispatcher.execute(invocation)
        except Exception as error:
            caught_error = error
            if _looks_like_notification(raw_request):
                self._rpc_error(
                    error,
                    request_context.method,
                    declared=invocation.method.raises if invocation else (),
                )
                response = None
            else:
                response = self.failure(
                    _request_id(raw_request),
                    error,
                    method=request_context.method,
                    declared=invocation.method.raises if invocation else (),
                )
        else:
            response = (
                RpcSuccess._with_result_annotation(
                    invocation.request.id,
                    result,
                    invocation.method.result,
                )
                if invocation.request.expects_response
                else None
            )
        await notify_observer(
            self._observer,
            "request_finished",
            RpcResponseContext(
                request=request_context,
                response=response,
                duration=time.perf_counter() - started,
                error=caught_error,
            ),
        )
        return response

    def failure(
        self,
        request_id: RpcRequestId,
        error: Exception,
        *,
        method: str | None = None,
        declared: tuple[type[RpcError], ...] = (),
    ) -> RpcFailure:
        rpc_error = self._rpc_error(error, method, declared=declared)
        return RpcFailure.from_error(request_id, rpc_error)

    def _rpc_error(
        self,
        error: Exception,
        method: str | None = None,
        *,
        declared: tuple[type[RpcError], ...] = (),
    ) -> RpcError:
        if isinstance(error, RpcError):
            mapped = error
        elif (
            contract := contract_for(
                error,
                bind_contracts(
                    contract
                    for declared_error in declared
                    if (contract := contract_of(declared_error)) is not None
                ),
            )
        ) is not None:
            try:
                mapped = contract.to_error(error)
            except Exception:
                logger.exception(
                    "RPC error binding %s failed for method %s", contract.code, method
                )
                return RpcInternalError()
        else:
            try:
                mapped = (
                    self._error_mapper(error)
                    if self._error_mapper is not None
                    else None
                )
                if mapped is not None and not isinstance(mapped, RpcError):
                    raise TypeError("RPC error mapper must return RpcError or None")
            except Exception:
                logger.exception("RPC error mapper failed for method %s", method)
                return RpcInternalError()
        if mapped is not None:
            if (
                self._strict_errors
                and method is not None
                and not mapped._builtin
                and not isinstance(mapped, declared)
            ):
                logger.error(
                    "RPC method %s raised undeclared error %s",
                    method,
                    type(mapped).__name__,
                )
                return RpcInternalError()
            return mapped
        logger.error("RPC method %s failed", method, exc_info=error)
        return RpcInternalError()


def _request_id(raw_request: object) -> RpcRequestId:
    if not isinstance(raw_request, dict):
        return None
    request_id = raw_request.get("id")
    if isinstance(request_id, bool):
        return None
    return request_id if isinstance(request_id, str | int) else None


def _request_method(raw_request: object) -> str | None:
    if not isinstance(raw_request, dict):
        return None
    method = raw_request.get("method")
    return method if isinstance(method, str) else None


def _looks_like_notification(raw_request: object) -> bool:
    return (
        isinstance(raw_request, dict)
        and "id" not in raw_request
        and raw_request.get("jsonrpc") == "2.0"
        and isinstance(raw_request.get("method"), str)
    )

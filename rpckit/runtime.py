import asyncio
import logging
import time
from collections.abc import Mapping
from contextlib import suppress
from typing import Any

from pydantic import ValidationError

from rpckit._adapter import adapter
from rpckit.codec import RpcCodec
from rpckit.connected_client import RpcConnectedClient
from rpckit.connection import (
    REJECTION_CLOSES,
    RpcBeforeAccept,
    RpcConnection,
    RpcConnectionClose,
    RpcDisconnect,
    RpcLimits,
    RpcReject,
    RpcRejection,
    RpcRejections,
    RpcSocket,
    rejection_for,
)
from rpckit.constants import LOGGER_NAME
from rpckit.dependencies import (
    RpcResolverLike,
    as_resolver,
    connection_scope,
    context_values,
)
from rpckit.envelopes import RpcNotification, RpcRequestEnvelope
from rpckit.errors import (
    RpcError,
    RpcInternalError,
    RpcParseError,
    bind_contracts,
    contract_for,
    contract_of,
)
from rpckit.observer import RpcConnectionContext, notify_observer
from rpckit.server import RpcErrorMapper, RpcServer, _request_id
from rpckit.service import RpcEndpoint, RpcStreamEndpoint
from rpckit.streams import (
    RpcBinaryInput,
    RpcBinaryOutput,
    RpcInputEndMessage,
    RpcStreamClose,
)
from rpckit.subscriptions import SubscriptionSession

logger = logging.getLogger(LOGGER_NAME)


def _rejection(error: Exception, rejections: RpcRejections | None) -> RpcReject | None:
    try:
        return rejection_for(error, rejections)
    except Exception:
        logger.exception("RPC rejection mapper failed")
        return None


async def _prepare(
    endpoint,
    socket,
    resolver,
    context,
    path_model=None,
    before_accept=None,
    rejections=None,
):
    endpoint.service.freeze()
    resolved = as_resolver(resolver)
    values = context_values(context)
    path_params = (
        socket.handshake.path_params or endpoint.match(socket.handshake.path) or {}
    )
    connection = RpcConnection._create(endpoint, socket, path_params)
    base_values = {**values, RpcConnection: connection}
    if (
        endpoint.subprotocol is not None
        and endpoint.subprotocol not in socket.handshake.subprotocols
    ):
        await socket.reject(RpcRejection.PROTOCOL_ERROR, "Unsupported subprotocol")
        return None
    path_values: dict[str, Any] = {}
    if path_model is not None:
        try:
            parsed = path_model.model_validate(dict(path_params))
        except ValidationError:
            await socket.reject(RpcRejection.NOT_FOUND, "Invalid path variable")
            return None
        path_values = {name: getattr(parsed, name) for name in path_model.model_fields}
        base_values[path_model] = parsed
    if before_accept is not None:
        try:
            accepted_values = await before_accept(socket.handshake)
        except Exception as error:
            rejected = _rejection(error, rejections)
            if rejected is None:
                logger.exception("RPC before_accept hook failed")
                rejected = RpcReject(RpcRejection.INTERNAL_ERROR, "Internal error")
            await socket.reject(
                rejected.rejection, rejected.reason, headers=rejected.headers
            )
            return None
        if accepted_values is not None:
            base_values.update(accepted_values)
    await socket.accept(endpoint.subprotocol)
    connection._accepted = True
    return connection, resolved, base_values, path_values


async def serve_endpoint(
    endpoint: RpcEndpoint,
    socket: RpcSocket,
    *,
    resolver: RpcResolverLike | None = None,
    context: object | Mapping[type[Any], object] | None = None,
    error_mapper: RpcErrorMapper | None = None,
    limits: RpcLimits | None = None,
    before_accept: RpcBeforeAccept | None = None,
    rejections: RpcRejections | None = None,
) -> None:
    limits = limits or RpcLimits()
    prepared = await _prepare(
        endpoint,
        socket,
        resolver,
        context,
        endpoint.path_model,
        before_accept,
        rejections,
    )
    if prepared is None:
        return
    connection, resolved, values, _ = prepared
    started = time.perf_counter()
    await notify_observer(endpoint.observer, "connection_opened", connection)
    close_event = asyncio.Event()
    close_value = [RpcConnectionClose.NORMAL, ""]
    client_closed = False
    outgoing: asyncio.Queue[str] = asyncio.Queue(limits.max_queue_size)
    slow_consumer_reported = False

    async def report_slow_consumer() -> None:
        nonlocal slow_consumer_reported
        if slow_consumer_reported:
            return
        slow_consumer_reported = True
        await notify_observer(endpoint.observer, "slow_consumer_closed", connection)

    async def send_outgoing(message: str) -> None:
        try:
            async with asyncio.timeout(limits.send_timeout):
                await outgoing.put(message)
        except TimeoutError:
            logger.warning("RPC client too slow: outgoing queue blocked")
            request_close(RpcConnectionClose.POLICY_VIOLATION, "Client too slow")
            await report_slow_consumer()

    connected_client = RpcConnectedClient._create(
        connection, endpoint.protocol.client_methods, send_outgoing, limits
    )
    values = {**values, RpcConnectedClient: connected_client}
    codec = RpcCodec()

    def request_close(code, reason, raw_close_code=None):
        if not close_event.is_set():
            close_value[:] = [code, reason]
            connection._close_code = code
            connection._close_reason = reason
            connection._raw_close_code = raw_close_code
            close_event.set()
            connected_client._close()

    connection._on_close = request_close
    tasks: set[asyncio.Task] = set()
    try:
        async with connection_scope(resolved, values) as scoped:
            server = RpcServer._from_channel(
                endpoint.protocol,
                resolver=scoped,
                error_mapper=error_mapper,
                observer=endpoint.observer,
                connection=connection,
                limits=limits,
                strict_errors=endpoint.service.strict_errors,
            )
            subscriptions = SubscriptionSession(
                endpoint.protocol.subscriptions,
                scoped,
                send_outgoing,
                limit=limits.max_subscriptions,
                server=server,
                observer=endpoint.observer,
            )

            async def writer():
                nonlocal client_closed
                try:
                    while True:
                        message = await outgoing.get()
                        try:
                            async with asyncio.timeout(limits.send_timeout):
                                await socket.send(message)
                        finally:
                            outgoing.task_done()
                except asyncio.CancelledError:
                    raise
                except RpcDisconnect as error:
                    client_closed = True
                    request_close(error.code, error.reason, error.raw_close_code)
                except TimeoutError:
                    logger.warning("RPC client too slow: socket send blocked")
                    request_close(
                        RpcConnectionClose.POLICY_VIOLATION, "Client too slow"
                    )
                    await report_slow_consumer()
                except Exception:
                    logger.exception("RPC writer failed")
                    request_close(RpcConnectionClose.INTERNAL_ERROR, "Internal error")

            async def invoke(message):
                if isinstance(message, dict) and subscriptions.owns(
                    str(message.get("method", ""))
                ):
                    try:
                        request = RpcRequestEnvelope.model_validate(message)
                    except ValidationError:
                        response = await server.handle(message)
                        if response is not None:
                            await send_outgoing(codec.encode(response))
                        return
                    await subscriptions.handle(request)
                    return
                response = await server.handle(message)
                if response is not None:
                    await send_outgoing(codec.encode(response))

            async def reject_pending(message):
                logger.warning("RPC client exceeded max_pending_requests")
                if _is_notification(message):
                    return
                failure = server.failure(_request_id(message), RpcPendingLimitError())
                await send_outgoing(codec.encode(failure))

            async def reader():
                nonlocal client_closed
                try:
                    while True:
                        frame = await socket.receive()
                        size = len(
                            frame if isinstance(frame, bytes) else frame.encode()
                        )
                        if (
                            limits.max_message_bytes is not None
                            and size > limits.max_message_bytes
                        ):
                            request_close(RpcConnectionClose.MESSAGE_TOO_BIG, "")
                            return
                        if isinstance(frame, bytes):
                            try:
                                frame = frame.decode()
                            except UnicodeDecodeError:
                                request_close(
                                    RpcConnectionClose.PROTOCOL_ERROR,
                                    "Invalid RPC frame",
                                )
                                return
                        try:
                            message = codec.decode(frame)
                        except RpcParseError as error:
                            await send_outgoing(
                                codec.encode(server.failure(None, error))
                            )
                            continue
                        if connected_client._resolve(message):
                            continue
                        if len(tasks) >= limits.max_pending_requests:
                            await reject_pending(message)
                            continue
                        task = asyncio.create_task(invoke(message))
                        tasks.add(task)
                        task.add_done_callback(tasks.discard)
                except RpcDisconnect as error:
                    client_closed = True
                    request_close(error.code, error.reason, error.raw_close_code)

            async def events():
                try:
                    await asyncio.gather(
                        *(
                            _event_source(
                                event,
                                scoped,
                                send_outgoing,
                                endpoint.observer,
                                rejections,
                                server,
                            )
                            for event in endpoint.protocol.notifications
                        )
                    )
                except asyncio.CancelledError:
                    raise
                except Exception as error:
                    rejected = _rejection(error, rejections)
                    if rejected is not None:
                        request_close(
                            REJECTION_CLOSES[rejected.rejection], rejected.reason
                        )
                        return
                    logger.exception("RPC event source failed")
                    request_close(RpcConnectionClose.INTERNAL_ERROR, "Internal error")
                    return
                if endpoint.close_when_events_complete:
                    request_close(RpcConnectionClose.NORMAL, "")

            writer_task = asyncio.create_task(writer())
            producers = [
                asyncio.create_task(reader()),
                asyncio.create_task(events()),
            ]
            try:
                await close_event.wait()
                for task in (*producers, *tasks):
                    task.cancel()
                await asyncio.gather(*producers, *tasks, return_exceptions=True)
                await subscriptions.close()
                if close_value[0] in (
                    RpcConnectionClose.NORMAL,
                    RpcConnectionClose.SHUTDOWN,
                ):
                    await _flush(outgoing, writer_task, limits.send_timeout)
            finally:
                for task in (writer_task, *producers, *tasks):
                    task.cancel()
                await asyncio.gather(
                    writer_task, *producers, *tasks, return_exceptions=True
                )
    except asyncio.CancelledError:
        close_value[:] = [RpcConnectionClose.SHUTDOWN, ""]
        connection._close_code = RpcConnectionClose.SHUTDOWN
        connection._close_reason = ""
        if not client_closed:
            with suppress(RpcDisconnect):
                await socket.close(*close_value)
        connection._closed = True
        raise
    finally:
        connected_client._close()
        if not client_closed and not connection.closed:
            with suppress(RpcDisconnect):
                await socket.close(*close_value)
        connection._closed = True
        await notify_observer(
            endpoint.observer,
            "connection_closed",
            RpcConnectionContext(
                connection=connection,
                close_code=connection.close_code,
                close_reason=connection.close_reason,
                duration=time.perf_counter() - started,
                raw_close_code=connection.raw_close_code,
            ),
        )


class RpcPendingLimitError(RpcError):
    code = "pending_limit"
    message = "Too many pending requests"


async def _flush(
    outgoing: asyncio.Queue[str], writer: asyncio.Task, timeout: float | None
) -> None:
    drained = asyncio.create_task(outgoing.join())
    try:
        await asyncio.wait(
            (drained, writer), timeout=timeout, return_when=asyncio.FIRST_COMPLETED
        )
    finally:
        drained.cancel()


def _is_notification(message: object) -> bool:
    return isinstance(message, dict) and "id" not in message


async def _event_source(event, resolver, send, observer, rejections, server):
    try:
        arguments = {
            parameter.name: await resolver.resolve(parameter.dependency)
            for parameter in event.injected_parameters
        }
        payload_adapter = adapter(event.payload)
        codec = RpcCodec()
        async for payload in event.function(**arguments):
            try:
                value = payload_adapter.validate_python(payload)
                message = codec.encode(
                    RpcNotification._with_payload_annotation(
                        event.name, value, event.payload
                    )
                )
            except Exception:
                logger.exception("RPC event %s produced an invalid payload", event.name)
                if event.on_error == "close":
                    raise
                continue
            await send(message)
            await notify_observer(
                observer, "notification_sent", event.name, len(message.encode())
            )
    except asyncio.CancelledError:
        raise
    except Exception as error:
        binding = contract_for(
            error,
            bind_contracts(
                binding
                for declared in event.raises
                if (binding := contract_of(declared)) is not None
            ),
        )
        if binding is not None:
            try:
                rejected = binding.reject(error)
            except Exception:
                logger.exception("RPC event error binding failed for %s", event.name)
                raise RpcReject(
                    RpcRejection.INTERNAL_ERROR, "Internal error"
                ) from error
            if rejected is not None:
                raise rejected from error
        rejected = _rejection(error, rejections)
        if rejected is not None:
            raise rejected from error
        mapped = server._rpc_error(error, event.name, declared=event.raises)
        if event.on_error == "close":
            rejection = (
                RpcRejection.INTERNAL_ERROR
                if isinstance(mapped, RpcInternalError)
                else RpcRejection.FORBIDDEN
            )
            raise RpcReject(rejection, mapped.message) from error


async def serve_stream_endpoint(
    endpoint: RpcStreamEndpoint,
    socket: RpcSocket,
    *,
    resolver: RpcResolverLike | None = None,
    context: object | Mapping[type[Any], object] | None = None,
    limits: RpcLimits | None = None,
    before_accept: RpcBeforeAccept | None = None,
    error_mapper: RpcErrorMapper | None = None,
    rejections: RpcRejections | None = None,
) -> None:
    limits = limits or RpcLimits()
    stream = endpoint.stream
    prepared = await _prepare(
        endpoint,
        socket,
        resolver,
        context,
        stream.path_model,
        before_accept,
        rejections,
    )
    if prepared is None:
        return
    connection, resolved, values, path_values = prepared
    started = time.perf_counter()
    await notify_observer(endpoint.observer, "connection_opened", connection)
    close_event = asyncio.Event()
    close_value = [RpcConnectionClose.NORMAL, ""]
    client_closed = False

    def request_close(code, reason, raw_close_code=None):
        if not close_event.is_set():
            close_value[:] = [code, reason]
            connection._close_code = code
            connection._close_reason = reason
            connection._raw_close_code = raw_close_code
            close_event.set()

    def client_disconnected(error: RpcDisconnect) -> None:
        nonlocal client_closed
        client_closed = True
        request_close(error.code, error.reason, error.raw_close_code)

    def close_stream_error(error: Exception) -> None:
        if isinstance(error, RpcStreamClose):
            request_close(error.close, error.reason)
            return
        rejected = _rejection(error, rejections)
        if rejected is not None:
            request_close(REJECTION_CLOSES[rejected.rejection], rejected.reason)
            return
        try:
            mapped = (
                error
                if isinstance(error, RpcError)
                else error_mapper(error)
                if error_mapper is not None
                else None
            )
        except Exception:
            logger.exception("RPC binary stream error mapper failed")
            mapped = None
        if mapped is not None:
            request_close(
                RpcConnectionClose.POLICY_VIOLATION,
                f"{mapped.code}: {mapped.message}",
            )
            return
        logger.error("RPC binary stream failed", exc_info=error)
        request_close(RpcConnectionClose.INTERNAL_ERROR, "Internal error")

    connection._on_close = request_close
    binary_input = (
        RpcBinaryInput._create(limits.max_queue_size) if stream.has_input else None
    )
    if not stream.is_generator:
        values = {
            **values,
            RpcBinaryOutput: RpcBinaryOutput._create(
                socket, connection, endpoint.observer
            ),
        }
        if binary_input is not None:
            values[RpcBinaryInput] = binary_input
    generator = None
    try:
        async with (
            connection_scope(resolved, values) as scoped,
            stream.resolver_scope(scoped) as stream_resolver,
        ):
            arguments = {
                p.name: await stream_resolver.resolve(p.dependency)
                for p in stream.injected_parameters
            }
            arguments.update(path_values)

            async def write():
                try:
                    async for frame in generator:
                        if not isinstance(frame, bytes | bytearray | memoryview):
                            logger.error("Binary stream yielded a non-bytes value")
                            request_close(
                                RpcConnectionClose.INTERNAL_ERROR, "Internal error"
                            )
                            return
                        data = bytes(frame)
                        await socket.send_bytes(data)
                        await notify_observer(
                            endpoint.observer,
                            "stream_frame_sent",
                            connection,
                            len(data),
                        )
                    request_close(RpcConnectionClose.NORMAL, "")
                except asyncio.CancelledError:
                    raise
                except RpcDisconnect as error:
                    client_disconnected(error)
                except Exception as error:
                    close_stream_error(error)

            async def handle():
                try:
                    await stream.function(**arguments)
                    request_close(RpcConnectionClose.NORMAL, "")
                except asyncio.CancelledError:
                    raise
                except RpcDisconnect as error:
                    client_disconnected(error)
                except Exception as error:
                    close_stream_error(error)

            async def read():
                input_ended = False
                try:
                    while True:
                        frame = await socket.receive()
                        if binary_input is None:
                            request_close(
                                RpcConnectionClose.PROTOCOL_ERROR,
                                "Binary stream is server-to-client",
                            )
                            return
                        size = len(frame.encode() if isinstance(frame, str) else frame)
                        if (
                            limits.max_message_bytes is not None
                            and size > limits.max_message_bytes
                        ):
                            request_close(RpcConnectionClose.MESSAGE_TOO_BIG, "")
                            return
                        if isinstance(frame, str):
                            if input_ended or not _is_input_end(frame):
                                request_close(
                                    RpcConnectionClose.PROTOCOL_ERROR,
                                    "Unexpected text frame on a binary stream",
                                )
                                return
                            input_ended = True
                            await binary_input._end()
                        elif input_ended:
                            request_close(
                                RpcConnectionClose.PROTOCOL_ERROR,
                                "Binary input after end",
                            )
                            return
                        else:
                            await binary_input._put(frame)
                            await notify_observer(
                                endpoint.observer,
                                "stream_frame_received",
                                connection,
                                len(frame),
                            )
                except RpcDisconnect as error:
                    client_disconnected(error)

            if stream.is_generator:
                generator = stream.function(**arguments)
                run = write()
            else:
                run = handle()
            tasks = [asyncio.create_task(run), asyncio.create_task(read())]
            await close_event.wait()
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
    except asyncio.CancelledError:
        close_value[:] = [RpcConnectionClose.SHUTDOWN, ""]
        connection._close_code = RpcConnectionClose.SHUTDOWN
        connection._close_reason = ""
        raise
    except Exception as error:
        close_stream_error(error)
    finally:
        if generator is not None:
            with suppress(Exception):
                await generator.aclose()
        if not client_closed:
            with suppress(RpcDisconnect):
                await socket.close(*close_value)
        connection._closed = True
        await notify_observer(
            endpoint.observer,
            "connection_closed",
            RpcConnectionContext(
                connection=connection,
                close_code=connection.close_code,
                close_reason=connection.close_reason,
                duration=time.perf_counter() - started,
                raw_close_code=connection.raw_close_code,
            ),
        )


def _is_input_end(frame: str) -> bool:
    try:
        RpcInputEndMessage.model_validate_json(frame)
    except ValidationError:
        return False
    return True

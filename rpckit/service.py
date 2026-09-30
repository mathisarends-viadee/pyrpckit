import re
import warnings
from collections import Counter
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from types import FunctionType, NoneType
from typing import Any
from urllib.parse import unquote

from pydantic import BaseModel

from rpckit.channel import RpcChannel, request_name
from rpckit.connection import (
    RpcBeforeAccept,
    RpcLimits,
    RpcRejection,
    RpcRejections,
    RpcSocket,
    chain_rejections,
)
from rpckit.contract import RpcContract, ServerVariable
from rpckit.dependencies import RpcResolverLike, resolver_with_context
from rpckit.errors import (
    ProtocolDefinitionError,
    RpcErrorBinding,
    contract_rejections,
    rejecting_contracts,
)
from rpckit.observer import RpcObserverLike
from rpckit.protocol import RpcProtocol, RpcStreamDefinition
from rpckit.server import (
    RpcErrorMapper,
    RpcServer,
)


@dataclass(frozen=True, slots=True, eq=False)
class RpcEndpoint[ContextT]:
    service: "RpcService"
    name: str
    path: str
    channels: tuple[RpcChannel, ...]
    error_mapper: RpcErrorMapper | None
    observer: RpcObserverLike | None
    limits: RpcLimits
    subprotocol: str | None
    summary: str | None
    path_variables: tuple[str, ...]
    before_accept: RpcBeforeAccept | None = None
    path_model: type[BaseModel] | None = None
    context: type[ContextT] | None = None
    close_when_events_complete: bool = False
    rejects: tuple[RpcErrorBinding[Any], ...] = ()
    _protocol: RpcProtocol | None = field(default=None, init=False, repr=False)

    @property
    def protocol(self) -> RpcProtocol:
        if self._protocol is not None:
            return self._protocol
        self.service.freeze()
        service_protocol = self.service.protocol
        methods = tuple(
            method for method in service_protocol.methods if method.server == self.name
        )
        notifications = tuple(
            notification
            for notification in service_protocol.notifications
            if notification.server == self.name
        )
        subscriptions = tuple(
            subscription
            for subscription in service_protocol.subscriptions
            if subscription.server == self.name
        )
        client_methods = tuple(
            client_method
            for client_method in service_protocol.client_methods
            if client_method.server == self.name
        )
        protocol = RpcProtocol(
            methods=methods,
            notifications=notifications,
            subscriptions=subscriptions,
            notification_types=service_protocol.notification_types,
            client_methods=client_methods,
            version=self.service.version,
        )
        object.__setattr__(self, "_protocol", protocol)
        return protocol

    def match(self, path: str) -> dict[str, str] | None:
        return _match(self.path, path)

    async def serve(
        self,
        socket: RpcSocket,
        *,
        resolver: RpcResolverLike | None = None,
        context: object | Mapping[type[Any], object] | None = None,
        error_mapper: RpcErrorMapper | None = None,
        limits: RpcLimits | None = None,
        before_accept: RpcBeforeAccept | None = None,
        rejections: RpcRejections | None = None,
    ) -> None:
        from rpckit.runtime import serve_endpoint

        await serve_endpoint(
            self,
            socket,
            resolver=resolver,
            context=context,
            error_mapper=error_mapper or self.error_mapper,
            limits=limits or self.limits,
            before_accept=before_accept or self.before_accept,
            rejections=chain_rejections(rejections, endpoint_rejections(self)),
        )

    def create_server(
        self,
        *,
        context: object | Mapping[type[Any], object] | None = None,
        resolver: RpcResolverLike | None = None,
        error_mapper: RpcErrorMapper | None = None,
        limits: RpcLimits | None = None,
        observer: RpcObserverLike | None = None,
    ) -> RpcServer:
        return RpcServer._from_channel(
            self.protocol,
            resolver=resolver_with_context(resolver, context),
            error_mapper=error_mapper or self.error_mapper,
            observer=observer or self.observer,
            limits=limits or self.limits,
            strict_errors=self.service.strict_errors,
        )


@dataclass(frozen=True, slots=True, eq=False)
class RpcStreamEndpoint[ContextT]:
    service: "RpcService"
    name: str
    path: str
    stream: RpcStreamDefinition
    observer: RpcObserverLike | None
    limits: RpcLimits
    subprotocol: str | None
    summary: str | None
    path_variables: tuple[str, ...]
    before_accept: RpcBeforeAccept | None = None
    error_mapper: RpcErrorMapper | None = None
    context: type[ContextT] | None = None
    rejects: tuple[RpcErrorBinding[Any], ...] = ()

    def match(self, path: str) -> dict[str, str] | None:
        return _match(self.path, path)

    async def serve(
        self,
        socket: RpcSocket,
        *,
        resolver: RpcResolverLike | None = None,
        context: object | Mapping[type[Any], object] | None = None,
        limits: RpcLimits | None = None,
        before_accept: RpcBeforeAccept | None = None,
        error_mapper: RpcErrorMapper | None = None,
        rejections: RpcRejections | None = None,
    ) -> None:
        from rpckit.runtime import serve_stream_endpoint

        await serve_stream_endpoint(
            self,
            socket,
            resolver=resolver,
            context=context,
            limits=limits or self.limits,
            before_accept=before_accept or self.before_accept,
            error_mapper=error_mapper or self.error_mapper,
            rejections=chain_rejections(rejections, endpoint_rejections(self)),
        )


class RpcService:
    def __init__(
        self,
        *,
        version: int = 1,
        error_mapper: RpcErrorMapper | None = None,
        observer: RpcObserverLike | None = None,
        limits: RpcLimits | None = None,
        strict_errors: bool = False,
        rejects: Sequence[RpcErrorBinding[Any]] = (),
    ) -> None:
        if not isinstance(version, int) or version < 1:
            raise ProtocolDefinitionError(
                "RPC service version must be a positive integer"
            )
        self._version = version
        self._error_mapper = error_mapper
        self._observer = observer
        self._limits = limits or RpcLimits()
        self._strict_errors = strict_errors
        self._rejects = rejecting_contracts(rejects, "RpcService")
        self._endpoints: list[RpcEndpoint | RpcStreamEndpoint] = []
        self._channels: set[RpcChannel] = set()
        self._mounted_streams: set[FunctionType] = set()
        self._protocol: RpcProtocol | None = None

    @property
    def version(self) -> int:
        return self._version

    @property
    def endpoints(self) -> tuple[RpcEndpoint | RpcStreamEndpoint, ...]:
        return tuple(self._endpoints)

    @property
    def protocol(self) -> RpcProtocol:
        return self.freeze()

    @property
    def strict_errors(self) -> bool:
        return self._strict_errors

    @property
    def rejects(self) -> tuple[RpcErrorBinding[Any], ...]:
        return self._rejects

    def socket[ContextT](
        self,
        path: str,
        /,
        *,
        channels: Sequence[RpcChannel],
        name: str | None = None,
        error_mapper: RpcErrorMapper | None = None,
        observer: RpcObserverLike | None = None,
        limits: RpcLimits | None = None,
        subprotocol: str | None = None,
        summary: str | None = None,
        before_accept: RpcBeforeAccept | None = None,
        path_model: type[BaseModel] | None = None,
        context: type[ContextT] = NoneType,
        close_when_events_complete: bool = False,
        rejects: Sequence[RpcErrorBinding[Any]] = (),
    ) -> RpcEndpoint[ContextT]:
        self._ensure_mutable()
        variables = _validate_endpoint(self._endpoints, path, name, subprotocol)
        _validate_context(context)
        if path_model is not None:
            if not isinstance(path_model, type) or not issubclass(
                path_model, BaseModel
            ):
                raise ProtocolDefinitionError(
                    "RPC socket path_model must be a Pydantic model"
                )
            if set(path_model.model_fields) != set(variables):
                raise ProtocolDefinitionError(
                    "RPC socket path_model fields must match path variables "
                    f"{variables!r}"
                )
        if not channels:
            raise ProtocolDefinitionError("RPC sockets need at least one channel")
        if any(not isinstance(channel, RpcChannel) for channel in channels):
            raise ProtocolDefinitionError("RPC sockets accept RpcChannel objects")
        if any(channel in self._channels for channel in channels):
            raise ProtocolDefinitionError("An RPC channel can only be mounted once")
        names = [c.name for c in (*self._channels, *channels)]
        if len(names) != len(set(names)):
            duplicates = sorted(
                name for name, count in Counter(names).items() if count > 1
            )
            raise ProtocolDefinitionError(
                f"RPC channel names must be unique: {', '.join(duplicates)}"
            )
        endpoint = RpcEndpoint(
            self,
            name or _endpoint_name(path),
            path,
            tuple(channels),
            error_mapper or self._error_mapper,
            observer or self._observer,
            limits or self._limits,
            subprotocol,
            summary,
            variables,
            before_accept,
            path_model,
            None if context is NoneType else context,
            close_when_events_complete,
            rejecting_contracts(rejects, f"RPC socket {path!r}"),
        )
        self._endpoints.append(endpoint)
        self._channels.update(channels)
        return endpoint

    def stream[ContextT](
        self,
        path: str,
        stream: Callable[..., Any],
        /,
        *,
        name: str | None = None,
        observer: RpcObserverLike | None = None,
        limits: RpcLimits | None = None,
        subprotocol: str | None = None,
        summary: str | None = None,
        before_accept: RpcBeforeAccept | None = None,
        error_mapper: RpcErrorMapper | None = None,
        context: type[ContextT] = NoneType,
        rejects: Sequence[RpcErrorBinding[Any]] = (),
    ) -> RpcStreamEndpoint[ContextT]:
        self._ensure_mutable()
        variables = _validate_endpoint(self._endpoints, path, name, subprotocol)
        _validate_context(context)
        marker = getattr(stream, "__rpckit_stream__", None)
        if not isinstance(stream, FunctionType) or marker is None:
            raise ProtocolDefinitionError(
                "rpc.stream() expects a function decorated by @channel.server.stream"
            )
        if stream in self._mounted_streams:
            raise ProtocolDefinitionError("An RPC stream can only be mounted once")
        channel, _ = marker
        definition = next(item for item in channel.streams if item.function is stream)
        missing = [p for p in definition.path_parameters if p not in variables]
        if missing:
            raise ProtocolDefinitionError(
                f"RPC binary stream {definition.name} parameters "
                f"{', '.join(missing)} are not path variables of {path!r}; "
                "annotate dependencies as Inject[T]"
            )
        endpoint = RpcStreamEndpoint(
            self,
            name or _endpoint_name(path),
            path,
            definition,
            observer or self._observer,
            limits or self._limits,
            subprotocol,
            summary,
            variables,
            before_accept,
            error_mapper or self._error_mapper,
            None if context is NoneType else context,
            rejecting_contracts(rejects, f"RPC stream {path!r}"),
        )
        self._endpoints.append(endpoint)
        self._channels.add(channel)
        self._mounted_streams.add(stream)
        return endpoint

    def endpoint(self, name: str) -> RpcEndpoint | RpcStreamEndpoint:
        for endpoint in self.endpoints:
            if endpoint.name == name:
                return endpoint
        raise KeyError(
            f"Unknown RPC endpoint {name!r}; known endpoints: "
            + ", ".join(e.name for e in self.endpoints)
        )

    def match(
        self, path: str
    ) -> tuple[RpcEndpoint | RpcStreamEndpoint, dict[str, str]] | None:
        ordered = sorted(
            enumerate(self.endpoints), key=lambda item: ("{" in item[1].path, item[0])
        )
        for _, endpoint in ordered:
            values = endpoint.match(path)
            if values is not None:
                return endpoint, values
        return None

    def freeze(self) -> RpcProtocol:
        if self._protocol is not None:
            return self._protocol
        methods = []
        notifications = []
        subscriptions = []
        types = []
        streams = []
        client_methods = []
        owners: dict[str, str] = {}
        errors: dict[str, type] = {}
        for endpoint in self.endpoints:
            if isinstance(endpoint, RpcEndpoint):
                if endpoint.close_when_events_complete and not any(
                    channel.freeze().notifications for channel in endpoint.channels
                ):
                    raise ProtocolDefinitionError(
                        f"RPC socket {endpoint.name!r} sets close_when_events_complete "
                        "but its channels declare no events"
                    )
                for channel in endpoint.channels:
                    protocol = channel.freeze()
                    methods.extend(
                        replace(item, server=endpoint.name) for item in protocol.methods
                    )
                    notifications.extend(
                        replace(item, server=endpoint.name)
                        for item in protocol.notifications
                    )
                    subscriptions.extend(
                        replace(item, server=endpoint.name)
                        for item in protocol.subscriptions
                    )
                    types.extend(protocol.notification_types)
                    client_methods.extend(
                        replace(item, server=endpoint.name)
                        for item in protocol.client_methods
                    )
                    for item in (
                        *protocol.methods,
                        *protocol.notifications,
                        *protocol.subscriptions,
                        *protocol.client_methods,
                    ):
                        _unique_name(owners, item.name, channel.name)
                    for method in (
                        *protocol.methods,
                        *protocol.client_methods,
                        *protocol.notifications,
                        *protocol.subscriptions,
                    ):
                        for error in method.raises:
                            previous = errors.get(error.code)
                            if previous is not None and previous is not error:
                                raise ProtocolDefinitionError(
                                    f"Duplicate RPC error code: {error.code}"
                                )
                            errors[error.code] = error
            else:
                channel = endpoint.stream.function.__rpckit_stream__[0]
                channel.freeze()
                _unique_name(owners, endpoint.stream.name, channel.name)
                streams.append(replace(endpoint.stream, server=endpoint.name))
        all_names = set(owners)
        numeric_codes: dict[int, list[str]] = {}
        for error in errors.values():
            numeric_codes.setdefault(error.rpc_code, []).append(error.code)
        for rpc_code, codes in numeric_codes.items():
            if rpc_code != -32000 and len(codes) > 1:
                warnings.warn(
                    f"RPC errors {', '.join(sorted(codes))} share rpc_code {rpc_code}; "
                    "clients should use data.code",
                    RuntimeWarning,
                    stacklevel=2,
                )
        for name in all_names:
            parts = name.split(".")
            for index in range(1, len(parts)):
                prefix = ".".join(parts[:index])
                if prefix in all_names:
                    raise ProtocolDefinitionError(
                        f"RPC name {prefix!r} is both an operation and a namespace"
                    )
        duplicate_requests = {
            name
            for name, count in Counter(m.request_name for m in methods).items()
            if count > 1
        }
        methods = [
            replace(
                m,
                request_name=request_name(m.name),
            )
            if m.request_name in duplicate_requests
            else m
            for m in methods
        ]
        self._protocol = RpcProtocol(
            methods=methods,
            notifications=notifications,
            subscriptions=subscriptions,
            notification_types=types,
            streams=streams,
            client_methods=client_methods,
            version=self.version,
        )
        return self._protocol

    async def serve(
        self,
        socket: RpcSocket,
        *,
        resolver: RpcResolverLike | None = None,
        context: object | Mapping[type[Any], object] | None = None,
        error_mapper: RpcErrorMapper | None = None,
        limits: RpcLimits | None = None,
        root_path: str = "",
        rejections: RpcRejections | None = None,
    ) -> None:
        path = socket.handshake.path
        if root_path and path.startswith(root_path):
            path = path[len(root_path) :] or "/"
        matched = self.match(path)
        if matched is None:
            await socket.reject(RpcRejection.NOT_FOUND, "Not found")
            return
        endpoint, _ = matched
        if isinstance(endpoint, RpcEndpoint):
            await endpoint.serve(
                socket,
                resolver=resolver,
                context=context,
                error_mapper=error_mapper,
                limits=limits,
                rejections=rejections,
            )
        else:
            await endpoint.serve(
                socket,
                resolver=resolver,
                context=context,
                limits=limits,
                error_mapper=error_mapper,
                rejections=rejections,
            )

    def contract(
        self,
        *,
        title: str,
        base_url: str,
        description: str = "Typed JSON-RPC API.",
        variables: Mapping[str, ServerVariable] | None = None,
    ) -> RpcContract:
        from rpckit.contract import contract_from_service

        return contract_from_service(
            self,
            title=title,
            base_url=base_url,
            description=description,
            variables=variables,
        )

    def _ensure_mutable(self) -> None:
        if self._protocol is not None:
            raise ProtocolDefinitionError(
                "RpcService is frozen because its protocol was already materialized"
            )


def endpoint_rejections(
    endpoint: RpcEndpoint[Any] | RpcStreamEndpoint[Any],
) -> RpcRejections | None:
    """The endpoint's ``rejects=``, falling back to the service's."""
    return chain_rejections(
        contract_rejections(endpoint.rejects),
        contract_rejections(endpoint.service.rejects),
    )


def _validate_context(context: object) -> None:
    if context is not None and not isinstance(context, type):
        raise ProtocolDefinitionError(
            f"RPC endpoint context must be a class, got {context!r}"
        )


def _validate_endpoint(
    endpoints, path: str, name: str | None, subprotocol: str | None
) -> tuple[str, ...]:
    if (
        not isinstance(path, str)
        or not path.startswith("/")
        or (path != "/" and path.endswith("/"))
        or "?" in path
        or "#" in path
        or "//" in path
    ):
        raise ProtocolDefinitionError(f"Invalid RPC endpoint path: {path!r}")
    variables = tuple(re.findall(r"{([^{}]+)}", path))
    if (
        len(variables) != len(set(variables))
        or any(re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", v) is None for v in variables)
        or re.sub(r"{[^{}]+}", "", path).find("{") >= 0
    ):
        raise ProtocolDefinitionError(f"Invalid RPC path variables: {path!r}")
    endpoint_name = name or _endpoint_name(path)
    if re.fullmatch(r"[A-Za-z][A-Za-z0-9_-]*", endpoint_name) is None:
        raise ProtocolDefinitionError(f"Invalid RPC endpoint name: {endpoint_name!r}")
    if subprotocol is not None and (
        not isinstance(subprotocol, str) or not subprotocol
    ):
        raise ProtocolDefinitionError("RPC subprotocol must be a non-empty string")
    normalized = re.sub(r"{[^{}]+}", "{}", path)
    if any(e.name == endpoint_name for e in endpoints):
        raise ProtocolDefinitionError(f"Duplicate RPC endpoint name: {endpoint_name}")
    if any(re.sub(r"{[^{}]+}", "{}", e.path) == normalized for e in endpoints):
        raise ProtocolDefinitionError(f"Duplicate RPC endpoint path: {path}")
    return variables


def _endpoint_name(path: str) -> str:
    for segment in reversed(path.split("/")):
        if segment and "{" not in segment:
            return segment
    raise ProtocolDefinitionError("Cannot derive endpoint name; pass name=...")


def _match(template: str, path: str) -> dict[str, str] | None:
    names = re.findall(r"{([^{}]+)}", template)
    pattern = (
        "^"
        + re.sub(
            r"{[^{}]+}",
            r"([^/]+)",
            re.escape(template).replace(r"\{", "{").replace(r"\}", "}"),
        )
        + "$"
    )
    match = re.match(pattern, path)
    return (
        None
        if match is None
        else {
            name: unquote(value)
            for name, value in zip(names, match.groups(), strict=True)
        }
    )


def _unique_name(owners: dict[str, str], name: str, channel: str) -> None:
    if name in owners:
        raise ProtocolDefinitionError(
            f"Duplicate RPC name: {name} (channels {owners[name]!r} and {channel!r})"
        )
    owners[name] = channel

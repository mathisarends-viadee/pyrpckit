import inspect
import re
from collections import Counter
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, replace
from types import FunctionType
from typing import Any, overload

from pydantic import BaseModel

from rpckit.connection import RpcLimits
from rpckit.dependencies import (
    RpcResolverLike,
    RpcResolverScope,
    call_scope,
    resolver_with_context,
)
from rpckit.errors import (
    ProtocolDefinitionError,
    RpcError,
    RpcErrorBinding,
    RpcErrorDeclaration,
    error_declarations,
    merge_error_declarations,
)
from rpckit.observer import RpcObserverLike
from rpckit.protocol import (
    RpcClientMethod,
    RpcNotificationDefinition,
    RpcProtocol,
    RpcStreamDefinition,
    RpcSubscriptionDefinition,
    client_method_definition,
    method_definition,
    notification_definition,
    notification_type_definitions,
    stream_definition,
    subscription_definition,
)
from rpckit.server import (
    RpcErrorMapper,
    RpcServer,
)


@dataclass(frozen=True, slots=True)
class RpcRoute:
    name: str
    function: FunctionType
    summary: str | None
    raises: tuple[type[RpcError], ...]
    resolver_scope: RpcResolverScope


class RpcChannel:
    def __init__(
        self,
        name: str | None = None,
        /,
        *,
        namespace: str | None = None,
        raises: Iterable[RpcErrorDeclaration] = (),
        resolver_scope: RpcResolverScope = call_scope,
    ) -> None:
        if name is None and namespace is None:
            raise ProtocolDefinitionError("RpcChannel needs a name or namespace")
        resolved_namespace = (
            normalize_namespace(name)
            if namespace is None
            else normalize_namespace(namespace)
        )
        self._name = (
            normalize_namespace(name) if name is not None else resolved_namespace
        )
        if not self._name:
            raise ProtocolDefinitionError("RPC channel name cannot be empty")
        self._namespace = resolved_namespace
        self._raises = error_declarations(raises, owner=f"RPC channel {self.name}")
        if not callable(resolver_scope):
            raise ProtocolDefinitionError("RPC resolver scope must be callable")
        self._resolver_scope = resolver_scope
        self._routes: list[RpcRoute] = []
        self._events: list[RpcNotificationDefinition] = []
        self._subscriptions: list[RpcSubscriptionDefinition] = []
        self._streams: list[RpcStreamDefinition] = []
        self._client_methods: list[RpcClientMethod[Any, Any]] = []
        self._children: list[RpcChannel] = []
        self._names: set[str] = set()
        self._protocol: RpcProtocol | None = None
        self._server = RpcServerSide(self)
        self._client = RpcClientSide(self)

    @property
    def name(self) -> str:
        return self._name

    @property
    def namespace(self) -> str:
        return self._namespace

    @property
    def raises(self) -> tuple[type[RpcError], ...]:
        return self._raises

    @property
    def resolver_scope(self) -> RpcResolverScope:
        return self._resolver_scope

    @property
    def routes(self) -> tuple[RpcRoute, ...]:
        return tuple(self._routes)

    @property
    def events(self) -> tuple[RpcNotificationDefinition, ...]:
        return tuple(self._events)

    @property
    def subscriptions(self) -> tuple[RpcSubscriptionDefinition, ...]:
        return tuple(self._subscriptions)

    @property
    def streams(self) -> tuple[RpcStreamDefinition, ...]:
        return tuple(self._streams)

    @property
    def client_methods(self) -> tuple[RpcClientMethod[Any, Any], ...]:
        return tuple(self._client_methods)

    @property
    def children(self) -> tuple["RpcChannel", ...]:
        return tuple(self._children)

    @property
    def protocol(self) -> RpcProtocol:
        return self.freeze()

    @property
    def server(self) -> "RpcServerSide":
        """What the server implements."""
        return self._server

    @property
    def client(self) -> "RpcClientSide":
        """What the connected client implements."""
        return self._client

    def child(
        self,
        name: str,
        /,
        *,
        raises: Iterable[RpcErrorDeclaration] = (),
        resolver_scope: RpcResolverScope | None = None,
    ) -> "RpcChannel":
        self._ensure_mutable()
        local = _name(name, "child channel name")
        child = RpcChannel(
            join_rpc_name(self.name, local),
            namespace=join_rpc_name(self.namespace, local),
            raises=merge_error_declarations(
                self.raises,
                raises,
                owner=f"RPC channel {join_rpc_name(self.name, local)}",
            ),
            resolver_scope=resolver_scope or self.resolver_scope,
        )
        self._children.append(child)
        return child

    def freeze(self) -> RpcProtocol:
        if self._protocol is None:
            definitions = [
                method_definition(
                    name=r.name,
                    function=r.function,
                    handler_name=r.function.__name__,
                    summary=r.summary,
                    raises=r.raises,
                    server=None,
                    resolver_scope=r.resolver_scope,
                )
                for r in self.routes
            ]
            notifications = list(self.events)
            subscriptions = list(self.subscriptions)
            notification_types = [
                item
                for event in self.events
                for item in notification_type_definitions(event.payload)
            ]
            streams = list(self.streams)
            client_methods = list(self.client_methods)
            for child in self.children:
                protocol = child.freeze()
                definitions.extend(protocol.methods)
                notifications.extend(protocol.notifications)
                subscriptions.extend(protocol.subscriptions)
                notification_types.extend(protocol.notification_types)
                streams.extend(protocol.streams)
                client_methods.extend(protocol.client_methods)
            duplicate_requests = {
                name
                for name, count in Counter(d.request_name for d in definitions).items()
                if count > 1
            }
            definitions = [
                replace(
                    d,
                    request_name=request_name(d.name)
                    if d.request_name in duplicate_requests
                    else d.request_name,
                )
                for d in definitions
            ]
            self._protocol = RpcProtocol(
                methods=definitions,
                notifications=notifications,
                subscriptions=subscriptions,
                notification_types=notification_types,
                streams=streams,
                client_methods=client_methods,
            )
        return self._protocol

    def create_server(
        self,
        *,
        context: object | Mapping[type[Any], object] | None = None,
        resolver: RpcResolverLike | None = None,
        error_mapper: RpcErrorMapper | None = None,
        observer: RpcObserverLike | None = None,
        limits: RpcLimits | None = None,
        strict_errors: bool = False,
    ) -> RpcServer:
        return RpcServer._from_channel(
            self.protocol,
            resolver=resolver_with_context(resolver, context),
            error_mapper=error_mapper,
            observer=observer,
            limits=limits,
            strict_errors=strict_errors,
        )

    def _reserve(self, name: str) -> None:
        if name in self._names:
            raise ProtocolDefinitionError(f"Duplicate RPC route: {name}")
        self._names.add(name)

    def _validate_function(self, function: Any, kind: str, *, coroutine: bool) -> None:
        if not isinstance(function, FunctionType) or not _is_free_function(function):
            raise ProtocolDefinitionError(
                f"RPC {kind} must decorate a free function, got {function!r}"
            )
        if coroutine and not inspect.iscoroutinefunction(function):
            raise ProtocolDefinitionError(
                f"RPC method {function.__qualname__} must be async"
            )

    def _ensure_mutable(self) -> None:
        if self._protocol is not None:
            raise ProtocolDefinitionError(
                f"RpcChannel {self.name!r} is frozen because its protocol was "
                "already materialized (directly or by RpcService)"
            )


class RpcServerSide:
    """``channel.server``: methods, events and streams the server implements."""

    def __init__(self, channel: RpcChannel) -> None:
        self._channel = channel

    @overload
    def method(self, function: FunctionType, /) -> FunctionType: ...

    @overload
    def method(
        self,
        name: str | None = None,
        /,
        *,
        summary: str | None = None,
        raises: Iterable[RpcErrorDeclaration] = (),
    ) -> Callable[[FunctionType], FunctionType]: ...

    def method(
        self,
        name: str | FunctionType | None = None,
        /,
        *,
        summary: str | None = None,
        raises: Iterable[RpcErrorDeclaration] = (),
    ) -> Any:
        channel = self._channel
        channel._ensure_mutable()
        if isinstance(name, FunctionType):
            return self.method()(name)
        if name is not None and not isinstance(name, str):
            raise ProtocolDefinitionError(
                "RPC method decorator expects a function or name"
            )
        local_name = None if name is None else _name(name, "method name")
        raises = tuple(raises)

        def decorate(function: FunctionType) -> FunctionType:
            channel._validate_function(function, "method", coroutine=True)
            wire_name = join_rpc_name(
                channel.namespace, local_name or function.__name__
            )
            merged_raises = merge_error_declarations(
                channel.raises, raises, owner=f"RPC method {wire_name}"
            )
            channel._reserve(wire_name)
            channel._routes.append(
                RpcRoute(
                    wire_name,
                    function,
                    summary or _docstring_summary(function),
                    merged_raises,
                    channel.resolver_scope,
                )
            )
            return function

        return decorate

    @overload
    def event(self, function: FunctionType, /) -> FunctionType: ...

    @overload
    def event(
        self,
        name: str | None = None,
        /,
        *,
        payload: Any = None,
        summary: str | None = None,
        on_error: str = "continue",
        raises: Iterable[RpcErrorDeclaration] = (),
    ) -> Callable[[FunctionType], FunctionType]: ...

    def event(
        self,
        name: str | FunctionType | None = None,
        /,
        *,
        payload: Any = None,
        summary: str | None = None,
        on_error: str = "continue",
        raises: Iterable[RpcErrorDeclaration] = (),
    ) -> Any:
        channel = self._channel
        channel._ensure_mutable()
        if isinstance(name, FunctionType):
            return self.event()(name)
        if on_error not in ("continue", "close"):
            raise ProtocolDefinitionError(
                "RPC event on_error must be 'continue' or 'close'"
            )
        if name is not None and not isinstance(name, str):
            raise ProtocolDefinitionError(
                "RPC event decorator expects a function or name"
            )
        raises = tuple(raises)

        def decorate(function: FunctionType) -> FunctionType:
            channel._validate_function(function, "event", coroutine=False)
            wire_name = join_rpc_name(
                channel.namespace, _name(name or function.__name__, "event name")
            )
            merged_raises = merge_error_declarations(
                channel.raises, raises, owner=f"RPC event {wire_name}"
            )
            definition = notification_definition(
                name=wire_name,
                payload=payload,
                function=function,
                summary=summary or _docstring_summary(function),
                server=None,
                on_error=on_error,
                raises=merged_raises,
            )
            channel._reserve(wire_name)
            channel._events.append(definition)
            return function

        return decorate

    @overload
    def subscription(self, function: FunctionType, /) -> FunctionType: ...

    @overload
    def subscription(
        self,
        name: str | None = None,
        /,
        *,
        summary: str | None = None,
        raises: Iterable[RpcErrorDeclaration] = (),
    ) -> Callable[[FunctionType], FunctionType]: ...

    def subscription(
        self,
        name: str | FunctionType | None = None,
        /,
        *,
        summary: str | None = None,
        raises: Iterable[RpcErrorDeclaration] = (),
    ) -> Any:
        channel = self._channel
        channel._ensure_mutable()
        if isinstance(name, FunctionType):
            return self.subscription()(name)
        if name is not None and not isinstance(name, str):
            raise ProtocolDefinitionError(
                "RPC subscription decorator expects a function or name"
            )
        raises = tuple(raises)

        def decorate(function: FunctionType) -> FunctionType:
            channel._validate_function(function, "subscription", coroutine=False)
            wire_name = join_rpc_name(
                channel.namespace,
                _name(name or function.__name__, "subscription name"),
            )
            merged_raises = merge_error_declarations(
                channel.raises, raises, owner=f"RPC subscription {wire_name}"
            )
            definition = subscription_definition(
                name=wire_name,
                function=function,
                summary=summary or _docstring_summary(function),
                resolver_scope=channel.resolver_scope,
                raises=merged_raises,
            )
            channel._reserve(wire_name)
            channel._subscriptions.append(definition)
            return function

        return decorate

    @overload
    def stream(self, function: FunctionType, /) -> FunctionType: ...

    @overload
    def stream(
        self,
        name: str | None = None,
        /,
        *,
        content_type: str = "application/octet-stream",
        input_content_type: str | None = None,
        summary: str | None = None,
    ) -> Callable[[FunctionType], FunctionType]: ...

    def stream(
        self,
        name: str | FunctionType | None = None,
        /,
        *,
        content_type: str = "application/octet-stream",
        input_content_type: str | None = None,
        summary: str | None = None,
    ) -> Any:
        channel = self._channel
        channel._ensure_mutable()
        if isinstance(name, FunctionType):
            return self.stream()(name)
        if name is not None and not isinstance(name, str):
            raise ProtocolDefinitionError(
                "RPC stream decorator expects a function or name"
            )
        if not isinstance(content_type, str) or not content_type:
            raise ProtocolDefinitionError("RPC stream content_type cannot be empty")
        if input_content_type is not None and (
            not isinstance(input_content_type, str) or not input_content_type
        ):
            raise ProtocolDefinitionError(
                "RPC stream input_content_type cannot be empty"
            )

        def decorate(function: FunctionType) -> FunctionType:
            channel._validate_function(function, "stream", coroutine=False)
            local = _name(name or function.__name__, "stream name")
            wire_name = join_rpc_name(channel.namespace, local)
            definition = stream_definition(
                name=wire_name,
                function=function,
                content_type=content_type,
                input_content_type=input_content_type,
                summary=summary or _docstring_summary(function),
                resolver_scope=channel.resolver_scope,
            )
            channel._reserve(wire_name)
            channel._streams.append(definition)
            function.__rpckit_stream__ = channel, local
            return function

        return decorate


class RpcClientSide:
    """``channel.client``: methods the connected client implements."""

    def __init__(self, channel: RpcChannel) -> None:
        self._channel = channel

    def method[ParamsT: BaseModel, ResultT](
        self,
        name: str,
        /,
        *,
        params: type[ParamsT] | None = None,
        result: type[ResultT] | None = None,
        raises: Iterable[type[RpcError]] = (),
        summary: str | None = None,
    ) -> RpcClientMethod[ParamsT, ResultT]:
        """Declare a request the server sends and the connected client answers."""
        channel = self._channel
        channel._ensure_mutable()
        wire_name = join_rpc_name(channel.namespace, _name(name, "client method name"))
        raises = tuple(raises)
        if any(isinstance(error, RpcErrorBinding) for error in raises):
            raise ProtocolDefinitionError(
                "Client methods declare RpcError subclasses: the server receives "
                "these errors and cannot rebuild a domain exception from them"
            )
        definition = client_method_definition(
            name=wire_name,
            params=params,
            result=result,
            summary=summary,
            raises=error_declarations(raises, owner=f"RPC client method {wire_name}"),
        )
        channel._reserve(wire_name)
        channel._client_methods.append(definition)
        return definition


def join_rpc_name(*parts: str) -> str:
    return ".".join(part for part in parts if part)


def normalize_namespace(value: object) -> str:
    namespace = str(value)
    if namespace:
        _name(namespace, "namespace")
    return namespace


def _name(value: object, kind: str) -> str:
    """Validate a name relative to a namespace; dots separate its segments."""
    if not isinstance(value, str) or any(
        re.fullmatch(r"[A-Za-z_][A-Za-z0-9_-]*", segment) is None
        for segment in value.split(".")
    ):
        raise ProtocolDefinitionError(f"Invalid RPC {kind}: {value!r}")
    return value


def _is_free_function(function: FunctionType) -> bool:
    return "." not in function.__qualname__.rsplit("<locals>.", 1)[-1]


def _docstring_summary(function: Any) -> str | None:
    doc = inspect.getdoc(function)
    return None if not doc else doc.splitlines()[0].strip() or None


def request_name(wire_name: str) -> str:
    return "".join(part.capitalize() for part in wire_name.split(".")) + "Request"

import inspect
from collections import Counter
from collections.abc import AsyncGenerator, AsyncIterator, Iterable
from dataclasses import dataclass
from types import FunctionType, UnionType
from typing import (
    Annotated,
    Any,
    TypeAliasType,
    Union,
    get_args,
    get_origin,
    get_type_hints,
)

from pydantic import (
    BaseModel,
    PydanticSchemaGenerationError,
    TypeAdapter,
    create_model,
)

from rpckit.dependencies import (
    RpcInjectedParameter,
    RpcResolverScope,
    injected_parameter,
)
from rpckit.errors import (
    ProtocolDefinitionError,
    RpcError,
    RpcMethodNotFoundError,
    bind_contracts,
    contract_of,
)
from rpckit.streams import RpcBinaryInput, RpcBinaryOutput, RpcStreamDirection
from rpckit.wire import wire_annotation


@dataclass(frozen=True, slots=True)
class RpcMethodDefinition:
    name: str
    handler_name: str
    request_name: str
    params: type[BaseModel] | None
    result: Any
    summary: str | None = None
    raises: tuple[type[RpcError], ...] = ()
    server: str | None = None
    function: FunctionType | None = None
    injected_parameters: tuple[RpcInjectedParameter, ...] = ()
    resolver_scope: RpcResolverScope | None = None
    params_parameter: str | None = None
    params_origin: type[BaseModel] | None = None


@dataclass(frozen=True, slots=True)
class RpcClientMethod[ParamsT: BaseModel | None, ResultT]:
    """A request the server sends to the client, which answers it."""

    name: str
    params: type[ParamsT] | None
    result: Any
    summary: str | None = None
    raises: tuple[type[RpcError], ...] = ()
    server: str | None = None


@dataclass(frozen=True, slots=True)
class RpcNotificationDefinition:
    name: str
    payload: Any
    summary: str | None = None
    server: str | None = None
    function: FunctionType | None = None
    injected_parameters: tuple[RpcInjectedParameter, ...] = ()
    on_error: str = "continue"
    raises: tuple[type[RpcError], ...] = ()


@dataclass(frozen=True, slots=True)
class RpcSubscriptionDefinition:
    name: str
    params: type[BaseModel] | None
    payload: Any
    function: FunctionType
    injected_parameters: tuple[RpcInjectedParameter, ...]
    params_parameter: str | None
    params_origin: type[BaseModel] | None
    resolver_scope: RpcResolverScope
    summary: str | None = None
    server: str | None = None
    raises: tuple[type[RpcError], ...] = ()


@dataclass(frozen=True, slots=True)
class RpcStreamDefinition:
    name: str
    function: FunctionType
    content_type: str
    summary: str | None
    injected_parameters: tuple[RpcInjectedParameter, ...]
    resolver_scope: RpcResolverScope
    server: str | None = None
    direction: RpcStreamDirection = RpcStreamDirection.SERVER_TO_CLIENT
    input_content_type: str | None = None
    path_parameters: tuple[str, ...] = ()
    path_model: type[BaseModel] | None = None

    @property
    def has_input(self) -> bool:
        return self.direction is not RpcStreamDirection.SERVER_TO_CLIENT

    @property
    def is_generator(self) -> bool:
        return inspect.isasyncgenfunction(self.function)


@dataclass(frozen=True, slots=True)
class RpcNotificationTypeDefinition:
    name: str
    payload: type[BaseModel]


class RpcProtocol:
    """The immutable protocol materialized by an ``RpcChannel``."""

    def __init__(
        self,
        *,
        methods: Iterable[RpcMethodDefinition] = (),
        notifications: Iterable[RpcNotificationDefinition] = (),
        subscriptions: Iterable[RpcSubscriptionDefinition] = (),
        notification_types: Iterable[RpcNotificationTypeDefinition] = (),
        streams: Iterable[RpcStreamDefinition] = (),
        client_methods: Iterable[RpcClientMethod[Any, Any]] = (),
        version: int = 1,
    ) -> None:
        self._version = version
        self._methods = _unique(
            "method",
            ((method.name, method) for method in methods),
        )
        self._notifications = _unique(
            "notification",
            ((notification.name, notification) for notification in notifications),
        )
        self._subscriptions = _unique(
            "subscription", ((item.name, item) for item in subscriptions)
        )
        self._notification_types = _unique(
            "notification type",
            ((item.name, item) for item in notification_types),
        )
        self._streams = _unique("stream", ((item.name, item) for item in streams))
        self._client_methods = _unique(
            "client method", ((item.name, item) for item in client_methods)
        )
        for operation in (*self.methods, *self.notifications, *self.subscriptions):
            bind_contracts(
                (
                    contract
                    for error in operation.raises
                    if (contract := contract_of(error)) is not None
                ),
                owner=f"RPC operation {operation.name}",
            )

    @property
    def version(self) -> int:
        return self._version

    @property
    def methods(self) -> tuple[RpcMethodDefinition, ...]:
        return tuple(self._methods.values())

    @property
    def notifications(self) -> tuple[RpcNotificationDefinition, ...]:
        return tuple(self._notifications.values())

    @property
    def subscriptions(self) -> tuple[RpcSubscriptionDefinition, ...]:
        return tuple(self._subscriptions.values())

    @property
    def notification_types(self) -> tuple[RpcNotificationTypeDefinition, ...]:
        return tuple(self._notification_types.values())

    @property
    def streams(self) -> tuple[RpcStreamDefinition, ...]:
        return tuple(self._streams.values())

    @property
    def client_methods(self) -> tuple[RpcClientMethod[Any, Any], ...]:
        return tuple(self._client_methods.values())

    def method(self, name: str) -> RpcMethodDefinition:
        try:
            return self._methods[name]
        except KeyError as error:
            raise RpcMethodNotFoundError(name) from error


def method_definition(
    *,
    name: str,
    function: FunctionType,
    handler_name: str,
    summary: str | None,
    raises: tuple[type[RpcError], ...],
    server: str | None,
    resolver_scope: RpcResolverScope,
    request_name: str | None = None,
) -> RpcMethodDefinition:
    request_name = request_name or f"{_pascal_case(handler_name)}Request"
    params, params_parameter, injected = _router_params_model(function)
    return RpcMethodDefinition(
        name=name,
        handler_name=handler_name,
        request_name=request_name,
        params=params,
        result=wire_annotation(_result_annotation(function)),
        summary=summary,
        raises=raises,
        server=server,
        function=function,
        injected_parameters=injected,
        resolver_scope=resolver_scope,
        params_parameter=params_parameter,
        params_origin=(
            get_type_hints(function, include_extras=True)[params_parameter]
            if params_parameter is not None
            else None
        ),
    )


def client_method_definition(
    *,
    name: str,
    params: Any,
    result: Any,
    summary: str | None,
    raises: tuple[type[RpcError], ...],
) -> RpcClientMethod[Any, Any]:
    if params is not None and not _is_model(params):
        raise ProtocolDefinitionError(
            f"RPC client method {name} params must be a Pydantic model or None"
        )
    result = type(None) if result is None else result
    try:
        TypeAdapter(result).json_schema()
    except PydanticSchemaGenerationError as error:
        raise ProtocolDefinitionError(
            f"RPC client method {name} result {result!r} has no JSON schema"
        ) from error
    return RpcClientMethod(
        name=name,
        params=None if params is None else wire_annotation(params),
        result=wire_annotation(result),
        summary=summary,
        raises=raises,
    )


def _router_params_model(
    function: Any,
) -> tuple[
    type[BaseModel] | None,
    str | None,
    tuple[RpcInjectedParameter, ...],
]:
    hints, injected, wire_parameters = _split_parameters(function)
    if not wire_parameters:
        return None, None, injected

    if len(wire_parameters) == 1:
        parameter = wire_parameters[0]
        annotation = hints[parameter.name]
        if parameter.kind in (inspect.Parameter.POSITIONAL_OR_KEYWORD,) and _is_model(
            annotation
        ):
            return wire_annotation(annotation), parameter.name, injected

    names = ", ".join(parameter.name for parameter in wire_parameters)
    raise ProtocolDefinitionError(
        f"RPC handler {function.__qualname__} must use at most one positional "
        f"Pydantic params model; unsupported wire parameters: {names}"
    )


def _split_parameters(
    function: Any,
) -> tuple[
    dict[str, Any],
    tuple[RpcInjectedParameter, ...],
    list[inspect.Parameter],
]:
    parameters = tuple(inspect.signature(function).parameters.values())
    hints = get_type_hints(function, include_extras=True)
    injected: list[RpcInjectedParameter] = []
    wire_parameters: list[inspect.Parameter] = []
    for parameter in parameters:
        annotation = hints.get(parameter.name)
        if annotation is None:
            raise ProtocolDefinitionError(
                f"RPC handler {function.__qualname__} parameter "
                f"{parameter.name!r} needs an annotation"
            )
        try:
            dependency = injected_parameter(parameter.name, annotation)
        except TypeError as error:
            raise ProtocolDefinitionError(str(error)) from error
        if dependency is None:
            wire_parameters.append(parameter)
            continue
        if parameter.kind in (
            inspect.Parameter.POSITIONAL_ONLY,
            inspect.Parameter.VAR_POSITIONAL,
            inspect.Parameter.VAR_KEYWORD,
        ):
            raise ProtocolDefinitionError(
                f"Injected RPC parameter {parameter.name!r} must be passable by name"
            )
        if parameter.default is not inspect.Parameter.empty:
            raise ProtocolDefinitionError(
                f"Injected RPC parameter {parameter.name!r} cannot have a default"
            )
        injected.append(dependency)
    return hints, tuple(injected), wire_parameters


def _result_annotation(function: Any) -> Any:
    result = get_type_hints(function, include_extras=True).get("return")
    if result is None:
        raise ProtocolDefinitionError(
            f"RPC handler {function.__qualname__} needs a return annotation"
        )
    return result


def notification_type_definitions(
    annotation: Any,
) -> tuple[RpcNotificationTypeDefinition, ...]:
    definitions: list[RpcNotificationTypeDefinition] = []
    messages = _notification_message_types(annotation)
    for message in messages:
        name = _declared_notification_type(message)
        if not isinstance(name, str):
            if len(messages) == 1:
                continue
            raise ProtocolDefinitionError(
                f"RPC notification type {message.__name__} needs a type field "
                "pinned to a string literal when used in a union"
            )
        definitions.append(RpcNotificationTypeDefinition(name, message))
    return tuple(definitions)


def notification_definition(
    *,
    name: str,
    payload: Any | None,
    function: FunctionType,
    summary: str | None,
    server: str | None,
    on_error: str = "continue",
    raises: tuple[type[RpcError], ...] = (),
) -> RpcNotificationDefinition:
    if not inspect.isasyncgenfunction(function):
        raise ProtocolDefinitionError(
            f"RPC event source {function.__qualname__} must be an async generator"
        )
    hints = get_type_hints(function, include_extras=True)
    result = hints.get("return")
    if result is None:
        raise ProtocolDefinitionError(
            f"RPC event source {function.__qualname__} needs a return annotation"
        )
    origin = get_origin(result)
    if origin not in (AsyncIterator, AsyncGenerator):
        raise ProtocolDefinitionError(
            f"RPC event source {function.__qualname__} must return "
            "AsyncIterator[Payload]"
        )
    yielded = get_args(result)[0]
    if payload is None:
        payload = yielded
    if yielded != payload:
        raise ProtocolDefinitionError(
            f"RPC event source {function.__qualname__} yields {yielded!r}, "
            f"expected {payload!r}"
        )
    params, _, injected = _router_params_model(function)
    if params is not None:
        raise ProtocolDefinitionError(
            f"RPC event source {function.__qualname__} parameters must use Inject[T]"
        )
    return RpcNotificationDefinition(
        name=name,
        payload=payload,
        summary=summary,
        server=server,
        function=function,
        injected_parameters=injected,
        on_error=on_error,
        raises=raises,
    )


def subscription_definition(
    *,
    name: str,
    function: FunctionType,
    summary: str | None,
    resolver_scope: RpcResolverScope,
    raises: tuple[type[RpcError], ...] = (),
) -> RpcSubscriptionDefinition:
    if not inspect.isasyncgenfunction(function):
        raise ProtocolDefinitionError(
            f"RPC subscription {function.__qualname__} must be an async generator"
        )
    hints = get_type_hints(function, include_extras=True)
    result = hints.get("return")
    if get_origin(result) not in (AsyncIterator, AsyncGenerator):
        raise ProtocolDefinitionError(
            f"RPC subscription {function.__qualname__} must return "
            "AsyncIterator[Payload]"
        )
    payload = get_args(result)[0]
    TypeAdapter(payload).json_schema()
    params, params_parameter, injected = _router_params_model(function)
    return RpcSubscriptionDefinition(
        name=name,
        params=params,
        payload=payload,
        function=function,
        injected_parameters=injected,
        params_parameter=params_parameter,
        params_origin=(
            hints[params_parameter] if params_parameter is not None else None
        ),
        resolver_scope=resolver_scope,
        summary=summary,
        raises=raises,
    )


def stream_definition(
    *,
    name: str,
    function: FunctionType,
    content_type: str,
    summary: str | None,
    resolver_scope: RpcResolverScope,
    input_content_type: str | None = None,
) -> RpcStreamDefinition:
    qualname = function.__qualname__
    hints, injected, path_parameters = _split_parameters(function)
    counts = Counter(parameter.dependency for parameter in injected)
    for dependency in (RpcBinaryInput, RpcBinaryOutput):
        if counts[dependency] > 1:
            raise ProtocolDefinitionError(
                f"RPC binary stream {qualname} injects {dependency.__name__} "
                "more than once"
            )
    has_input = RpcBinaryInput in counts
    has_output = RpcBinaryOutput in counts
    result = hints.get("return")
    if inspect.isasyncgenfunction(function):
        if (
            get_origin(result) not in (AsyncIterator, AsyncGenerator)
            or get_args(result)[0] is not bytes
        ):
            raise ProtocolDefinitionError(
                f"RPC binary stream {qualname} must yield bytes and be annotated "
                "as AsyncIterator[bytes]"
            )
        if has_input or has_output:
            raise ProtocolDefinitionError(
                f"RPC binary stream {qualname} is an async generator and cannot "
                "inject RpcBinaryInput or RpcBinaryOutput; write an async "
                "function that injects them and calls `await output.send(frame)` "
                "instead of yielding"
            )
        direction = RpcStreamDirection.SERVER_TO_CLIENT
    elif inspect.iscoroutinefunction(function):
        if not (has_input or has_output):
            raise ProtocolDefinitionError(
                f"RPC binary stream {qualname} must be an async generator "
                "yielding bytes or an async function that injects "
                "RpcBinaryInput and/or RpcBinaryOutput"
            )
        if result is not None and result is not type(None):
            raise ProtocolDefinitionError(
                f"RPC binary stream {qualname} must return None; send output "
                "frames through RpcBinaryOutput"
            )
        if has_input and has_output:
            direction = RpcStreamDirection.BIDIRECTIONAL
        elif has_input:
            direction = RpcStreamDirection.CLIENT_TO_SERVER
        else:
            direction = RpcStreamDirection.SERVER_TO_CLIENT
    else:
        raise ProtocolDefinitionError(
            f"RPC binary stream {qualname} must be an async generator or an "
            "async function"
        )
    if input_content_type is not None and not has_input:
        raise ProtocolDefinitionError(
            f"RPC binary stream {qualname} declares input_content_type but does "
            "not inject RpcBinaryInput"
        )
    return RpcStreamDefinition(
        name=name,
        function=function,
        content_type=content_type,
        summary=summary,
        injected_parameters=injected,
        resolver_scope=resolver_scope,
        direction=direction,
        input_content_type=(input_content_type or content_type) if has_input else None,
        path_parameters=tuple(parameter.name for parameter in path_parameters),
        path_model=_path_model(function, hints, path_parameters),
    )


def _path_model(
    function: Any,
    hints: dict[str, Any],
    parameters: list[inspect.Parameter],
) -> type[BaseModel] | None:
    if not parameters:
        return None
    fields: dict[str, Any] = {}
    for parameter in parameters:
        if parameter.kind not in (
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
            inspect.Parameter.KEYWORD_ONLY,
        ):
            raise ProtocolDefinitionError(
                f"RPC binary stream path parameter {parameter.name!r} must be "
                "passable by name"
            )
        annotation = hints[parameter.name]
        if _is_model(annotation):
            raise ProtocolDefinitionError(_path_type_message(parameter.name))
        default = (
            ... if parameter.default is inspect.Parameter.empty else parameter.default
        )
        fields[parameter.name] = (annotation, default)
    try:
        return create_model(f"{_pascal_case(function.__name__)}PathParams", **fields)
    except PydanticSchemaGenerationError as error:
        names = ", ".join(fields)
        raise ProtocolDefinitionError(_path_type_message(names)) from error


def _path_type_message(name: str) -> str:
    return (
        f"RPC binary stream parameter {name!r} is not a path variable type; "
        "path variables are scalars such as str, int, UUID, or an Enum, and "
        "dependencies use Inject[T]"
    )


def _notification_message_types(annotation: Any) -> tuple[type[BaseModel], ...]:
    value = (
        annotation.__value__ if isinstance(annotation, TypeAliasType) else annotation
    )
    if get_origin(value) is Annotated:
        value = get_args(value)[0]
    members = get_args(value) if get_origin(value) in (Union, UnionType) else (value,)
    if not all(_is_model(member) for member in members):
        raise ProtocolDefinitionError("RPC event payload must contain Pydantic models")
    return members


def _declared_notification_type(message: type[BaseModel]) -> Any:
    schema = message.model_json_schema()
    return schema.get("properties", {}).get("type", {}).get("const")


def _unique[ValueT](
    kind: str,
    entries: Iterable[tuple[str, ValueT]],
) -> dict[str, ValueT]:
    unique: dict[str, ValueT] = {}
    for name, value in entries:
        if name in unique:
            raise ProtocolDefinitionError(f"Duplicate RPC {kind}: {name}")
        unique[name] = value
    return unique


def _is_model(annotation: Any) -> bool:
    return isinstance(annotation, type) and issubclass(annotation, BaseModel)


def _pascal_case(value: str) -> str:
    return "".join(part.capitalize() for part in value.split("_"))

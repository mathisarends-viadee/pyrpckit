import json
import keyword
from collections.abc import Iterable
from dataclasses import dataclass, replace
from typing import Any

from rpckit.codegen.ir import (
    BinaryStreamDecl,
    ClientIr,
    EnumDecl,
    EnumLiteralType,
    FieldDecl,
    ListType,
    LiteralType,
    MapType,
    ModelDecl,
    NamedType,
    NotificationDecl,
    Primitive,
    PrimitiveType,
    RouteDecl,
    ServerDecl,
    ServerVariableDecl,
    TypeExpr,
    UnionType,
    UnsupportedSchemaError,
    named_types,
)
from rpckit.codegen.names import (
    NamespaceViewNode,
    assert_unique_names,
    client_view,
    pascal_case,
    snake_case,
)
from rpckit.codegen.options import PythonClientOptions
from rpckit.codegen.templating import render_template


class _Imports:
    def __init__(self, relative_to: str | None = None) -> None:
        self._modules: dict[str, set[str]] = {}
        self._plain: set[str] = set()
        self._relative_to = relative_to

    def add(self, module: str, *names: str) -> None:
        if names:
            self._modules.setdefault(self._resolve(module), set()).update(names)

    def add_module(self, module: str) -> None:
        """Import a whole module, as in ``import asyncio``."""
        self._plain.add(module)

    def _resolve(self, module: str) -> str:
        """Render imports of the own package as relative imports."""
        package = self._relative_to
        if package is None:
            return module
        if module == package:
            return "."
        if module.startswith(f"{package}."):
            return f".{module[len(package) + 1 :]}"
        return module

    def render(self) -> str:
        groups: dict[int, list[str]] = {}
        for module in sorted(self._plain):
            groups.setdefault(_import_group(module), []).append(f"import {module}")
        for module in sorted(self._modules):
            groups.setdefault(_import_group(module), []).append(
                _import_line(
                    module,
                    sorted(
                        self._modules[module],
                        key=_import_name_key,
                    ),
                )
            )
        return "\n\n".join("\n".join(groups[rank]) for rank in sorted(groups))


def render_files(ir: ClientIr, options: PythonClientOptions) -> dict[str, str]:
    """Render one generated Python leaf package."""
    view = client_view(ir, api_root=options.api_root, api_names=options.api_names)
    client_name = _client_name(ir, options)
    _validate(
        ir,
        view.root_operations,
        view.root_streams,
        view.nodes,
        options,
        client_name,
    )
    files = {
        "__init__.py": _render_package_init(ir, options, client_name, view.nodes),
        **_render_runtime(ir, options),
        "client.py": _render_client(
            ir,
            view.root_operations,
            view.nodes,
            view.root_notifications,
            view.root_streams,
            options,
            client_name,
        ),
    }
    if ir.declarations:
        files["models.py"] = _render_models(ir, options)
    if ir.operations or ir.notifications:
        files["routes.py"] = _render_routes(ir, options)
    files["errors.py"] = _render_errors(ir, options)
    if ir.servers:
        files["endpoints.py"] = _render_endpoints(ir, options)
    if ir.binary_streams:
        files["streams.py"] = _render_streams(ir, options)
    if ir.client_methods:
        files["handlers.py"] = _render_handlers(ir, options)
    if options.with_transport == "websocket":
        files["transport.py"] = _render_websocket_transport(options)
    if view.nodes:
        files["namespaces/__init__.py"] = _render_namespaces_init(view.nodes, options)
        for node in view.nodes:
            files[_api_file(node)] = _render_api(node, ir, options)
    for name, content in options.extra_files.items():
        if name != _identifier(name.removesuffix(".py")) + ".py":
            raise UnsupportedSchemaError(f"Invalid extra Python file name: {name!r}")
        if name in files:
            raise UnsupportedSchemaError(
                f"Extra file conflicts with generated file: {name}"
            )
        files[name] = content
    return files


def _render_runtime(ir: ClientIr, options: PythonClientOptions) -> dict[str, str]:
    files = {
        "internal/__init__.py": _render_runtime_init(ir, options),
        "internal/core.py": _render_runtime_module(options, "core"),
        "internal/errors.py": _render_runtime_module(options, "errors"),
        "internal/metadata.py": _render_runtime_module(options, "metadata"),
        "internal/transport.py": _render_runtime_module(options, "transport"),
        "internal/unset.py": _render_runtime_module(options, "unset"),
    }
    if options.with_transport == "websocket":
        files["internal/connection.py"] = _render_runtime_module(
            options,
            "connection",
        )
    if ir.client_methods:
        files["internal/client_methods.py"] = _render_runtime_module(
            options, "client_methods"
        )
    return files


def _render_runtime_init(ir: ClientIr, options: PythonClientOptions) -> str:
    imports = _Imports()
    exported = [
        "UNSET",
        "RpcClientCore",
        "RpcClientHook",
        "RpcTransportSource",
        "UnsetType",
    ]
    if ir.client_methods:
        imports.add(
            ".client_methods", "RpcClientMethodDispatcher", "RpcClientMethodInfo"
        )
        exported.extend(["RpcClientMethodDispatcher", "RpcClientMethodInfo"])
    if options.with_transport == "websocket":
        imports.add(".connection", "ClientConnection", "RpcTransportPool")
        exported.extend(["ClientConnection", "RpcTransportPool"])
    imports.add(".core", "RpcClientCore", "RpcClientHook", "RpcTransportSource")
    imports.add(
        ".errors",
        "RpcClientError",
        "RpcNotificationValidationError",
        "RpcRemoteError",
        "RpcResponseValidationError",
        "RpcStreamClosed",
        "RpcStreamFailed",
        "RpcStreamRefused",
        "RpcStreamsUnavailableError",
        "RpcTransportError",
    )
    imports.add(
        ".metadata",
        "JsonValue",
        "RpcContractInfo",
        "RpcNotificationInfo",
        "RpcRouteInfo",
        "RpcServerInfo",
        "RpcServerVariable",
        "RpcTransportDescriptor",
        "resolve_url_template",
    )
    imports.add(".transport", "RpcTransport")
    imports.add(".unset", "UNSET", "UnsetType")
    exported.extend(
        [
            "JsonValue",
            "RpcClientError",
            "RpcContractInfo",
            "RpcNotificationInfo",
            "RpcNotificationValidationError",
            "RpcRemoteError",
            "RpcResponseValidationError",
            "RpcRouteInfo",
            "RpcServerInfo",
            "RpcServerVariable",
            "RpcStreamClosed",
            "RpcStreamFailed",
            "RpcStreamRefused",
            "RpcStreamsUnavailableError",
            "RpcTransport",
            "RpcTransportDescriptor",
            "RpcTransportError",
            "resolve_url_template",
        ]
    )
    body = render_template(
        "python/package_init.py.j2",
        filters={"literal": _literal},
        exports=sorted(exported),
    ).rstrip()
    return _module(options, imports, body)


def _render_runtime_module(options: PythonClientOptions, name: str) -> str:
    body = render_template(f"python/runtime/{name}.py.j2").rstrip()
    return _module(options, _Imports(), body)


def _runtime_module(options: PythonClientOptions) -> str:
    return f"{options.package}.internal"


def _render_models(ir: ClientIr, options: PythonClientOptions) -> str:
    imports = _Imports()
    if ir.models:
        imports.add("pydantic", "BaseModel", "ConfigDict")
    for declaration in ir.declarations:
        if isinstance(declaration, EnumDecl):
            imports.add("enum", "IntEnum" if declaration.integral else "StrEnum")
        elif isinstance(declaration, ModelDecl) and declaration.closed:
            imports.add("pydantic", "ConfigDict")
    body = render_template(
        "python/models.py.j2",
        filters=_template_filters(imports, options),
        declarations=ir.declarations,
        models=ir.models,
        base_model_name=options.base_model_name,
    )
    body = _collapse_blank_lines(body)
    return _module(options, imports, body, future_annotations=True)


def _render_routes(ir: ClientIr, options: PythonClientOptions) -> str:
    imports = _Imports()
    if ir.operations or ir.notifications:
        imports.add("pydantic", "TypeAdapter")
    if ir.operations:
        imports.add(_runtime_module(options), "RpcRouteInfo")
    if ir.notifications:
        imports.add(_runtime_module(options), "RpcNotificationInfo")
    for route in ir.operations:
        params_models = (
            () if route.params_model is None else (_schema_name(route.params_model),)
        )
        imports.add(
            f"{options.package}.models",
            *params_models,
            *_model_names(route.result),
        )
    for event in ir.notifications:
        if event.errors:
            imports.add(f"{options.package}.errors", "error_from_response")
        imports.add(
            f"{options.package}.models",
            *_model_names(event.message),
            *_model_names(event.payload),
            *(
                ()
                if event.params_model is None
                else (_schema_name(event.params_model),)
            ),
        )
    body = render_template(
        "python/routes.py.j2",
        filters=_template_filters(imports, options),
        routes=ir.operations,
        notifications=ir.notifications,
    )
    return _module(options, imports, body)


def _render_endpoints(ir: ClientIr, options: PythonClientOptions) -> str:
    imports = _Imports()
    imports.add("collections.abc", "Mapping")
    imports.add("dataclasses", "dataclass", "replace")
    imports.add("enum", "StrEnum")
    imports.add(
        _runtime_module(options),
        "RpcServerInfo",
        "RpcTransportDescriptor",
    )
    if any(server.variables for server in ir.servers):
        imports.add(_runtime_module(options), "RpcServerVariable")
    if any(variable.enum for server in ir.servers for variable in server.variables):
        imports.add("typing", "Literal")

    body = render_template(
        "python/endpoints.py.j2",
        filters=_template_filters(imports, options),
        servers=ir.servers,
    )
    body = _collapse_blank_lines(body)
    return _module(options, imports, body)


def _render_streams(ir: ClientIr, options: PythonClientOptions) -> str:
    imports = _Imports()
    imports.add_module("asyncio")
    imports.add("collections.abc", "AsyncIterator", "Awaitable", "Callable", "Mapping")
    imports.add("contextlib", "asynccontextmanager")
    imports.add("dataclasses", "dataclass", "field")
    imports.add("enum", "StrEnum")
    imports.add("types", "MappingProxyType")
    imports.add("typing", "Any", "Literal", "Protocol", "Self")
    imports.add("pydantic", "BaseModel", "ConfigDict")
    imports.add(
        _runtime_module(options),
        "RpcServerVariable",
        "RpcStreamClosed",
        "RpcStreamsUnavailableError",
        "RpcTransportError",
        "resolve_url_template",
    )
    if options.with_transport == "websocket":
        imports.add(_runtime_module(options), "RpcStreamFailed", "RpcStreamRefused")
    body = render_template(
        "python/streams.py.j2",
        filters=_template_filters(imports, options),
        streams=ir.binary_streams,
        with_websocket=options.with_transport == "websocket",
    )
    body = _collapse_blank_lines(body)
    return _module(options, imports, body)


@dataclass(frozen=True, slots=True)
class _HandlerGroup:
    class_name: str
    namespace: str
    client_methods: tuple[RouteDecl, ...]


def _handler_groups(ir: ClientIr) -> tuple[_HandlerGroup, ...]:
    grouped: dict[tuple[str, ...], list[RouteDecl]] = {}
    for route in ir.client_methods:
        grouped.setdefault(route.path, []).append(route)
    return tuple(
        _HandlerGroup(_handler_class(path), ".".join(path), tuple(routes))
        for path, routes in grouped.items()
    )


def _handler_class(path: tuple[str, ...]) -> str:
    return f"{_api_class(path) or 'Root'}Handler"


def _render_handlers(ir: ClientIr, options: PythonClientOptions) -> str:
    imports = _Imports()
    imports.add("abc", "ABC", "abstractmethod")
    imports.add("collections.abc", "Iterable")
    imports.add("pydantic", "TypeAdapter")
    imports.add(
        _runtime_module(options), "RpcClientMethodDispatcher", "RpcClientMethodInfo"
    )
    for route in ir.client_methods:
        if route.params_model is not None:
            imports.add(f"{options.package}.models", _schema_name(route.params_model))
        imports.add(f"{options.package}.models", *_model_names(route.result))
    filters = _template_filters(imports, options)
    filters["client_method_signature"] = lambda route: _client_method_signature(
        route, imports
    )
    body = render_template(
        "python/handlers.py.j2",
        filters=filters,
        groups=_handler_groups(ir),
        handler_alias=_handler_alias(ir),
    )
    body = _collapse_blank_lines(body)
    return _module(options, imports, body)


def _handler_alias(ir: ClientIr) -> str:
    names = [group.class_name for group in _handler_groups(ir)]
    inline = f"type Handler = {' | '.join(names)}"
    if len(inline) <= 88:
        return inline
    members = "\n    | ".join(names)
    return f"type Handler = (\n    {members}\n)"


def _client_method_signature(route: RouteDecl, imports: _Imports) -> str:
    name = _identifier(route.operation_name)
    result = _annotation(route.result, imports)
    parameters = ["self"]
    if route.params_model is not None:
        parameters.append(f"params: {_schema_name(route.params_model)}")
    inline = f"    async def {name}({', '.join(parameters)}) -> {result}:"
    if len(inline) <= 88:
        return inline
    lines = "".join(f"        {parameter},\n" for parameter in parameters)
    return f"    async def {name}(\n{lines}    ) -> {result}:"


def _server_subprotocols(server: ServerDecl) -> str:
    if server.transport is None:
        return "()"
    return _tuple_literal(server.transport.subprotocols)


def _render_errors(ir: ClientIr, options: PythonClientOptions) -> str:
    imports = _Imports()
    imports.add("typing", "Any")
    imports.add("pydantic", "ValidationError")
    imports.add(_runtime_module(options), "RpcRemoteError")
    errors = []
    seen: set[str] = set()
    client_method_codes = {
        error.code for route in ir.client_methods for error in route.errors
    }
    if client_method_codes:
        imports.add("typing", "Self")
    for route in (*ir.operations, *ir.client_methods, *ir.notifications):
        for error in route.errors:
            if error.name is None or error.name in seen:
                continue
            seen.add(error.name)
            imports.add("typing", "ClassVar")
            if error.data is not None:
                imports.add("pydantic", "TypeAdapter")
                imports.add(f"{options.package}.models", *_model_names(error.data))
            errors.append(error)
    body = render_template(
        "python/errors.py.j2",
        filters=_template_filters(imports, options),
        errors=errors,
        client_method_codes=client_method_codes,
    )
    body = _collapse_blank_lines(body.lstrip("\n"))
    return _module(options, imports, body)


def _add_notification_imports(
    event: NotificationDecl, imports: _Imports, options: PythonClientOptions
) -> None:
    imports.add("collections.abc", "AsyncIterator")
    imports.add(f"{options.package}.models", *_model_names(event.payload))
    if event.params_model is not None:
        imports.add(f"{options.package}.models", _schema_name(event.params_model))
    for parameter in event.params:
        imports.add(f"{options.package}.models", *_model_names(parameter.type))
        _parameter_annotation(parameter, imports, options)
    if event.params_model is not None:
        imports.add(f"{options.package}.models", _schema_name(event.params_model))
    imports.add(f"{options.package}.routes", _constant(event.rpc_name))
    _annotation(event.payload, imports)


def _render_api(
    root: NamespaceViewNode,
    ir: ClientIr,
    options: PythonClientOptions,
) -> str:
    imports = _Imports()
    imports.add(_runtime_module(options), "RpcClientCore")
    nodes = tuple(_walk_postorder((root,)))
    if any(node.streams for node in nodes):
        imports.add("contextlib", "AbstractAsyncContextManager")
        imports.add(
            f"{options.package}.streams",
            "BINARY_STREAMS",
            "BinaryStreamName",
            "open_binary_stream",
            "resolve_stream_endpoint",
        )
    for node in nodes:
        for route in node.operations:
            _add_operation_imports(route, imports, options)
        for event in node.notifications:
            _add_notification_imports(event, imports, options)
    body = render_template(
        "python/api.py.j2",
        filters=_template_filters(imports, options),
        nodes=nodes,
    )
    body = _collapse_blank_lines(body)
    return _module(options, imports, body)


def _render_namespaces_init(
    nodes: tuple[NamespaceViewNode, ...],
    options: PythonClientOptions,
) -> str:
    imports = _Imports()
    exported = []
    for node in nodes:
        api_class = _api_class(node.path)
        imports.add(f".{_identifier(node.path[0])}", api_class)
        exported.append(api_class)
    body = render_template(
        "python/package_init.py.j2",
        filters={"literal": _literal},
        exports=sorted(exported),
    ).rstrip()
    return _module(options, imports, body)


def _render_client(
    ir: ClientIr,
    root_operations: tuple[RouteDecl, ...],
    nodes: tuple[NamespaceViewNode, ...],
    root_events: tuple[NotificationDecl, ...],
    root_streams: tuple[BinaryStreamDecl, ...],
    options: PythonClientOptions,
    client_name: str,
) -> str:
    imports = _Imports()
    imports.add_module("asyncio")
    imports.add("collections.abc", "Iterable", "Mapping")
    imports.add("typing", "Self")
    imports.add(
        _runtime_module(options),
        "RpcClientCore",
        "RpcClientHook",
        "RpcTransport",
        "RpcTransportSource",
    )
    if ir.binary_streams:
        imports.add(f"{options.package}.streams", "BinaryStreamOpener")
        if root_streams:
            imports.add("contextlib", "AbstractAsyncContextManager")
            imports.add(
                f"{options.package}.streams",
                "BINARY_STREAMS",
                "BinaryStreamName",
                "open_binary_stream",
                "resolve_stream_endpoint",
            )
        if options.with_transport == "websocket":
            imports.add(
                f"{options.package}.streams",
                "BinaryStreamEndpoint",
                "BinaryStreamTransport",
                "BinaryWebSocketFactory",
                "BinaryWebSocketStream",
            )
    if ir.servers:
        imports.add(f"{options.package}.endpoints", "ServerName")
    if options.with_transport == "websocket" and ir.client_methods:
        imports.add(
            f"{options.package}.handlers",
            "Handler",
            "handler_dispatcher",
        )
    if options.with_transport == "websocket":
        imports.add("collections.abc", "Awaitable", "Callable")
        imports.add(_runtime_module(options), "ClientConnection", "RpcTransportPool")
        imports.add(
            f"{options.package}.endpoints",
            "Endpoint",
            "resolve_endpoints",
        )
        imports.add(
            f"{options.package}.transport",
            "WebSocketFactory",
            "WebSocketTransport",
        )
    for node in nodes:
        imports.add(_namespaces_module(options), _api_class(node.path))
    for route in root_operations:
        _add_operation_imports(route, imports, options)
    for event in root_events:
        _add_notification_imports(event, imports, options)
    body = render_template(
        "python/client.py.j2",
        filters=_template_filters(imports, options),
        client_name=client_name,
        servers=ir.servers,
        variables=_connect_variables(ir),
        nodes=nodes,
        operations=root_operations,
        notifications=root_events,
        streams=root_streams,
        binary_streams=ir.binary_streams,
        client_methods=ir.client_methods,
        with_websocket=options.with_transport == "websocket",
    )
    return _module(options, imports, body)


def _connect_variables(ir: ClientIr) -> tuple[ServerVariableDecl, ...]:
    """The server URL variables ``connect`` accepts; streams inherit them too."""
    declared = {variable.name for server in ir.servers for variable in server.variables}
    merged: dict[str, ServerVariableDecl] = {}
    for declaration in (*ir.servers, *ir.binary_streams):
        for variable in declaration.variables:
            if variable.name not in declared:
                continue
            previous = merged.get(variable.name)
            if previous is None:
                merged[variable.name] = variable
            elif previous.enum != variable.enum:
                merged[variable.name] = replace(previous, enum=())
    return tuple(merged.values())


def _render_package_init(
    ir: ClientIr,
    options: PythonClientOptions,
    client_name: str,
    nodes: tuple[NamespaceViewNode, ...],
) -> str:
    imports = _Imports(relative_to=options.package)
    imports.add(
        _runtime_module(options),
        "RpcClientError",
        "RpcNotificationValidationError",
        "RpcRemoteError",
        "RpcResponseValidationError",
        "RpcTransportError",
    )
    imports.add(f"{options.package}.client", client_name)
    exported = [
        client_name,
        "RpcClientError",
        "RpcNotificationValidationError",
        "RpcRemoteError",
        "RpcResponseValidationError",
        "RpcTransportError",
    ]
    error_names = [_schema_name(name) + "Error" for name in _named_error_codes(ir)]
    if error_names:
        imports.add(f"{options.package}.errors", *error_names)
        exported.extend(error_names)
    declaration_names = [
        _schema_name(declaration.name) for declaration in ir.declarations
    ]
    if declaration_names:
        imports.add(options.package, "models")
        imports.add(f"{options.package}.models", *declaration_names)
        exported.extend([*declaration_names, "models"])
    if nodes:
        namespace_classes = [_api_class(node.path) for node in nodes]
        imports.add(_namespaces_module(options), *namespace_classes)
        exported.extend(namespace_classes)
    if ir.servers:
        imports.add(options.package, "endpoints")
        imports.add(f"{options.package}.endpoints", "Endpoint", "ServerName")
        exported.extend(["Endpoint", "ServerName", "endpoints"])
    if options.with_transport == "websocket":
        imports.add(f"{options.package}.transport", "WebSocketTransport")
        exported.append("WebSocketTransport")
    if ir.client_methods:
        handler_exports = [
            *(group.class_name for group in _handler_groups(ir)),
            "Handler",
            "handler_dispatcher",
        ]
        imports.add(options.package, "handlers")
        imports.add(f"{options.package}.handlers", *handler_exports)
        exported.extend([*handler_exports, "handlers"])
    if ir.binary_streams:
        imports.add(options.package, "streams")
        imports.add(
            f"{options.package}.streams",
            "BinaryChannel",
            "BinaryInputEnd",
            "BinaryInputTransport",
            "BinaryReceiver",
            "BinarySender",
            "BinaryStreamEndpoint",
            "BinaryStreamName",
            "BinaryStreamOpener",
            "BinaryStreamTransport",
        )
        imports.add(
            _runtime_module(options),
            "RpcStreamClosed",
            "RpcStreamFailed",
            "RpcStreamRefused",
            "RpcStreamsUnavailableError",
        )
        exported.extend(
            [
                "BinaryChannel",
                "BinaryInputEnd",
                "BinaryInputTransport",
                "BinaryReceiver",
                "BinarySender",
                "BinaryStreamEndpoint",
                "BinaryStreamName",
                "BinaryStreamOpener",
                "BinaryStreamTransport",
                "RpcStreamClosed",
                "RpcStreamFailed",
                "RpcStreamRefused",
                "RpcStreamsUnavailableError",
                "streams",
            ]
        )
        if options.with_transport == "websocket":
            imports.add(f"{options.package}.streams", "BinaryWebSocketStream")
            exported.append("BinaryWebSocketStream")
    for name, module in options.extra_exports.items():
        if not name.isidentifier() or module + ".py" not in options.extra_files:
            raise UnsupportedSchemaError(
                f"Invalid extra export: {name!r} from {module!r}"
            )
        if name in exported:
            raise UnsupportedSchemaError(
                f"Extra export conflicts with generated name: {name}"
            )
        imports.add(f"{options.package}.{module}", name)
        exported.append(name)
    body = render_template(
        "python/package_init.py.j2",
        filters={"literal": _literal},
        exports=sorted(exported),
    ).rstrip()
    return _module(options, imports, body)


def _add_operation_imports(
    route: RouteDecl, imports: _Imports, options: PythonClientOptions
) -> None:
    models = f"{options.package}.models"
    imports.add(f"{options.package}.routes", route.method_member)
    if route.params_model is not None:
        imports.add(models, _schema_name(route.params_model))
    for parameter in route.params:
        imports.add(models, *_model_names(parameter.type))
    imports.add(models, *_model_names(route.result))
    _annotation(route.result, imports)
    for parameter in route.params:
        _parameter_annotation(parameter, imports, options)


def _field_line(field: FieldDecl, imports: _Imports) -> str:
    annotation = _annotation(field.type, imports)
    name = _identifier(field.name)
    if isinstance(field.type, EnumLiteralType):
        default = f"{_schema_name(field.type.enum)}.{field.type.member}"
    elif field.has_default:
        default = _literal(field.default)
    elif field.required:
        if name == field.name:
            return f"    {name}: {annotation}"
        imports.add("pydantic", "Field")
        return f"    {name}: {annotation} = Field(alias={_literal(field.name)})"
    else:
        if not _allows_none(field.type):
            annotation = _union((annotation, "None"))
        default = "None"
    if name != field.name:
        imports.add("pydantic", "Field")
        default = f"Field({default}, alias={_literal(field.name)})"
    return f"    {name}: {annotation} = {default}"


def _annotation(expression: TypeExpr, imports: _Imports) -> str:
    if isinstance(expression, PrimitiveType):
        return _primitive_annotation(expression.primitive, imports)
    if isinstance(expression, NamedType):
        return _schema_name(expression.name)
    if isinstance(expression, LiteralType):
        imports.add("typing", "Literal")
        return f"Literal[{_literal(expression.value)}]"
    if isinstance(expression, EnumLiteralType):
        imports.add("typing", "Literal")
        return f"Literal[{_schema_name(expression.enum)}.{expression.member}]"
    if isinstance(expression, ListType):
        return f"list[{_annotation(expression.item, imports)}]"
    if isinstance(expression, MapType):
        return f"dict[str, {_annotation(expression.value, imports)}]"
    union = _union(tuple(_annotation(member, imports) for member in expression.members))
    if expression.discriminator is None:
        return union
    imports.add("typing", "Annotated")
    imports.add("pydantic", "Field")
    return (
        f"Annotated[{union}, Field(discriminator={_literal(expression.discriminator)})]"
    )


def _primitive_annotation(primitive: Primitive, imports: _Imports) -> str:
    if primitive is Primitive.UUID:
        imports.add("uuid", "UUID")
        return "UUID"
    if primitive is Primitive.DATETIME:
        imports.add("datetime", "datetime")
        return "datetime"
    if primitive is Primitive.ANY:
        imports.add("typing", "Any")
        return "Any"
    return {
        Primitive.STRING: "str",
        Primitive.INTEGER: "int",
        Primitive.NUMBER: "float",
        Primitive.BOOLEAN: "bool",
        Primitive.NULL: "None",
    }[primitive]


def _parameter_annotation(
    parameter: Any,
    imports: _Imports,
    options: PythonClientOptions,
) -> str:
    annotation = _annotation(parameter.type, imports)
    if parameter.required:
        return annotation
    imports.add(_runtime_module(options), "UNSET", "UnsetType")
    return f"{_union((annotation, 'UnsetType'))} = UNSET"


def _stream_connection(
    stream: BinaryStreamDecl,
    imports: _Imports,
    options: PythonClientOptions,
) -> str:
    if stream.direction == "client-to-server":
        name = "BinarySender"
    elif stream.direction == "bidirectional":
        name = "BinaryChannel"
    else:
        name = "BinaryReceiver"
    imports.add(f"{options.package}.streams", name)
    return name


def _template_filters(
    imports: _Imports,
    options: PythonClientOptions,
) -> dict[str, Any]:
    return {
        "annotation": lambda expression: _annotation(expression, imports),
        "api_class": _api_class,
        "constant": _constant,
        "compact_variable_replace": lambda variable: (
            len(
                "        endpoint_url = endpoint_url.replace("
                + _literal(f"{{{variable.name}}}")
                + ", "
                + _identifier(variable.name)
                + ")"
            )
            <= 88
        ),
        "direct_params": lambda parameters: all(
            parameter.required for parameter in parameters
        ),
        "docstring": _docstring,
        "enum_decl": lambda declaration: isinstance(declaration, EnumDecl),
        "field_line": lambda field: _field_line(field, imports),
        "identifier": _identifier,
        "literal": _literal,
        "model_decl": lambda declaration: isinstance(declaration, ModelDecl),
        "stream_connection": lambda stream: _stream_connection(
            stream, imports, options
        ),
        "null_type": _is_null,
        "options_literal": lambda values: _literal(dict(values)),
        "placeholder": lambda value: _literal(f"{{{value}}}"),
        "parameter_annotation": lambda parameter: _parameter_annotation(
            parameter, imports, options
        ),
        "route_params": lambda route: (
            "None" if route.params_model is None else _schema_name(route.params_model)
        ),
        "schema_name": _schema_name,
        "server_subprotocols": _server_subprotocols,
        "tuple_literal": _tuple_literal,
        "variable_annotation": _variable_annotation,
    }


def _variable_annotation(variable: Any) -> str:
    if not variable.enum:
        return "str"
    return "Literal[" + ", ".join(_literal(value) for value in variable.enum) + "]"


def _validate(
    ir: ClientIr,
    root_operations: tuple[RouteDecl, ...],
    root_streams: tuple[BinaryStreamDecl, ...],
    nodes: tuple[NamespaceViewNode, ...],
    options: PythonClientOptions,
    client_name: str,
) -> None:
    if options.with_transport not in {None, "websocket"}:
        raise UnsupportedSchemaError("with_transport must be None or 'websocket'")
    if options.with_transport == "websocket":
        if not ir.servers:
            raise UnsupportedSchemaError(
                "with_transport='websocket' requires at least one OpenRPC server"
            )
        invalid = [
            server.name
            for server in ir.servers
            if server.transport is None or server.transport.type != "websocket"
        ]
        if invalid:
            raise UnsupportedSchemaError(
                "WebSocket generation requires websocket transport descriptors for: "
                + ", ".join(invalid)
            )
        if len(ir.servers) > 1:
            unassigned = [
                item.rpc_name
                for item in (*ir.operations, *ir.notifications)
                if item.server is None
            ]
            if unassigned:
                raise UnsupportedSchemaError(
                    "Routes need a server when generating multiple WebSocket "
                    "connections: " + ", ".join(unassigned)
                )
    _assert_class_name(client_name, "client_name")
    _assert_class_name(options.base_model_name, "base_model_name")
    assert_unique_names(
        "schemas",
        (
            (declaration.name, pascal_case(declaration.name))
            for declaration in ir.declarations
        ),
    )
    for declaration in ir.declarations:
        generated_name = _schema_name(declaration.name)
        if generated_name in _RUNTIME_NAMES:
            raise UnsupportedSchemaError(
                f"Schema {declaration.name!r} collides with runtime import "
                f"{generated_name!r}"
            )
    for model in ir.models:
        assert_unique_names(
            f"model {model.name}",
            ((field.name, _identifier(field.name)) for field in model.fields),
        )
    for route in ir.operations:
        assert_unique_names(
            f"method {route.rpc_name}",
            (
                (parameter.name, _identifier(parameter.name))
                for parameter in route.params
            ),
        )
    client_members = [
        ("<client.close>", "close"),
        ("<client.from_transport>", "from_transport"),
        *((node.source_path[-1], _identifier(node.segment)) for node in nodes),
        *(
            (route.rpc_name, _identifier(route.operation_name))
            for route in root_operations
        ),
        *(
            (event.rpc_name, _identifier(event.operation_name))
            for event in client_view(
                ir,
                api_root=options.api_root,
                api_names=options.api_names,
            ).root_notifications
        ),
        *(
            (stream.rpc_name, _identifier(stream.operation_name))
            for stream in root_streams
        ),
    ]
    if ir.servers:
        client_members.extend(
            [
                ("<client.from_transports>", "from_transports"),
                ("<client.from_transport_map>", "from_transport_map"),
            ]
        )
    if options.with_transport == "websocket":
        client_members.append(("<client.connect>", "connect"))
        connect_options = [
            *((option, option) for option in _CONNECT_OPTIONS),
            *(
                (variable.name, _identifier(variable.name))
                for variable in _connect_variables(ir)
            ),
        ]
        if len(ir.servers) == 1:
            connect_options.append(("<connect.url>", "url"))
        if ir.binary_streams:
            connect_options.append(
                ("<connect.stream_socket_factory>", "stream_socket_factory")
            )
        if ir.client_methods:
            connect_options.append(("<connect.handlers>", "handlers"))
        assert_unique_names("connect options", connect_options)
    assert_unique_names("root client", client_members)
    _assert_unique_package_exports(ir, options, nodes, client_name)
    _validate_nodes(nodes)
    for group in _handler_groups(ir):
        assert_unique_names(
            f"handlers {group.namespace or '<root>'}",
            (
                (route.rpc_name, _identifier(route.operation_name))
                for route in group.client_methods
            ),
        )
    for node in nodes:
        assert_unique_names(
            f"Python namespace module {_identifier(node.segment)!r}",
            (
                (".".join(item.source_path), _api_class(item.path))
                for item in _walk((node,))
            ),
        )
    assert_unique_names(
        "servers",
        ((server.name, _identifier(server.name)) for server in ir.servers),
    )
    assert_unique_names(
        "binary streams",
        ((stream.name, _identifier(stream.name)) for stream in ir.binary_streams),
    )
    for server in ir.servers:
        assert_unique_names(
            f"server {server.name}",
            (
                (variable.name, _identifier(variable.name))
                for variable in server.variables
            ),
        )


def _assert_unique_package_exports(
    ir: ClientIr,
    options: PythonClientOptions,
    nodes: tuple[NamespaceViewNode, ...],
    client_name: str,
) -> None:
    exports: list[tuple[str, str]] = [("<client>", client_name)]
    exports.extend(("<runtime>", name) for name in _PACKAGE_EXPORTS)
    if ir.servers:
        exports.extend(("<endpoints>", name) for name in ("Endpoint", "ServerName"))
        exports.append(("<endpoints>", "endpoints"))
    if options.with_transport == "websocket":
        exports.append(("<transport>", "WebSocketTransport"))
    if ir.client_methods:
        exports.extend(
            (group.namespace or "<root handlers>", group.class_name)
            for group in _handler_groups(ir)
        )
        exports.extend(
            ("<handlers>", name)
            for name in (
                "Handler",
                "handler_dispatcher",
                "handlers",
            )
        )
    if ir.binary_streams:
        exports.extend(("<streams>", name) for name in _STREAM_EXPORTS)
        exports.append(("<streams>", "streams"))
    if ir.declarations:
        exports.append(("<models>", "models"))
    exports.extend(
        (declaration.name, _schema_name(declaration.name))
        for declaration in ir.declarations
    )
    exports.extend(
        (".".join(node.source_path), _api_class(node.path)) for node in nodes
    )
    exports.extend(
        (code, f"{_schema_name(code)}Error") for code in _named_error_codes(ir)
    )
    assert_unique_names("package exports", exports)


def _validate_nodes(nodes: tuple[NamespaceViewNode, ...]) -> None:
    for node in nodes:
        values = [
            (child.segment, _identifier(child.segment)) for child in node.children
        ]
        values.extend(
            (route.rpc_name, _identifier(route.operation_name))
            for route in node.operations
        )
        values.extend(
            (event.rpc_name, _identifier(event.operation_name))
            for event in node.notifications
        )
        values.extend(
            (stream.rpc_name, _identifier(stream.operation_name))
            for stream in node.streams
        )
        assert_unique_names(f"API path {'.'.join(node.path)}", values)
        _validate_nodes(node.children)


def _client_name(ir: ClientIr, options: PythonClientOptions) -> str:
    return options.client_name or f"{pascal_case(ir.title)}Client"


def _assert_class_name(value: str, option: str) -> None:
    if not value.isidentifier() or keyword.iskeyword(value):
        raise UnsupportedSchemaError(
            f"{option} must be a valid Python identifier: {value!r}"
        )


def _identifier(value: str) -> str:
    identifier = snake_case(value)
    if not identifier:
        raise UnsupportedSchemaError(
            f"Cannot derive a Python identifier from {value!r}"
        )
    if identifier[0].isdigit():
        identifier = f"_{identifier}"
    if keyword.iskeyword(identifier):
        identifier = f"{identifier}_"
    return identifier


def _schema_name(value: str) -> str:
    name = pascal_case(value)
    if not name:
        raise UnsupportedSchemaError(
            f"Cannot derive a Python class name from {value!r}"
        )
    if name[0].isdigit():
        name = f"_{name}"
    return name


def _model_names(expression: TypeExpr) -> set[str]:
    return {_schema_name(name) for name in named_types(expression)}


def _constant(value: str) -> str:
    return _identifier(value).upper()


def _api_class(path: tuple[str, ...]) -> str:
    return "".join(pascal_case(segment) for segment in path)


def _namespaces_module(options: PythonClientOptions) -> str:
    return f"{options.package}.namespaces"


def _api_module(node: NamespaceViewNode) -> str:
    return f"namespaces.{_identifier(node.path[0])}"


def _api_file(node: NamespaceViewNode) -> str:
    return f"namespaces/{_identifier(node.path[0])}.py"


def _walk(nodes: tuple[NamespaceViewNode, ...]) -> Iterable[NamespaceViewNode]:
    for node in nodes:
        yield node
        yield from _walk(node.children)


def _walk_postorder(
    nodes: tuple[NamespaceViewNode, ...],
) -> Iterable[NamespaceViewNode]:
    for node in nodes:
        yield from _walk_postorder(node.children)
        yield node


def _named_errors(ir: ClientIr) -> bool:
    return bool(_named_error_codes(ir))


def _named_error_codes(ir: ClientIr) -> tuple[str, ...]:
    codes: dict[str, None] = {}
    for route in (*ir.operations, *ir.client_methods, *ir.notifications):
        for error in route.errors:
            if error.name is not None:
                codes.setdefault(error.code, None)
    return tuple(codes)


def _render_websocket_transport(options: PythonClientOptions) -> str:
    body = render_template(
        "python/transport.py.j2",
        runtime_module=_runtime_module(options),
    ).rstrip()
    return _module(options, _Imports(), body)


def _collapse(expression: UnionType) -> TypeExpr:
    return expression.members[0] if len(expression.members) == 1 else expression


def _allows_none(expression: TypeExpr) -> bool:
    if isinstance(expression, PrimitiveType):
        return expression.primitive is Primitive.NULL
    if isinstance(expression, UnionType):
        return any(_allows_none(member) for member in expression.members)
    return False


def _is_null(expression: TypeExpr) -> bool:
    return (
        isinstance(expression, PrimitiveType) and expression.primitive is Primitive.NULL
    )


def _union(items: Iterable[str]) -> str:
    return " | ".join(dict.fromkeys(items))


def _docstring(value: str) -> str:
    return " ".join(value.split()).replace('"""', '"""')


def _collapse_blank_lines(body: str) -> str:
    while "\n\n\n\n" in body:
        body = body.replace("\n\n\n\n", "\n\n\n")
    return body


def _module(
    options: PythonClientOptions,
    imports: _Imports,
    body: str,
    *,
    future_annotations: bool = False,
) -> str:
    rendered_imports = imports.render()
    import_spacing = ""
    if rendered_imports:
        if not body:
            import_spacing = "\n"
        elif body.startswith(("class ", "def ", "async def ", "@")):
            import_spacing = "\n\n\n"
        else:
            import_spacing = "\n\n"
    rendered = render_template(
        "python/module.py.j2",
        source=options.source,
        future_annotations=future_annotations,
        imports=rendered_imports,
        import_spacing=import_spacing,
        body=body.rstrip(),
    )
    return f"{rendered.rstrip()}\n"


def _literal(value: Any) -> str:
    if isinstance(value, str):
        return json.dumps(value, ensure_ascii=False)
    if value is None:
        return "None"
    if value is True:
        return "True"
    if value is False:
        return "False"
    if isinstance(value, dict):
        items = ", ".join(f"{_literal(k)}: {_literal(v)}" for k, v in value.items())
        return f"{{{items}}}"
    if isinstance(value, list):
        return f"[{', '.join(_literal(item) for item in value)}]"
    return repr(value)


def _tuple_literal(values: tuple[Any, ...]) -> str:
    if not values:
        return "()"
    items = ", ".join(_literal(value) for value in values)
    suffix = "," if len(values) == 1 else ""
    return f"({items}{suffix})"


def _import_line(module: str, names: list[str]) -> str:
    inline = f"from {module} import {', '.join(names)}"
    if len(inline) <= 88:
        return inline
    values = "".join(f"    {name},\n" for name in names)
    return f"from {module} import (\n{values})"


def _import_name_key(name: str) -> tuple[int, str]:
    if len(name) > 1 and name.isupper():
        group = 0
    elif name[0].isupper():
        group = 1
    else:
        group = 2
    return group, name.casefold()


def _import_group(module: str) -> int:
    if module.startswith("."):
        return 3
    root = module.split(".", 1)[0]
    if root in {
        "__future__",
        "abc",
        "asyncio",
        "collections",
        "contextlib",
        "dataclasses",
        "datetime",
        "enum",
        "types",
        "typing",
        "uuid",
    }:
        return 0
    if root in {"pydantic", "rpckit"}:
        return 1
    return 2


_CONNECT_OPTIONS = (
    "eager",
    "hooks",
    "notification_queue_size",
    "request_timeout",
    "servers",
    "socket_factory",
)

_PACKAGE_EXPORTS = (
    "RpcClientError",
    "RpcNotificationValidationError",
    "RpcRemoteError",
    "RpcResponseValidationError",
    "RpcTransportError",
)

_STREAM_EXPORTS = (
    "BinaryChannel",
    "BinaryInputEnd",
    "BinaryInputTransport",
    "BinaryReceiver",
    "BinarySender",
    "BinaryStreamEndpoint",
    "BinaryStreamName",
    "BinaryStreamOpener",
    "BinaryStreamTransport",
    "BinaryWebSocketStream",
    "RpcStreamClosed",
    "RpcStreamFailed",
    "RpcStreamRefused",
    "RpcStreamsUnavailableError",
)

_RUNTIME_NAMES = frozenset(
    {
        "AsyncIterator",
        "RpcClientCore",
        "RpcRemoteError",
        "RpcTransport",
        "Self",
        "TypeAdapter",
        "UNSET",
        "UnsetType",
    }
)

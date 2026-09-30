import json
import re
from collections.abc import Iterable
from dataclasses import replace

from rpckit.codegen.ir import (
    BinaryStreamDecl,
    ClientIr,
    EnumDecl,
    EnumLiteralType,
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
    camel_case,
    client_view,
    pascal_case,
)
from rpckit.codegen.options import TypeScriptClientOptions
from rpckit.codegen.templating import render_template


def render_files(ir: ClientIr, options: TypeScriptClientOptions) -> dict[str, str]:
    """Render one generated TypeScript leaf package."""
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
    renderer = _Renderer(
        ir,
        view.root_operations,
        view.nodes,
        view.root_notifications,
        view.root_streams,
        options,
        client_name,
    )
    files = {
        "core.ts": renderer.core(),
        "client.ts": renderer.client(),
        "index.ts": renderer.index(),
    }
    if ir.declarations:
        files["models.ts"] = renderer.models()
    if ir.operations or ir.notifications:
        files["routes.ts"] = renderer.routes()
    files["errors.ts"] = renderer.errors()
    if ir.servers:
        files["endpoints.ts"] = renderer.endpoints()
    if ir.binary_streams:
        files["streams.ts"] = renderer.streams()
    if options.with_transport == "websocket":
        files["transport.ts"] = renderer.transport()
    if view.nodes:
        files["namespaces/index.ts"] = renderer.namespaces_index()
        for node in view.nodes:
            files[_api_file(node)] = renderer.api(node)
    return files


class _Renderer:
    def __init__(
        self,
        ir: ClientIr,
        root_operations: tuple[RouteDecl, ...],
        nodes: tuple[NamespaceViewNode, ...],
        root_events: tuple[NotificationDecl, ...],
        root_streams: tuple[BinaryStreamDecl, ...],
        options: TypeScriptClientOptions,
        client_name: str,
    ) -> None:
        self.ir = ir
        self.root_operations = root_operations
        self.nodes = nodes
        self.root_events = root_events
        self.root_streams = root_streams
        self.options = options
        self.client_name = client_name

    def module(self, body: str) -> str:
        while "\n\n\n" in body:
            body = body.replace("\n\n\n", "\n\n")
        return render_template(
            "typescript/module.ts.j2",
            source=self.options.source,
            body=body,
        )

    def template(self, name: str, **context: object) -> str:
        return render_template(
            f"typescript/{name}.ts.j2",
            filters=self._filters(),
            **context,
        ).rstrip()

    def _filters(self) -> dict[str, object]:
        return {
            "all_optional": lambda parameters: (
                not any(parameter.required for parameter in parameters)
            ),
            "api_class": _api_class,
            "array": _array,
            "comment": _comment,
            "compact_notification": self._compact_notification,
            "enum_decl": lambda declaration: isinstance(declaration, EnumDecl),
            "identifier": _identifier,
            "literal": _ts_literal,
            "model_decl": lambda declaration: isinstance(declaration, ModelDecl),
            "null_type": _is_null,
            "optional_field": lambda field: (
                not field.required and not _is_discriminator(field.name, field.type)
            ),
            "property": _property,
            "route_access": lambda route: f"routes.{_route_key(route)}",
            "route_key": _route_key,
            "route_params": lambda route: (
                "undefined"
                if route.params_model is None
                else _schema_name(route.params_model)
            ),
            "schema_name": _schema_name,
            "server_variable_type": _server_variable_type,
            "stream_endpoint": _stream_endpoint,
            "stream_signature": _stream_signature,
            "stream_connection": _stream_connection,
            "subprotocols": _server_subprotocols,
            "type": self._type,
        }

    def _compact_notification(
        self,
        event: NotificationDecl,
        root: bool,
    ) -> bool:
        receiver = "this.#rpc" if root else "this.rpc"
        call = (
            f"    return {receiver}.notifications(notifications.{_route_key(event)});"
        )
        return len(call) <= 80

    def models(self) -> str:
        return self.module(self.template("models", declarations=self.ir.declarations))

    def namespaces_index(self) -> str:
        return self.module(self.template("namespace_index", nodes=self.nodes))

    def routes(self) -> str:
        model_names = _route_model_names(self.ir.operations)
        model_names.update(
            name
            for event in self.ir.notifications
            for name in _model_names(event.payload)
        )
        return self.module(
            self.template(
                "routes",
                routes=self.ir.operations,
                notifications=self.ir.notifications,
                model_import=(
                    _type_import(model_names, "./models") if model_names else ""
                ),
            )
        )

    def core(self) -> str:
        body = render_template(
            "typescript/core.ts.j2",
            transport_module=json.dumps(self._transport_module()),
            binary_streams=bool(self.ir.binary_streams),
        ).rstrip()
        return self.module(body)

    def transport(self) -> str:
        return self.module(render_template("typescript/transport.ts.j2").rstrip())

    def streams(self) -> str:
        return self.module(
            self.template(
                "streams",
                streams=self.ir.binary_streams,
                with_websocket=self.options.with_transport == "websocket",
            )
        )

    def api(self, root_node: NamespaceViewNode) -> str:
        root = "../"
        nodes = tuple(_walk((root_node,)))
        imports = [f'import type {{ RpcClientCore }} from "{root}core";']
        if any(node.streams for node in nodes):
            imports.append(
                _value_import(
                    [
                        *_stream_connections(
                            stream for node in nodes for stream in node.streams
                        ),
                        "binaryStreams",
                        "resolveStreamEndpoint",
                    ],
                    f"{root}streams",
                )
            )
        route_imports = []
        if any(node.operations for node in nodes):
            route_imports.append("routes")
        if any(node.notifications for node in nodes):
            route_imports.append("notifications")
        if route_imports:
            imports.append(_value_import(route_imports, f"{root}routes"))
        model_names: set[str] = set()
        for node in nodes:
            model_names.update(_route_model_names(node.operations))
            model_names.update(
                name
                for event in node.notifications
                for name in _model_names(event.payload)
            )
            model_names.update(
                event.params_model
                for event in node.notifications
                if event.params_model is not None
            )
        if model_names:
            imports.append(_type_import(model_names, f"{root}models"))
        body = self.template(
            "api",
            imports=imports,
            nodes=tuple(_walk_postorder((root_node,))),
        )
        return self.module(body)

    def client(self) -> str:
        core_imports = [
            "RpcClientCore",
            "type RpcClientHook",
            "type RpcTransport",
            "type RpcTransportSource",
        ]
        if self.options.with_transport == "websocket":
            core_imports.insert(1, "RpcTransportPool")
        imports = [_value_import(core_imports, "./core")]
        if self.ir.binary_streams:
            stream_imports = ["type BinaryStreamOpener"]
            if self.root_streams:
                stream_imports = [
                    *_stream_connections(self.root_streams),
                    "binaryStreams",
                    "resolveStreamEndpoint",
                    *stream_imports,
                ]
            if self.options.with_transport == "websocket":
                stream_imports = [
                    "BinaryWebSocketStream",
                    *stream_imports,
                    "type BinaryWebSocketFactory",
                ]
            imports.append(_value_import(stream_imports, "./streams"))
        route_imports = []
        if self.root_operations:
            route_imports.append("routes")
        if self.root_events:
            route_imports.append("notifications")
        if route_imports:
            imports.append(_value_import(route_imports, "./routes"))
        models = _route_model_names(self.root_operations)
        models.update(
            name for event in self.root_events for name in _model_names(event.payload)
        )
        models.update(
            event.params_model
            for event in self.root_events
            if event.params_model is not None
        )
        if models:
            imports.append(_type_import(models, "./models"))
        if self.ir.servers:
            endpoint_imports = ["type ServerName"]
            if self.options.with_transport == "websocket":
                endpoint_imports = [
                    "resolveEndpoints",
                    "type EndpointOverrides",
                    *endpoint_imports,
                ]
            imports.append(_value_import(endpoint_imports, "./endpoints"))
        if self.options.with_transport == "websocket":
            imports.extend(
                [
                    "import { WebSocketTransport, type WebSocketFactory } "
                    'from "./transport";',
                ]
            )
        if self.nodes:
            imports.append(
                _value_import(
                    [_api_class(node.path) for node in self.nodes],
                    "./namespaces",
                )
            )
        body = self.template(
            "client",
            imports=imports,
            client_name=self.client_name,
            servers=self.ir.servers,
            variables=_connect_variables(self.ir),
            nodes=self.nodes,
            operations=self.root_operations,
            notifications=self.root_events,
            streams=self.root_streams,
            binary_streams=self.ir.binary_streams,
            with_websocket=self.options.with_transport == "websocket",
        )
        return self.module(body)

    def _transport_module(self) -> str:
        if self.options.with_transport == "websocket":
            return "./transport"
        return self.options.transport_module

    def endpoints(self) -> str:
        return self.module(self.template("endpoints", servers=self.ir.servers))

    def errors(self) -> str:
        named = {
            error.name: error
            for route in (*self.ir.operations, *self.ir.notifications)
            for error in route.errors
            if error.name is not None
        }
        model_names = {
            name
            for error in named.values()
            if error.data is not None
            for name in _model_names(error.data)
        }
        model_import = _type_import(model_names, "./models") if model_names else ""
        return self.module(
            self.template(
                "errors",
                errors=tuple(named.values()),
                model_import=model_import,
            )
        )

    def index(self) -> str:
        body = self.template(
            "index",
            client_name=self.client_name,
            servers=self.ir.servers,
            with_websocket=self.options.with_transport == "websocket",
            nodes=self.nodes,
            routes=bool(self.ir.operations),
            notifications=self.ir.notifications,
            binary_streams=self.ir.binary_streams,
            models=bool(self.ir.declarations),
        )
        return self.module(body)

    def _type(self, expression: TypeExpr) -> str:
        if isinstance(expression, PrimitiveType):
            return _PRIMITIVES[expression.primitive]
        if isinstance(expression, NamedType):
            return _schema_name(expression.name)
        if isinstance(expression, LiteralType):
            return json.dumps(expression.value)
        if isinstance(expression, EnumLiteralType):
            return f"typeof {_schema_name(expression.enum)}.{expression.member}"
        if isinstance(expression, ListType):
            item = self._type(expression.item)
            if isinstance(expression.item, UnionType):
                item = f"({item})"
            return f"{item}[]"
        if isinstance(expression, MapType):
            return f"Record<string, {self._type(expression.value)}>"
        if isinstance(expression, UnionType):
            return " | ".join(
                dict.fromkeys(self._type(member) for member in expression.members)
            )
        raise TypeError(f"Unsupported type: {type(expression).__name__}")


def _connect_variables(ir: ClientIr) -> tuple[ServerVariableDecl, ...]:
    """The server URL variables `connect` accepts; streams inherit them too."""
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


def _validate(
    ir: ClientIr,
    root_operations: tuple[RouteDecl, ...],
    root_streams: tuple[BinaryStreamDecl, ...],
    nodes: tuple[NamespaceViewNode, ...],
    options: TypeScriptClientOptions,
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
    if not _VALID_IDENTIFIER.fullmatch(client_name):
        raise UnsupportedSchemaError(
            f"client_name must be a valid TypeScript identifier: {client_name!r}"
        )
    assert_unique_names(
        "schemas",
        (
            (declaration.name, _schema_name(declaration.name))
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
    client_members = [
        ("<client.close>", "close"),
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
    client_members.append(("<client.withTransports>", "withTransports"))
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
            connect_options.extend(
                [
                    ("<connect.streamSocketFactory>", "streamSocketFactory"),
                    ("<connect.streamQueueSize>", "streamQueueSize"),
                ]
            )
        assert_unique_names("connect options", connect_options)
    assert_unique_names("root client", client_members)
    _validate_nodes(nodes)
    for node in nodes:
        assert_unique_names(
            f"TypeScript namespace module {_identifier(node.segment)!r}",
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


def _client_name(ir: ClientIr, options: TypeScriptClientOptions) -> str:
    return options.client_name or f"{pascal_case(ir.title)}Client"


def _identifier(value: str) -> str:
    identifier = camel_case(value)
    if not identifier:
        raise UnsupportedSchemaError(
            f"Cannot derive a TypeScript identifier from {value!r}"
        )
    if identifier[0].isdigit():
        identifier = f"_{identifier}"
    if identifier in _RESERVED_WORDS:
        identifier = f"{identifier}_"
    return identifier


def _schema_name(value: str) -> str:
    name = pascal_case(value)
    if not name:
        raise UnsupportedSchemaError(
            f"Cannot derive a TypeScript class name from {value!r}"
        )
    if name[0].isdigit():
        name = f"_{name}"
    return name


def _model_names(expression: TypeExpr) -> set[str]:
    return {_schema_name(name) for name in named_types(expression)}


def _property(value: str) -> str:
    return value if _VALID_IDENTIFIER.fullmatch(value) else json.dumps(value)


def _route_key(route: RouteDecl) -> str:
    return _identifier(route.rpc_name)


def _api_class(path: tuple[str, ...]) -> str:
    return "".join(pascal_case(segment) for segment in path)


def _api_file(node: NamespaceViewNode) -> str:
    return f"namespaces/{_identifier(node.path[0])}.ts"


def _route_model_names(routes: Iterable[RouteDecl]) -> set[str]:
    names: set[str] = set()
    for route in routes:
        if route.params and route.params_model is not None:
            names.add(_schema_name(route.params_model))
        names.update(_model_names(route.result))
    return names


def _notification_type(ir: ClientIr) -> TypeExpr | None:
    if not ir.notifications:
        return None
    members = tuple(item.payload for item in ir.notifications)
    return members[0] if len(members) == 1 else UnionType(members)


def _stream_connection(stream: BinaryStreamDecl) -> str:
    if stream.direction == "client-to-server":
        return "BinarySender"
    if stream.direction == "bidirectional":
        return "BinaryChannel"
    return "BinaryReceiver"


def _stream_connections(streams: Iterable[BinaryStreamDecl]) -> list[str]:
    return sorted({_stream_connection(stream) for stream in streams})


def _value_import(names: Iterable[str], module: str) -> str:
    """Render one import, wrapped the way Prettier would wrap it."""
    values = list(names)
    inline = f'import {{ {", ".join(values)} }} from "{module}";'
    if len(inline) <= 80:
        return inline
    body = "\n".join(f"  {name}," for name in values)
    return f'import {{\n{body}\n}} from "{module}";'


def _type_import(names: Iterable[str], module: str) -> str:
    values = sorted(set(names))
    inline = f'import type {{ {", ".join(values)} }} from "{module}";'
    if len(inline) <= 80:
        return inline
    body = "\n".join(f"  {name}," for name in values)
    return f'import type {{\n{body}\n}} from "{module}";'


def _type_export(names: Iterable[str], module: str) -> str:
    return f'export type {{ {", ".join(sorted(set(names)))} }} from "{module}";'


def _array(values: Iterable[object]) -> str:
    return "[" + ", ".join(_ts_literal(value) for value in values) + "]"


def _stream_signature(stream: BinaryStreamDecl) -> str:
    """A stream method's opening line, laid out as Prettier would."""
    name = _identifier(stream.operation_name)
    returns = f"Promise<{_stream_connection(stream)}>"
    if not stream.call_variables:
        return f"  {name}(): {returns} {{"
    fields = [
        f"readonly {_property(variable.name)}?: {_server_variable_type(variable)}"
        for variable in stream.call_variables
    ]
    inline = f"  {name}(options?: {{ {'; '.join(fields)} }}): {returns} {{"
    if len(inline) <= _TS_WIDTH:
        return inline
    body = "".join(f"    {field};\n" for field in fields)
    return f"  {name}(options?: {{\n{body}  }}): {returns} {{"


def _stream_endpoint(stream: BinaryStreamDecl, rpc: str) -> str:
    """The ``resolveStreamEndpoint(...)`` argument, laid out as Prettier would."""
    indent = " " * 6
    head = [f"binaryStreams.{_identifier(stream.name)}", f"{rpc}.variables"]
    if not stream.call_variables:
        inline = f"{indent}resolveStreamEndpoint({', '.join(head)}),"
        if len(inline) <= _TS_WIDTH:
            return inline
        args = "".join(f"{indent}  {arg},\n" for arg in head)
        return f"{indent}resolveStreamEndpoint(\n{args}{indent}),"
    fields = [
        f"{_property(variable.name)}: options?.{_property(variable.name)},"
        for variable in stream.call_variables
    ]
    hugged = f"{indent}resolveStreamEndpoint({', '.join(head)}, {{"
    if len(hugged) <= _TS_WIDTH:
        body = "".join(f"{indent}  {field}\n" for field in fields)
        return f"{hugged}\n{body}{indent}}}),"
    args = "".join(f"{indent}  {arg},\n" for arg in head)
    body = "".join(f"{indent}    {field}\n" for field in fields)
    return (
        f"{indent}resolveStreamEndpoint(\n{args}{indent}  {{\n{body}"
        f"{indent}  }},\n{indent}),"
    )


_TS_WIDTH = 80


def _server_variable_type(variable: ServerVariableDecl) -> str:
    if variable.enum:
        return " | ".join(_ts_literal(value) for value in variable.enum)
    return "string"


def _server_subprotocols(server: ServerDecl) -> str:
    if server.transport is None:
        return "[]"
    return _array(server.transport.subprotocols)


def _ts_literal(value: object) -> str:
    if value is None:
        return "undefined"
    if isinstance(value, bool):
        return str(value).lower()
    return json.dumps(value)


def _comment(value: str) -> str:
    return " ".join(value.split()).replace("*/", "* /")


def _is_null(expression: TypeExpr) -> bool:
    return (
        isinstance(expression, PrimitiveType) and expression.primitive is Primitive.NULL
    )


def _is_discriminator(name: str, expression: TypeExpr) -> bool:
    return name == "type" and isinstance(expression, LiteralType | EnumLiteralType)


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
    return any(
        error.name is not None
        for route in (*ir.operations, *ir.notifications)
        for error in route.errors
    )


_PRIMITIVES = {
    Primitive.STRING: "string",
    Primitive.INTEGER: "number",
    Primitive.NUMBER: "number",
    Primitive.BOOLEAN: "boolean",
    Primitive.NULL: "null",
    Primitive.UUID: "string",
    Primitive.DATETIME: "string",
    Primitive.ANY: "unknown",
}

_CONNECT_OPTIONS = (
    "eager",
    "hooks",
    "notificationQueueSize",
    "requestTimeoutMs",
    "servers",
    "socketFactory",
)

_VALID_IDENTIFIER = re.compile(r"[A-Za-z_$][A-Za-z0-9_$]*")

_RESERVED_WORDS = frozenset(
    {
        "break",
        "case",
        "class",
        "const",
        "constructor",
        "continue",
        "debugger",
        "default",
        "delete",
        "do",
        "else",
        "enum",
        "export",
        "extends",
        "false",
        "finally",
        "for",
        "function",
        "if",
        "import",
        "in",
        "instanceof",
        "new",
        "null",
        "return",
        "super",
        "switch",
        "this",
        "throw",
        "true",
        "try",
        "typeof",
        "var",
        "void",
        "while",
        "with",
        "yield",
    }
)

_RUNTIME_NAMES = frozenset(
    {"RpcClientCore", "RpcRemoteError", "RpcRouteInfo", "RpcTransport"}
)

import re
from collections.abc import Iterable
from types import UnionType
from typing import Any, TypeAliasType, get_args, get_origin

from pydantic import BaseModel, TypeAdapter

from rpckit.errors import ProtocolDefinitionError
from rpckit.protocol import (
    RpcNotificationDefinition,
    RpcProtocol,
)

EMPTY_PARAMS_SCHEMA: dict[str, Any] = {
    "additionalProperties": False,
    "type": "object",
}
"""What a method without params accepts: an omitted or empty ``params`` member."""


def components(protocol: RpcProtocol) -> dict[str, Any]:
    """Build the JSON Schema components referenced by the OpenRPC document."""
    annotations = _annotations(protocol)
    _assert_unique_wire_names(annotations.values())
    _, root = TypeAdapter.json_schemas(
        (
            (name, "serialization", TypeAdapter(annotation))
            for name, annotation in annotations.items()
        ),
        by_alias=True,
    )
    definitions = root.get("$defs", {})
    for method in protocol.methods:
        definitions[method.request_name] = _request_schema(
            method.name, method.request_name, method.params, method.summary
        )
    for client_method in protocol.client_methods:
        name = client_method_schema_name(client_method.name)
        if name in definitions:
            raise ProtocolDefinitionError(f"Duplicate protocol schema name: {name}")
        definitions[name] = _request_schema(
            client_method.name, name, client_method.params, client_method.summary
        )
    for notification in protocol.notifications:
        name = notification_schema_name(notification.name)
        definitions[name] = _notification_schema(notification, name)
    return definitions


def described(entry: dict[str, Any], summary: str | None) -> dict[str, Any]:
    """Attach a summary only when the definition carries one."""
    if summary is not None:
        entry["summary"] = summary
    return entry


def type_name(annotation: Any) -> str:
    name = getattr(annotation, "__name__", None)
    if not isinstance(name, str):
        raise ProtocolDefinitionError(
            f"Protocol type has no stable schema name: {annotation!r}"
        )
    return name


def notification_schema_name(name: str) -> str:
    parts = (part for part in re.split(r"[^a-zA-Z0-9]+", name) if part)
    return "".join(part.capitalize() for part in parts) + "Notification"


def client_method_schema_name(name: str) -> str:
    parts = (part for part in re.split(r"[^a-zA-Z0-9]+", name) if part)
    return "".join(part.capitalize() for part in parts) + "ClientMethod"


def _annotations(protocol: RpcProtocol) -> dict[str, Any]:
    annotations: dict[str, Any] = {}
    for method in protocol.methods:
        if method.params is not None:
            _add(annotations, method.params)
        _add_result_types(annotations, method.result)
    for notification in protocol.notifications:
        _add(annotations, notification.payload)
    for subscription in protocol.subscriptions:
        if subscription.params is not None:
            _add(annotations, subscription.params)
        _add(annotations, subscription.payload)
    for notification_type in protocol.notification_types:
        _add(annotations, notification_type.payload)
    for client_method in protocol.client_methods:
        if client_method.params is not None:
            _add(annotations, client_method.params)
        _add_result_types(annotations, client_method.result)
    for method in (
        *protocol.methods,
        *protocol.client_methods,
        *protocol.notifications,
        *protocol.subscriptions,
    ):
        for error in method.raises:
            if error.details_type is not None:
                _add(annotations, error.details_type)
    return annotations


def _add_result_types(annotations: dict[str, Any], annotation: Any) -> None:
    if isinstance(annotation, TypeAliasType) or (
        isinstance(annotation, type) and issubclass(annotation, BaseModel)
    ):
        _add(annotations, annotation)
        return
    for argument in get_args(annotation):
        _add_result_types(annotations, argument)


def _add(annotations: dict[str, Any], annotation: Any) -> None:
    name = type_name(annotation)
    existing = annotations.get(name)
    if existing is not None and existing != annotation:
        raise ProtocolDefinitionError(f"Duplicate protocol schema name: {name}")
    annotations[name] = annotation


def _request_schema(
    name: str,
    title: str,
    params_model: type[BaseModel] | None,
    summary: str | None,
) -> dict[str, Any]:
    required = ["jsonrpc", "method"]
    if params_model is None:
        params = EMPTY_PARAMS_SCHEMA
    else:
        params = _ref(type_name(params_model))
        if params_model.model_json_schema(by_alias=True, mode="serialization").get(
            "required"
        ):
            required.append("params")
    schema: dict[str, Any] = {
        "additionalProperties": False,
        "type": "object",
        "title": title,
        "properties": {
            "jsonrpc": {"const": "2.0", "type": "string"},
            "id": {
                "anyOf": [
                    {"type": "string"},
                    {"type": "integer"},
                    {"type": "null"},
                ],
                "default": None,
            },
            "method": {"const": name, "type": "string"},
            "params": params,
        },
        "required": required,
    }
    if summary is not None:
        schema["description"] = summary
    return schema


def _notification_schema(
    notification: RpcNotificationDefinition,
    schema_name: str,
) -> dict[str, Any]:
    schema: dict[str, Any] = {
        "additionalProperties": False,
        "type": "object",
        "title": schema_name,
        "properties": {
            "jsonrpc": {"const": "2.0", "type": "string"},
            "method": {"const": notification.name, "type": "string"},
            "params": _ref(type_name(notification.payload)),
        },
        "required": ["jsonrpc", "method", "params"],
    }
    if notification.summary is not None:
        schema["description"] = notification.summary
    return schema


def _ref(name: str) -> dict[str, str]:
    return {"$ref": f"#/$defs/{name}"}


def _assert_unique_wire_names(annotations: Iterable[Any]) -> None:
    seen_models: set[type[BaseModel]] = set()

    def visit(annotation: Any) -> None:
        if isinstance(annotation, type) and issubclass(annotation, BaseModel):
            if annotation in seen_models:
                return
            seen_models.add(annotation)
            wire_fields: dict[str, str] = {}
            for python_name, model_field in annotation.model_fields.items():
                wire_name = (
                    model_field.serialization_alias or model_field.alias or python_name
                )
                previous = wire_fields.get(wire_name)
                if previous is not None:
                    raise ProtocolDefinitionError(
                        f"RPC model {annotation.__name__} maps both "
                        f"{previous!r} and {python_name!r} to wire field "
                        f"{wire_name!r}"
                    )
                wire_fields[wire_name] = python_name
                visit(model_field.annotation)
            return
        origin = get_origin(annotation)
        if origin is not None or isinstance(annotation, UnionType):
            for argument in get_args(annotation):
                visit(argument)

    for annotation in annotations:
        visit(annotation)

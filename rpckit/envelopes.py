from typing import Any, Literal, Self

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    PrivateAttr,
    field_serializer,
)

from rpckit._adapter import adapter
from rpckit.errors import RpcError

JSONRPC_VERSION = "2.0"

type RpcRequestId = str | int | None


class RpcSchema(BaseModel):
    """Base model for protocol payloads: immutable and strict about extra keys."""

    model_config = ConfigDict(extra="forbid", frozen=True, serialize_by_alias=True)


class RpcRequestEnvelope(RpcSchema):
    jsonrpc: Literal["2.0"]
    id: RpcRequestId = None
    method: str
    params: dict[str, Any] | list[Any] = Field(default_factory=dict)

    @property
    def expects_response(self) -> bool:
        return "id" in self.model_fields_set


class RpcErrorData(RpcSchema):
    code: int
    message: str
    data: "RpcErrorPayload"


class RpcErrorPayload(RpcSchema):
    code: str
    details: Any = None
    _details_annotation: Any = PrivateAttr(default=None)

    @field_serializer("details")
    def _serialize_details(self, details: Any, info: Any) -> Any:
        if details is None or self._details_annotation is None:
            return details
        return adapter(self._details_annotation).dump_python(
            details, mode=info.mode, by_alias=True
        )


class RpcSuccess(RpcSchema):
    jsonrpc: Literal["2.0"] = JSONRPC_VERSION
    id: RpcRequestId
    result: Any
    _result_annotation: Any = PrivateAttr(default=None)

    @classmethod
    def _with_result_annotation(
        cls,
        request_id: RpcRequestId,
        result: Any,
        annotation: Any,
    ) -> Self:
        response = cls(id=request_id, result=result)
        response._result_annotation = annotation
        return response

    @field_serializer("result")
    def _serialize_result(self, result: Any, info: Any) -> Any:
        if self._result_annotation is None:
            return result
        return adapter(self._result_annotation).dump_python(
            result,
            mode=info.mode,
            by_alias=True,
        )


class RpcFailure(RpcSchema):
    jsonrpc: Literal["2.0"] = JSONRPC_VERSION
    id: RpcRequestId
    error: RpcErrorData

    @classmethod
    def from_error(cls, request_id: RpcRequestId, error: RpcError) -> Self:
        payload = RpcErrorPayload(code=error.code, details=error.details)
        payload._details_annotation = error.details_type
        return cls(
            id=request_id,
            error=RpcErrorData(
                code=int(error.rpc_code), message=error.message, data=payload
            ),
        )


class RpcNotification(RpcSchema):
    jsonrpc: Literal["2.0"] = JSONRPC_VERSION
    method: str
    params: Any
    _payload_annotation: Any = PrivateAttr(default=None)

    @classmethod
    def _with_payload_annotation(
        cls,
        method: str,
        payload: Any,
        annotation: Any,
    ) -> Self:
        notification = cls(method=method, params=payload)
        notification._payload_annotation = annotation
        return notification

    @field_serializer("params")
    def _serialize_params(self, params: Any, info: Any) -> Any:
        if self._payload_annotation is None:
            return params
        return adapter(self._payload_annotation).dump_python(
            params,
            mode=info.mode,
            by_alias=True,
        )

import asyncio
import logging

import pytest
from pydantic import BaseModel

from rpckit import (
    RpcChannel,
    RpcError,
    RpcErrorCode,
    RpcFailure,
    RpcLimits,
    RpcServer,
    RpcService,
    RpcSuccess,
)

from .conftest import (
    GreetingRpcMethod,
    GreetingState,
    SayParams,
    TestResolver,
    greeting_app,
)


def _server(handler: GreetingState) -> RpcServer:
    return greeting_app.create_server(resolver=TestResolver(handler))


@pytest.mark.parametrize("params", [[], ["M"], [1, 2]])
async def test_positional_params_are_invalid_params(handler, params) -> None:
    response = await _server(handler).handle(
        {"jsonrpc": "2.0", "id": 7, "method": GreetingRpcMethod.SAY, "params": params}
    )
    assert response.error.code == RpcErrorCode.INVALID_PARAMS
    assert response.id == 7


async def test_a_request_is_answered_with_its_result(
    handler: GreetingState,
) -> None:
    response = await _server(handler).handle(
        {
            "jsonrpc": "2.0",
            "id": 7,
            "method": GreetingRpcMethod.SAY,
            "params": {"name": "M"},
        }
    )

    assert isinstance(response, RpcSuccess)
    assert response.id == 7
    assert response.result is not None
    assert response.result.text == "Hello, M!"


async def test_observer_receives_request_outcome_and_duration(
    handler: GreetingState,
) -> None:
    class Observer:
        def __init__(self) -> None:
            self.requests = []
            self.responses = []

        async def request_started(self, context) -> None:
            self.requests.append(context)

        async def request_finished(self, context) -> None:
            self.responses.append(context)

        async def connection_closed(self, context) -> None: ...

    observer = Observer()

    server = greeting_app.create_server(
        resolver=TestResolver(handler),
        observer=observer,
    )
    response = await server.handle(
        {
            "jsonrpc": "2.0",
            "id": 7,
            "method": GreetingRpcMethod.SAY,
            "params": {"name": "M"},
        }
    )

    assert observer.requests[0].method == GreetingRpcMethod.SAY
    assert observer.requests[0].request_id == 7
    assert observer.requests[0].notification is False
    assert observer.responses[0].request is observer.requests[0]
    assert observer.responses[0].response is response
    assert observer.responses[0].duration >= 0


async def test_a_notification_is_served_without_a_response(
    handler: GreetingState,
) -> None:
    response = await _server(handler).handle(
        {"jsonrpc": "2.0", "method": GreetingRpcMethod.SAY, "params": {"name": "M"}}
    )

    assert response is None
    assert handler.greeted == ["M"]


async def test_an_unknown_method_becomes_a_failure(handler: GreetingState) -> None:
    response = await _server(handler).handle(
        {"jsonrpc": "2.0", "id": 1, "method": "greeting.unknown", "params": {}}
    )

    assert isinstance(response, RpcFailure)
    assert response.id == 1
    assert response.error.code == RpcErrorCode.METHOD_NOT_FOUND


async def test_invalid_params_become_a_failure(handler: GreetingState) -> None:
    response = await _server(handler).handle(
        {"jsonrpc": "2.0", "id": 1, "method": GreetingRpcMethod.SAY, "params": {}}
    )

    assert isinstance(response, RpcFailure)
    assert response.error.code == RpcErrorCode.INVALID_PARAMS
    assert "params.name" in response.error.message


async def test_a_malformed_envelope_becomes_an_invalid_request(
    handler: GreetingState,
) -> None:
    response = await _server(handler).handle({"id": 1, "method": "x"})

    assert isinstance(response, RpcFailure)
    assert response.error.code == RpcErrorCode.INVALID_REQUEST


async def test_a_declared_error_goes_on_the_wire_as_declared(
    handler: GreetingState,
) -> None:
    response = await _server(handler).handle(
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": GreetingRpcMethod.FORGET,
            "params": {"name": "M"},
        }
    )

    assert isinstance(response, RpcFailure)
    assert response.error.code == -32001
    assert response.error.message == "Unknown greeting: M"


async def test_foreign_errors_are_translated_by_the_error_mapper() -> None:
    server = broken_app.create_server(error_mapper=_broken_error)

    response = await server.handle(
        {"jsonrpc": "2.0", "id": 1, "method": "greeting.break"}
    )

    assert isinstance(response, RpcFailure)
    assert response.error.code == -32004
    assert response.error.message == "Mapped: boom"


async def test_unmapped_handler_failures_stay_internal(caplog) -> None:
    server = broken_app.create_server()
    with caplog.at_level(logging.ERROR, logger="rpckit"):
        response = await server.handle(
            {"jsonrpc": "2.0", "id": 1, "method": "greeting.break"}
        )

    assert isinstance(response, RpcFailure)
    assert response.error.code == RpcErrorCode.INTERNAL_ERROR
    assert response.error.message == "Internal error"
    assert "RPC method greeting.break failed" in caplog.text
    assert "BreakageError: boom" in caplog.text


async def test_a_non_object_payload_fails_without_an_id(
    handler: GreetingState,
) -> None:
    response = await _server(handler).handle("nonsense")

    assert isinstance(response, RpcFailure)
    assert response.id is None


def test_rpc_errors_carry_their_own_code(handler: GreetingState) -> None:
    class BusyError(RpcError):
        rpc_code = -32001

    failure = _server(handler).failure(3, BusyError(message="Busy"))

    assert failure.id == 3
    assert failure.error.code == -32001


async def test_a_boolean_id_on_a_failed_request_is_not_echoed_back(
    handler: GreetingState,
) -> None:
    response = await _server(handler).handle(
        {"jsonrpc": "2.0", "id": True, "method": "greeting.unknown", "params": {}}
    )

    assert isinstance(response, RpcFailure)
    assert response.id is None


async def test_a_validation_error_in_handler_is_internal(caplog) -> None:
    class NestedParams(BaseModel):
        params: str

    router = RpcChannel("greeting")

    @router.server.method("broken")
    async def broken(params: SayParams) -> None:
        NestedParams.model_validate({"params": 1})

    with caplog.at_level(logging.ERROR, logger="rpckit"):
        response = await router.create_server().handle(
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "greeting.broken",
                "params": {"name": "M"},
            }
        )

    assert isinstance(response, RpcFailure)
    assert response.error.code == RpcErrorCode.INTERNAL_ERROR
    assert "ValidationError" in caplog.text


async def test_invalid_handler_result_is_internal(caplog) -> None:
    channel = RpcChannel("result")

    @channel.server.method()
    async def broken() -> int:
        return "abc"  # type: ignore[return-value]

    with caplog.at_level(logging.ERROR, logger="rpckit"):
        response = await channel.create_server().handle(
            {"jsonrpc": "2.0", "id": 1, "method": "result.broken"}
        )

    assert isinstance(response, RpcFailure)
    assert response.error.code == RpcErrorCode.INTERNAL_ERROR
    assert "ValidationError" in caplog.text


async def test_matching_dict_handler_result_is_accepted() -> None:
    class Result(BaseModel):
        value: int

    channel = RpcChannel("result")

    @channel.server.method()
    async def valid() -> Result:
        return {"value": 3}  # type: ignore[return-value]

    response = await channel.create_server().handle(
        {"jsonrpc": "2.0", "id": 1, "method": "result.valid"}
    )

    assert isinstance(response, RpcSuccess)
    assert response.result.value == 3


async def test_batch_runs_concurrently_with_shared_limit() -> None:
    started = asyncio.Event()
    release = asyncio.Event()
    running = 0
    peak = 0
    channel = RpcChannel("batch")

    @channel.server.method()
    async def wait() -> None:
        nonlocal running, peak
        running += 1
        peak = max(peak, running)
        if peak == 2:
            started.set()
        await release.wait()
        running -= 1

    server = channel.create_server(limits=RpcLimits(max_concurrency=2))
    batch = [
        {"jsonrpc": "2.0", "id": index, "method": "batch.wait"} for index in range(3)
    ]
    task = asyncio.create_task(server.handle(batch))
    await asyncio.wait_for(started.wait(), 1)
    assert peak == 2
    release.set()
    responses = await asyncio.wait_for(task, 1)
    assert [item.id for item in responses] == [0, 1, 2]


async def test_batch_size_limit_rejects_batch() -> None:
    server = broken_app.create_server(limits=RpcLimits(max_batch_size=0))
    response = await server.handle(
        [{"jsonrpc": "2.0", "id": 1, "method": "greeting.break"}]
    )
    assert isinstance(response, RpcFailure)
    assert response.error.code == RpcErrorCode.INVALID_REQUEST


async def test_custom_error_mapper_and_strict_errors() -> None:
    class DomainMissing(Exception):
        pass

    class MissingError(RpcError):
        pass

    channel = RpcChannel("mapped")

    @channel.server.method(raises=(MissingError,))
    async def declared() -> None:
        raise DomainMissing("gone")

    @channel.server.method()
    async def undeclared() -> None:
        raise DomainMissing("gone")

    def mapper(error: Exception) -> RpcError | None:
        return (
            MissingError(message=str(error))
            if isinstance(error, DomainMissing)
            else None
        )

    service = RpcService(error_mapper=mapper, strict_errors=True)
    endpoint = service.socket("/mapped", channels=(channel,))
    server = endpoint.create_server()
    declared_response = await server.handle(
        {"jsonrpc": "2.0", "id": 1, "method": "mapped.declared"}
    )
    undeclared_response = await server.handle(
        {"jsonrpc": "2.0", "id": 2, "method": "mapped.undeclared"}
    )
    assert isinstance(declared_response, RpcFailure)
    assert declared_response.error.data.code == "missing"
    assert declared_response.error.message == "gone"
    assert isinstance(undeclared_response, RpcFailure)
    assert undeclared_response.error.code == RpcErrorCode.INTERNAL_ERROR


class BreakageError(Exception):
    pass


class BrokenParams(BaseModel):
    pass


broken_channel = RpcChannel("greeting")


@broken_channel.server.method("break")
async def fail(params: BrokenParams) -> None:
    raise BreakageError("boom")


broken_app = broken_channel


class MappedError(RpcError):
    rpc_code = -32004


def _broken_error(error: Exception) -> RpcError | None:
    if isinstance(error, BreakageError):
        return MappedError(message=f"Mapped: {error}")
    return None

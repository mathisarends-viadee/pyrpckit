import asyncio
import json
from collections.abc import AsyncIterator

import pytest

from rpckit import RpcChannel, RpcErrorBinding, RpcLimits, RpcModel, RpcService
from rpckit.testing import RpcTestClient

from .testing import InMemorySocket


class SessionParams(RpcModel):
    session_id: str


class SessionEvent(RpcModel):
    value: str


class SessionMissing(Exception):
    def __init__(self, session_id: str) -> None:
        self.session_id = session_id


@pytest.mark.parametrize("declared", [True, False])
async def test_subscription_failures_use_scoped_bindings(declared, caplog) -> None:
    binding = RpcErrorBinding(SessionMissing, details=SessionParams)
    channel = RpcChannel("session")

    @channel.server.subscription(raises=[binding] if declared else [])
    async def events(params: SessionParams) -> AsyncIterator[SessionEvent]:
        yield SessionEvent(value="first")
        raise SessionMissing(params.session_id)

    @channel.server.method(raises=[binding])
    async def lookup() -> None: ...

    service = RpcService()
    service.socket("/rpc", channels=[channel])
    document = service.contract(
        title="Sessions", base_url="ws://localhost"
    ).to_openrpc()
    subscription = document["x-rpc-subscriptions"][0]
    assert ("errors" in subscription) is declared
    if declared:
        assert subscription["errors"][0]["x-rpckit-details-schema"] == {
            "$ref": "#/components/schemas/SessionParams"
        }
    async with RpcTestClient(service, "/rpc") as client:
        response = await client.request(
            "session.events.subscribe", {"sessionId": "abc"}
        )
        _, first = await client.next_notification(timeout=1)
        _, terminal = await client.next_notification(timeout=1)
    assert first["payload"] == {"value": "first"}
    assert terminal["subscriptionId"] == response["subscriptionId"]
    assert terminal["complete"] is False
    error = terminal["error"]
    assert error["data"]["code"] == (
        "session_missing" if declared else "internal_error"
    )
    assert error["message"] == ("Session missing" if declared else "Internal error")
    if declared:
        assert error["data"]["details"] == {"sessionId": "abc"}
    else:
        assert "RPC method session.events failed" in caplog.text


async def test_subscription_params_notifications_and_cleanup() -> None:
    channel = RpcChannel("session")
    stopped = asyncio.Event()

    @channel.server.subscription()
    async def events(params: SessionParams) -> AsyncIterator[SessionEvent]:
        try:
            yield SessionEvent(value=params.session_id)
            await asyncio.Future()
        finally:
            stopped.set()

    service = RpcService()
    service.socket("/rpc", channels=(channel,))
    socket = InMemorySocket("/rpc")
    task = asyncio.create_task(service.serve(socket))
    await asyncio.sleep(0)
    await socket.client_send(
        json.dumps(
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "session.events.subscribe",
                "params": {"sessionId": "abc"},
            }
        )
    )
    response = json.loads(await asyncio.wait_for(socket.client_receive(), 1))
    subscription_id = response["result"]["subscriptionId"]
    notification = json.loads(await asyncio.wait_for(socket.client_receive(), 1))
    assert notification == {
        "jsonrpc": "2.0",
        "method": "session.events",
        "params": {
            "subscriptionId": subscription_id,
            "payload": {"value": "abc"},
        },
    }
    await socket.client_send(
        json.dumps(
            {
                "jsonrpc": "2.0",
                "id": 2,
                "method": "session.events.unsubscribe",
                "params": {"subscriptionId": subscription_id},
            }
        )
    )
    assert (
        json.loads(await asyncio.wait_for(socket.client_receive(), 1))["result"] is None
    )
    await asyncio.wait_for(stopped.wait(), 1)
    stopped.clear()
    await socket.client_send(
        json.dumps(
            {
                "jsonrpc": "2.0",
                "id": 3,
                "method": "session.events.subscribe",
                "params": {"sessionId": "second"},
            }
        )
    )
    assert "result" in json.loads(await asyncio.wait_for(socket.client_receive(), 1))
    await asyncio.wait_for(socket.client_receive(), 1)
    await socket.client_disconnect()
    await task
    assert stopped.is_set()


async def test_subscription_limit_and_contract() -> None:
    channel = RpcChannel("session")

    @channel.server.subscription()
    async def events(params: SessionParams) -> AsyncIterator[SessionEvent]:
        await asyncio.Future()
        yield SessionEvent(value=params.session_id)

    service = RpcService()
    service.socket("/rpc", channels=(channel,))
    document = service.contract(title="Session", base_url="ws://localhost").to_openrpc()
    assert document["x-rpc-subscriptions"][0]["name"] == "session.events"
    socket = InMemorySocket("/rpc")
    task = asyncio.create_task(
        service.serve(socket, limits=RpcLimits(max_subscriptions=1))
    )
    await asyncio.sleep(0)
    for request_id in (1, 2):
        await socket.client_send(
            json.dumps(
                {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "method": "session.events.subscribe",
                    "params": {"sessionId": "abc"},
                }
            )
        )
        response = json.loads(await asyncio.wait_for(socket.client_receive(), 1))
        assert ("result" in response) is (request_id == 1)
    await socket.client_disconnect()
    await task

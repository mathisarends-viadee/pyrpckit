import asyncio
import importlib
import json
import sys
from collections.abc import AsyncIterator
from contextlib import aclosing, suppress
from pathlib import Path
from typing import Any

import pytest

from rpckit import RpcChannel, RpcDisconnect, RpcErrorBinding, RpcModel, RpcService
from rpckit.codegen import generate_python_client
from rpckit.codegen.python import PythonClientOptions
from tests.testing import InMemorySocket


class SessionParams(RpcModel):
    session_id: str


class SessionEvent(RpcModel):
    value: str


class FailureDetails(RpcModel):
    resource_id: int


class SessionMissing(Exception):
    def __init__(self, resource_id: int) -> None:
        self.resource_id = resource_id


@pytest.mark.parametrize("declared", [True, False])
async def test_generated_python_subscription_errors_are_typed(
    tmp_path, declared
) -> None:
    default = RpcErrorBinding(SessionMissing, code="session_unavailable")
    channel = RpcChannel("session", raises=[default] if declared else [])
    binding = RpcErrorBinding(SessionMissing, details=FailureDetails)

    @channel.server.subscription(raises=[binding] if declared else [])
    async def events() -> AsyncIterator[SessionEvent]:
        yield SessionEvent(value="first")
        raise SessionMissing(42)

    service = RpcService()
    service.socket("/rpc", channels=[channel])
    package = f"failure_subscription_client_{declared}"
    generate_python_client(
        service.contract(title="Session", base_url="ws://localhost").to_openrpc(),
        tmp_path / package,
        PythonClientOptions(
            package=package, client_name="SessionClient", with_transport="websocket"
        ),
    )
    sys.path.insert(0, str(tmp_path))
    importlib.invalidate_caches()
    try:
        module = importlib.import_module(package)
        assert not hasattr(module, "SessionUnavailableError")
        socket = InMemorySocket("/rpc")
        server = asyncio.create_task(service.serve(socket))

        class SocketAdapter:
            async def send(self, message: str) -> None:
                await socket.client_send(message)

            async def recv(self) -> str | bytes:
                return await socket.client_receive()

            async def close(self) -> None:
                with suppress(RpcDisconnect):
                    await socket.client_disconnect()

        async def socket_factory(*_: object, **__: object) -> SocketAdapter:
            return SocketAdapter()

        try:
            async with module.SessionClient.connect(
                socket_factory=socket_factory
            ) as client:
                stream = client.session.events()
                async with aclosing(stream):
                    assert (await asyncio.wait_for(anext(stream), 1)).value == "first"
                    error_type = (
                        module.SessionMissingError
                        if declared
                        else module.RpcRemoteError
                    )
                    with pytest.raises(error_type) as caught:
                        await asyncio.wait_for(anext(stream), 1)
                    assert caught.value.code == (
                        "session_missing" if declared else "internal_error"
                    )
                    if declared:
                        assert caught.value.details.resource_id == 42
        finally:
            await server
    finally:
        sys.path.remove(str(tmp_path))
        for name in list(sys.modules):
            if name == package or name.startswith(f"{package}."):
                del sys.modules[name]


async def test_generated_python_subscription_roundtrip(tmp_path: Path) -> None:
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
    package = "subscription_client"
    generate_python_client(
        service.contract(title="Session", base_url="ws://localhost").to_openrpc(),
        tmp_path / package,
        PythonClientOptions(
            package=package,
            client_name="SessionClient",
            with_transport="websocket",
        ),
    )
    sys.path.insert(0, str(tmp_path))
    importlib.invalidate_caches()
    try:
        module = importlib.import_module(package)
        socket = InMemorySocket("/rpc")
        server = asyncio.create_task(service.serve(socket))

        class SocketAdapter:
            async def send(self, message: str) -> None:
                await socket.client_send(message)

            async def recv(self) -> str | bytes:
                return await socket.client_receive()

            async def close(self) -> None:
                with suppress(RpcDisconnect):
                    await socket.client_disconnect()

        async def socket_factory(*_: object, **__: object) -> SocketAdapter:
            return SocketAdapter()

        try:
            async with module.SessionClient.connect(
                socket_factory=socket_factory
            ) as client:
                stream = client.session.events(session_id="abc")
                async with aclosing(stream):
                    result = await asyncio.wait_for(anext(stream), 1)
                    assert result.value == "abc"
                await asyncio.wait_for(stopped.wait(), 1)
        finally:
            await server
    finally:
        sys.path.remove(str(tmp_path))
        for name in list(sys.modules):
            if name == package or name.startswith(f"{package}."):
                del sys.modules[name]


async def test_subscription_resubscribes_after_reconnect(tmp_path: Path) -> None:
    channel = RpcChannel("session")

    @channel.server.subscription()
    async def events(params: SessionParams) -> AsyncIterator[SessionEvent]:
        yield SessionEvent(value=params.session_id)

    service = RpcService()
    service.socket("/rpc", channels=(channel,))
    package = "reconnect_subscription_client"
    generate_python_client(
        service.contract(title="Session", base_url="ws://localhost").to_openrpc(),
        tmp_path / package,
        PythonClientOptions(
            package=package,
            client_name="SessionClient",
            with_transport="websocket",
        ),
    )

    class Socket:
        def __init__(self, generation: int) -> None:
            self.generation = generation
            self.frames: asyncio.Queue[str] = asyncio.Queue()
            self.methods: list[str] = []

        async def send(self, raw: str) -> None:
            request = json.loads(raw)
            method = request["method"]
            self.methods.append(method)
            result: Any = None
            if method.endswith(".subscribe"):
                result = {"subscriptionId": "1"}
            self.frames.put_nowait(
                json.dumps({"jsonrpc": "2.0", "id": request["id"], "result": result})
            )
            if method.endswith(".subscribe"):
                self.frames.put_nowait(
                    json.dumps(
                        {
                            "jsonrpc": "2.0",
                            "method": "session.events",
                            "params": {
                                "subscriptionId": "1",
                                "payload": {"value": str(self.generation)},
                            },
                        }
                    )
                )

        async def recv(self) -> str:
            return await self.frames.get()

        async def close(self) -> None: ...

    sockets: list[Socket] = []

    async def socket_factory(*_: object, **__: object) -> Socket:
        socket = Socket(len(sockets) + 1)
        sockets.append(socket)
        return socket

    sys.path.insert(0, str(tmp_path))
    importlib.invalidate_caches()
    try:
        module = importlib.import_module(package)
        async with module.SessionClient.connect(
            socket_factory=socket_factory,
            reconnect=True,
            reconnect_initial_delay=0.001,
        ) as client:
            stream = client.session.events(session_id="abc")
            async with aclosing(stream):
                assert (await asyncio.wait_for(anext(stream), 1)).value == "1"
                sockets[0].frames.put_nowait("not json")
                assert (await asyncio.wait_for(anext(stream), 1)).value == "2"
            assert sockets[1].methods == [
                "session.events.subscribe",
                "session.events.unsubscribe",
            ]
    finally:
        sys.path.remove(str(tmp_path))
        for name in list(sys.modules):
            if name == package or name.startswith(f"{package}."):
                del sys.modules[name]

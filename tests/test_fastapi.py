import asyncio
import subprocess
import sys
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Annotated, Any
from uuid import UUID

import pytest
from fastapi import APIRouter, Depends, FastAPI, WebSocket, WebSocketDisconnect
from fastapi.testclient import TestClient
from pydantic import BaseModel
from starlette.testclient import WebSocketDenialResponse

from rpckit import (
    Inject,
    ProtocolDefinitionError,
    RpcBinaryInput,
    RpcBinaryOutput,
    RpcChannel,
    RpcConnection,
    RpcDisconnect,
    RpcErrorBinding,
    RpcReject,
    RpcRejection,
    RpcRejections,
    RpcService,
)
from rpckit.fastapi import (
    FastApiSocket,
    RpcRoutes,
    create_router,
    serve_websocket,
)


class EchoParams(BaseModel):
    value: str


def create_service(
    *, path: str = "/rpc", connections: list[RpcConnection] | None = None
) -> RpcService:
    channel = RpcChannel("demo")

    @channel.server.method()
    async def echo(params: EchoParams, connection: Inject[RpcConnection]) -> EchoParams:
        if connections is not None:
            connections.append(connection)
        return params

    service = RpcService()
    service.socket(path, channels=(channel,))
    return service


def test_router_serves_websocket_with_fastapi_options() -> None:
    dependencies_called = []
    handshakes = []

    async def dependency() -> None:
        dependencies_called.append(True)

    web = FastAPI()
    web.include_router(
        create_router(create_service(path="/rpc/{endpoint}", connections=handshakes)),
        prefix="/api",
        dependencies=[Depends(dependency)],
    )

    with (
        TestClient(web) as client,
        client.websocket_connect(
            "/api/rpc/primary?endpoint=ignored&view=compact",
            headers={"x-test": "yes"},
            subprotocols=["rpc.test"],
        ) as websocket,
    ):
        websocket.send_json(
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "demo.echo",
                "params": {"value": "hello"},
            }
        )
        assert websocket.receive_json() == {
            "jsonrpc": "2.0",
            "id": 1,
            "result": {"value": "hello"},
        }

    assert dependencies_called == [True]
    assert len(handshakes) == 1
    connection = handshakes[0]
    assert connection.path == "/api/rpc/primary"
    assert connection.path_params == {"endpoint": "primary"}
    assert connection.query_params == {"endpoint": "ignored", "view": "compact"}
    assert connection.headers["x-test"] == "yes"
    assert connection.subprotocols == ("rpc.test",)
    assert connection.client is not None


def test_repeated_test_client_disconnects_do_not_leak_cancellation() -> None:
    web = FastAPI()
    web.include_router(create_router(create_service()))

    with TestClient(web) as client:
        for value in range(200):
            with client.websocket_connect("/rpc") as websocket:
                websocket.send_json(
                    {
                        "jsonrpc": "2.0",
                        "id": value,
                        "method": "demo.echo",
                        "params": {"value": str(value)},
                    }
                )
                assert websocket.receive_json()["result"] == {"value": str(value)}


def test_router_serves_binary_stream() -> None:
    channel = RpcChannel("media")

    @channel.server.stream()
    async def frames() -> AsyncIterator[bytes]:
        yield b"frame"

    service = RpcService()
    service.stream("/frames", frames)
    web = FastAPI()
    web.include_router(create_router(service))

    with (
        TestClient(web) as client,
        client.websocket_connect("/frames") as websocket,
    ):
        assert websocket.receive_bytes() == b"frame"


async def test_send_translates_websocket_disconnect() -> None:
    class DisconnectedWebSocket:
        async def send_text(self, message: str) -> None:
            raise WebSocketDisconnect()

    socket = FastApiSocket.__new__(FastApiSocket)
    socket._websocket = DisconnectedWebSocket()

    try:
        await socket.send("message")
    except RpcDisconnect:
        pass
    else:
        raise AssertionError("RpcDisconnect was not raised")


async def test_send_preserves_unexpected_runtime_errors() -> None:
    class BrokenWebSocket:
        async def send_text(self, message: str) -> None:
            raise RuntimeError("invalid WebSocket state")

    socket = FastApiSocket.__new__(FastApiSocket)
    socket._websocket = BrokenWebSocket()

    try:
        await socket.send("message")
    except RuntimeError as error:
        assert str(error) == "invalid WebSocket state"
    else:
        raise AssertionError("RuntimeError was not raised")


def test_router_serves_bidirectional_streams_without_cancellation_leaks() -> None:
    channel = RpcChannel("voice")

    @channel.server.stream()
    async def media(
        session_id: int,
        frames: Inject[RpcBinaryInput],
        output: Inject[RpcBinaryOutput],
    ) -> None:
        async for frame in frames:
            await output.send(str(session_id).encode() + b":" + frame)

    service = RpcService()
    service.stream("/voice/{session_id}/media", media)
    web = FastAPI()
    web.include_router(create_router(service))

    with TestClient(web) as client:
        for session_id in range(200):
            with client.websocket_connect(f"/voice/{session_id}/media") as websocket:
                websocket.send_bytes(b"pcm")
                assert websocket.receive_bytes() == f"{session_id}:pcm".encode()
                websocket.send_text('{"type":"end"}')
                assert websocket.receive()["code"] == 1000


def test_router_rejects_invalid_stream_path_variables_as_not_found() -> None:
    channel = RpcChannel("voice")

    @channel.server.stream()
    async def media(session_id: int, output: Inject[RpcBinaryOutput]) -> None: ...

    service = RpcService()
    service.stream("/voice/{session_id}/media", media)
    web = FastAPI()
    web.include_router(create_router(service))

    with (
        TestClient(web) as client,
        pytest.raises(WebSocketDenialResponse) as denied,
        client.websocket_connect("/voice/abc/media"),
    ):
        pass
    assert denied.value.status_code == 404


@pytest.mark.parametrize(
    ("rejection", "status"),
    [(RpcRejection.UNAUTHORIZED, 401), (RpcRejection.FORBIDDEN, 403)],
)
def test_before_accept_rejects_with_http_status_and_headers(rejection, status) -> None:
    service = create_service()

    async def authenticate(handshake):
        if handshake.headers.get("authorization") != "Bearer secret":
            raise RpcReject(
                rejection,
                "Unauthorized",
                headers={"WWW-Authenticate": "Bearer"},
            )
        return None

    web = FastAPI()
    web.include_router(create_router(service, before_accept=authenticate))
    with (
        TestClient(web) as client,
        pytest.raises(WebSocketDenialResponse) as denied,
        client.websocket_connect("/rpc"),
    ):
        pass
    assert denied.value.status_code == status
    assert denied.value.headers["www-authenticate"] == "Bearer"


def test_before_accept_supplies_injected_context() -> None:
    class Principal:
        def __init__(self, name: str) -> None:
            self.name = name

    channel = RpcChannel("auth")

    @channel.server.method()
    async def who(principal: Inject[Principal]) -> str:
        return principal.name

    async def authenticate(handshake):
        return {Principal: Principal("Ada")}

    service = RpcService()
    service.socket("/auth", channels=(channel,), before_accept=authenticate)
    web = FastAPI()
    web.include_router(create_router(service))
    with TestClient(web) as client, client.websocket_connect("/auth") as websocket:
        websocket.send_json({"jsonrpc": "2.0", "id": 1, "method": "auth.who"})
        assert websocket.receive_json()["result"] == "Ada"


def test_serve_websocket_helper_serves_manual_route() -> None:
    service = create_service()
    web = FastAPI()

    @web.websocket("/rpc")
    async def route(websocket: WebSocket) -> None:
        await serve_websocket(service.endpoint("rpc"), websocket)

    with TestClient(web) as client, client.websocket_connect("/rpc") as websocket:
        websocket.send_json(
            {"jsonrpc": "2.0", "id": 1, "method": "demo.echo", "params": {"value": "x"}}
        )
        assert websocket.receive_json()["result"] == {"value": "x"}


def test_import_without_fastapi_reports_missing_extra() -> None:
    code = """
import importlib.abc
import sys

class BlockFastApi(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname == "fastapi" or fullname.startswith("fastapi."):
            raise ModuleNotFoundError("blocked fastapi")
        return None

sys.meta_path.insert(0, BlockFastApi())
try:
    import rpckit.fastapi
except ModuleNotFoundError as error:
    print(error.name, error, sep="|")
"""
    result = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, check=False
    )

    assert result.stdout.strip() == (
        "fastapi|rpckit.fastapi requires the 'fastapi' extra; install pyrpckit[fastapi]"
    )


class Job:
    def __init__(self, job_id: UUID) -> None:
        self.id = job_id


class JobNotFound(Exception):
    pass


job_not_found = RpcErrorBinding(
    JobNotFound, message=str, rejection=RpcRejection.NOT_FOUND
)


class JobOutput:
    def __init__(self, frames: list[bytes]) -> None:
        self.frames = frames


class Actor:
    def __init__(self, name: str) -> None:
        self.name = name


@dataclass(frozen=True)
class JobSession:
    job: Job
    actor: Actor


KNOWN_JOB = UUID(int=1)


async def get_actor() -> Actor:
    return Actor("ada")


async def open_job_session(
    job_id: UUID, actor: Annotated[Actor, Depends(get_actor)]
) -> JobSession:
    if job_id != KNOWN_JOB:
        raise JobNotFound("Job not found")
    return JobSession(Job(job_id), actor)


def create_job_service(
    released: list[bool] | None = None,
    *,
    rejects: tuple[RpcErrorBinding[Any], ...] = (),
) -> RpcService:
    events = RpcChannel("jobs")

    @events.server.method()
    async def describe(session: Inject[JobSession]) -> str:
        return f"{session.actor.name}:{session.job.id}"

    output = RpcChannel("output")

    @output.server.stream()
    async def frames(
        session: Inject[JobSession], source: Inject[JobOutput]
    ) -> AsyncIterator[bytes]:
        try:
            for frame in source.frames:
                yield frame
            await asyncio.Event().wait()
        finally:
            if released is not None:
                released.append(True)

    failing = RpcChannel("failing")

    @failing.server.stream()
    async def broken(session: Inject[JobSession]) -> AsyncIterator[bytes]:
        yield b"first"
        raise JobNotFound("Job was deleted")

    service = RpcService(rejects=rejects)
    service.socket(
        "/jobs/{job_id}/events", channels=(events,), name="events", context=JobSession
    )
    service.stream("/jobs/{job_id}/output", frames, name="output", context=JobSession)
    service.stream("/jobs/{job_id}/broken", broken, name="broken", context=JobSession)
    return service


def create_job_app(
    service: RpcService,
    *,
    rejects: tuple[RpcErrorBinding[Any], ...] = (),
    rejections: RpcRejections | None = None,
) -> FastAPI:
    async def resolve(dependency: type) -> JobOutput:
        return JobOutput([b"one", b"two"])

    router = APIRouter(prefix="/jobs")
    routes = RpcRoutes(
        router,
        context=open_job_session,
        resolver=resolve,
        rejects=rejects,
        rejections=rejections,
    )
    for name in ("events", "output", "broken"):
        routes.mount(service.endpoint(name))
    web = FastAPI()
    web.include_router(router)
    return web


def test_rpc_routes_supply_the_context_to_json_rpc_handlers() -> None:
    web = create_job_app(create_job_service())

    with (
        TestClient(web) as client,
        client.websocket_connect(f"/jobs/{KNOWN_JOB}/events") as websocket,
    ):
        websocket.send_json({"jsonrpc": "2.0", "id": 1, "method": "jobs.describe"})
        assert websocket.receive_json()["result"] == f"ada:{KNOWN_JOB}"


def test_rpc_routes_serve_binary_streams_and_release_them_on_disconnect() -> None:
    released: list[bool] = []
    web = create_job_app(create_job_service(released))

    with (
        TestClient(web) as client,
        client.websocket_connect(f"/jobs/{KNOWN_JOB}/output") as websocket,
    ):
        assert websocket.receive_bytes() == b"one"
        assert websocket.receive_bytes() == b"two"
    assert released == [True]


@pytest.mark.parametrize(
    "rejections",
    [
        {JobNotFound: RpcRejection.NOT_FOUND},
        lambda error: RpcReject(RpcRejection.NOT_FOUND, "Job not found"),
    ],
)
def test_rpc_routes_reject_context_failures_before_accepting(rejections) -> None:
    web = create_job_app(create_job_service(), rejections=rejections)

    with (
        TestClient(web) as client,
        pytest.raises(WebSocketDenialResponse) as denied,
        client.websocket_connect(f"/jobs/{UUID(int=2)}/events"),
    ):
        pass
    assert denied.value.status_code == 404
    assert denied.value.text == "Job not found"


def test_rpc_routes_reject_context_failures_their_contracts_cover() -> None:
    web = create_job_app(create_job_service(), rejects=(job_not_found,))

    with (
        TestClient(web) as client,
        pytest.raises(WebSocketDenialResponse) as denied,
        client.websocket_connect(f"/jobs/{UUID(int=2)}/events"),
    ):
        pass
    assert denied.value.status_code == 404
    assert denied.value.text == "Job not found"


def test_rpc_routes_raises_needs_contracts_with_a_rejection() -> None:
    with pytest.raises(ProtocolDefinitionError, match="with a rejection"):
        RpcRoutes(APIRouter(), rejects=(RpcErrorBinding(JobNotFound),))


def test_rpc_routes_reject_raised_rejections_with_their_headers() -> None:
    async def authenticate(job_id: UUID) -> JobSession:
        raise RpcReject(
            RpcRejection.UNAUTHORIZED,
            "Sign in",
            headers={"WWW-Authenticate": "Bearer"},
        )

    router = APIRouter(prefix="/jobs")
    RpcRoutes(router, context=authenticate).mount(
        create_job_service().endpoint("events")
    )
    web = FastAPI()
    web.include_router(router)

    with (
        TestClient(web) as client,
        pytest.raises(WebSocketDenialResponse) as denied,
        client.websocket_connect(f"/jobs/{KNOWN_JOB}/events"),
    ):
        pass
    assert denied.value.status_code == 401
    assert denied.value.headers["www-authenticate"] == "Bearer"


def test_rpc_routes_preserve_unmapped_context_failures() -> None:
    web = create_job_app(create_job_service())

    with (
        TestClient(web) as client,
        pytest.raises(JobNotFound),
        client.websocket_connect(f"/jobs/{UUID(int=2)}/events"),
    ):
        pass


def test_rpc_routes_close_accepted_streams_with_mapped_rejections() -> None:
    web = create_job_app(
        create_job_service(), rejections={JobNotFound: RpcRejection.NOT_FOUND}
    )

    with (
        TestClient(web) as client,
        client.websocket_connect(f"/jobs/{KNOWN_JOB}/broken") as websocket,
    ):
        assert websocket.receive_bytes() == b"first"
        assert websocket.receive() == {
            "type": "websocket.close",
            "code": 1008,
            "reason": "Job was deleted",
        }


def test_rpc_routes_fall_back_to_the_service_contracts() -> None:
    web = create_job_app(
        create_job_service(rejects=(job_not_found,)),
        rejections={KeyError: RpcRejection.UNAUTHORIZED},
    )

    with TestClient(web) as client:
        with (
            pytest.raises(WebSocketDenialResponse) as denied,
            client.websocket_connect(f"/jobs/{UUID(int=2)}/events"),
        ):
            pass
        with client.websocket_connect(f"/jobs/{KNOWN_JOB}/broken") as websocket:
            assert websocket.receive_bytes() == b"first"
            assert websocket.receive()["code"] == 1008
    assert denied.value.status_code == 404


def test_rpc_routes_key_a_subclass_context_by_the_declared_type() -> None:
    class GuestJobSession(JobSession):
        pass

    async def open_guest_session(job_id: UUID) -> GuestJobSession:
        return GuestJobSession(Job(job_id), Actor("guest"))

    router = APIRouter(prefix="/jobs")
    RpcRoutes(router, context=open_guest_session).mount(
        create_job_service().endpoint("events")
    )
    web = FastAPI()
    web.include_router(router)

    with (
        TestClient(web) as client,
        client.websocket_connect(f"/jobs/{KNOWN_JOB}/events") as websocket,
    ):
        websocket.send_json({"jsonrpc": "2.0", "id": 1, "method": "jobs.describe"})
        assert websocket.receive_json()["result"] == f"guest:{KNOWN_JOB}"


def test_rpc_routes_without_context_mount_endpoints_without_context() -> None:
    router = APIRouter()
    RpcRoutes(router).mount(create_service().endpoint("rpc"))
    web = FastAPI()
    web.include_router(router)

    with TestClient(web) as client, client.websocket_connect("/rpc") as websocket:
        websocket.send_json(
            {"jsonrpc": "2.0", "id": 1, "method": "demo.echo", "params": {"value": "x"}}
        )
        assert websocket.receive_json()["result"] == {"value": "x"}


def test_rpc_routes_use_a_fastapi_resolver_per_connection() -> None:
    resolved: list[WebSocket] = []
    prepared: list[str] = []

    class Resolver:
        def for_websocket(self, websocket: WebSocket):
            async def resolve(dependency: type) -> JobOutput:
                resolved.append(websocket)
                return JobOutput([b"resolved"])

            return resolve

        def dependency(self, function):
            prepared.append(function.__name__)
            return function

    router = APIRouter(prefix="/jobs")
    RpcRoutes(router, context=open_job_session, resolver=Resolver()).mount(
        create_job_service().endpoint("output")
    )
    web = FastAPI()
    web.include_router(router)

    with (
        TestClient(web) as client,
        client.websocket_connect(f"/jobs/{KNOWN_JOB}/output") as websocket,
    ):
        assert websocket.receive_bytes() == b"resolved"
    assert len(resolved) == 1
    assert prepared == ["open_job_session"]


async def untyped_session(job_id: UUID):
    return None


async def any_session(job_id: UUID) -> Any:
    return None


async def open_actor(job_id: UUID) -> Actor:
    return Actor("ada")


def test_rpc_routes_reject_invalid_mounts() -> None:
    service = create_job_service()
    routes = RpcRoutes(APIRouter(prefix="/job"), context=open_job_session)

    with pytest.raises(ValueError, match="outside the router prefix"):
        routes.mount(service.endpoint("events"))
    for context in (untyped_session, any_session):
        with pytest.raises(TypeError, match="needs a concrete class"):
            RpcRoutes(APIRouter(), context=context)
    with pytest.raises(TypeError, match="expects an async function"):
        RpcRoutes(APIRouter(), context=JobSession)  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="declares context none, but these routes"):
        RpcRoutes(APIRouter(), context=open_actor).mount(
            create_service().endpoint("rpc")  # type: ignore[arg-type]
        )
    with pytest.raises(TypeError, match="declares context JobSession, but these"):
        RpcRoutes(APIRouter(prefix="/jobs")).mount(service.endpoint("events"))
    with pytest.raises(TypeError, match="JobSession, but these routes provide Actor"):
        RpcRoutes(APIRouter(prefix="/jobs"), context=open_actor).mount(
            service.endpoint("events")
        )

    routes = RpcRoutes(APIRouter(prefix="/jobs"), context=open_job_session)
    routes.mount(service.endpoint("events"))
    with pytest.raises(ValueError, match="already mounted"):
        routes.mount(service.endpoint("events"))

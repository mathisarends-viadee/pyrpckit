# Transports

rpckit's core deals in an `RpcSocket` protocol, not a web framework. Use the
FastAPI adapter or implement that small protocol for another server.

## FastAPI

Install the optional adapter:

```bash
uv add "pyrpckit[fastapi]"
```

Mount every endpoint declared on a service:

```python
from fastapi import FastAPI

from rpckit.fastapi import create_router

web = FastAPI()
web.include_router(create_router(app))
```

Common runtime options are configured once on the router:

```python
from fastapi import Depends


web.include_router(
    create_router(
        app,
        resolver=resolver,
        context={Settings: settings},
        limits=limits,
        error_mapper=map_error,
    ),
    prefix="/api",
    dependencies=[Depends(authenticate)],
)
```

When a resolver can only be built after the app exists, use
`resolver_factory=`. It receives the live `WebSocket` once per connection:

```python
web.include_router(
    create_router(
        app,
        resolver_factory=lambda websocket: Resolver(websocket.app.state.container),
    )
)
```

Configure FastAPI concerns such as the prefix and dependencies through
`include_router()`. The adapter registers JSON-RPC and binary-stream endpoints
as WebSocket routes and maps pre-acceptance rejections to HTTP denial responses
when the server supports the WebSocket denial extension. Without that extension,
the ASGI server falls back to a generic HTTP 403 denial.

FastAPI dependencies on `include_router()` are useful for transport-level
checks such as authentication. Their return values are not injected into RPC
methods; use `context=` or `resolver=` for application dependencies, or mount
endpoints individually with `RpcRoutes`.

### Mount endpoints with FastAPI dependencies

`RpcRoutes` mounts endpoints on an existing router, including routers from
other libraries. An endpoint declares the type of its connection context with
`context=`; its handlers receive that value as `Inject[T]`:

```python
from collections.abc import AsyncIterator

from rpckit import Inject, RpcChannel, RpcService

output = RpcChannel("output")


@output.server.stream(content_type="text/plain")
async def lines(job: Inject[Job]) -> AsyncIterator[bytes]:
    async for line in job.output():
        yield line


app = RpcService()
job_events = app.socket("/jobs/{job_id}/events", channels=(jobs,), context=Job)
job_output = app.stream("/jobs/{job_id}/output", lines, context=Job)
```

`RpcRoutes` supplies that value through an async context function. It runs as a
regular FastAPI dependency per connection, path parameters included:

```python
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends

from rpckit.fastapi import RpcRoutes


async def open_job(
    job_id: UUID,
    actor: Annotated[Actor, Depends(authenticate)],
    jobs: Annotated[JobRepository, Depends(get_job_repository)],
) -> Job:
    return await jobs.get_authorized(job_id, actor)


router = APIRouter(prefix="/jobs")
job_routes = RpcRoutes(
    router,
    context=open_job,
    rejects=[job_not_found, job_access_denied],
)
job_routes.mount(job_events)
job_routes.mount(job_output)
web.include_router(router)
```

The routes take their type from the context function's return annotation, so
`job_routes` is an `RpcRoutes[Job]` and type checkers reject endpoints that
declare another context or none. `mount()` checks the same at runtime; a
context function annotated with a subclass of the declared type is accepted.
Routes without `context=` mount endpoints without a declared context. To supply
several values, declare a dataclass as the context.

`mount()` adds one WebSocket route at the endpoint's declared path, so it
always matches the contract. Mounting fails when that path lies outside the
router's prefix or the endpoint is already mounted. JSON-RPC sockets and binary
streams mount the same way.

`resolver=` accepts any rpckit resolver for the remaining `Inject[T]` values.
To integrate a DI library, pass an object implementing `FastApiResolver`
instead: it creates a resolver per WebSocket and may wrap the context function.
`rpckit.dishka.Dishka` is one such [integration](dependencies.md#dishka).

`rejects=` takes [error bindings](errors.md#bind-domain-exceptions) with a
`rejection=`, and `rejections=` takes a mapping or callable. Both follow
[the core rules](connections-and-events.md#map-failures-to-rejections), are
consulted before the `rejects=` of the mounted endpoint and its service, and
also cover the context function and its dependencies: before acceptance the
handshake is rejected with an HTTP denial response, after acceptance the
socket closes with the matching code. Unmapped failures propagate unchanged.
Dependencies passed to `APIRouter(dependencies=...)` run before this check, so
map their failures with FastAPI's own exception handling.

### Write your own route

When a route needs its own code around the connection, such as a path that
differs from the contract, call `serve_websocket()` from a regular FastAPI
WebSocket route. It serves JSON-RPC sockets and binary streams alike, accepts
the same runtime options as `create_router()`, and returns quietly when
Starlette cancels the task after a disconnect:

```python
from fastapi import WebSocket

from rpckit.fastapi import serve_websocket


@web.websocket("/legacy/rpc")
async def legacy_rpc(websocket: WebSocket) -> None:
    await serve_websocket(app.endpoint("events"), websocket, context={Clock: clock})
```

Prefer `RpcRoutes` for routes that only need FastAPI dependencies.

## Custom adapters

Implement `RpcSocket` and pass it to `app.serve(socket)`. The protocol consists
of a handshake property and six async operations:

```python
from rpckit import RpcHandshake


class MySocket:
    @property
    def handshake(self) -> RpcHandshake: ...

    async def accept(self, subprotocol: str | None = None) -> None: ...
    async def reject(self, rejection, reason: str) -> None: ...
    async def receive(self) -> str | bytes: ...
    async def send(self, message: str) -> None: ...
    async def send_bytes(self, data: bytes) -> None: ...
    async def close(self, close, reason: str) -> None: ...
```

`RpcHandshake` carries the request path, headers, query parameters, path
parameters, offered subprotocols, and optional client address. Signal client
disconnects by raising `RpcDisconnect` from `receive()` or a send operation.

`app.serve()` matches the handshake path to the declared endpoint. Pass
`root_path=` when an upstream server has already consumed a URL prefix.

For low-level dispatch tests that do not need connection behavior, obtain an
`RpcServer` from `endpoint.create_server(...)` or `channel.create_server(...)` and call
`handle()` or `handle_json()` directly.

[Back to documentation](README.md)

# Connections and events

`RpcConnection` is available as an injected dependency in methods and events.
Use it to inspect connection metadata or close an accepted connection. Perform
authentication in the hosting framework before calling rpckit.

```python
from collections.abc import AsyncIterator

from rpckit import (
    Inject,
    RpcChannel,
    RpcConnection,
    RpcModel,
    RpcService,
)

tasks = RpcChannel("tasks")
app = RpcService()
app.socket("/rpc", channels=(tasks,))
```

Headers are case-insensitive. `RpcConnection` also exposes `endpoint`, `path`,
`path_params`, `query_params`, `subprotocols`, `client`, `closed`, `close_code`,
`raw_close_code`, and `close_reason`. `close_code` is always a
`RpcConnectionClose` value; unknown peer codes become `OTHER` and remain
available as `raw_close_code`. Connection-scope finalizers can inspect these
fields to log why a socket ended.

Inject it like any other server-side dependency:

```python
@tasks.server.method()
async def connection_path(connection: Inject[RpcConnection]) -> str:
    return connection.path
```

After acceptance, injected code can close the live connection:

```python
from rpckit import RpcConnection, RpcConnectionClose


@tasks.server.method()
async def sign_out(connection: Inject[RpcConnection]) -> None:
    await connection.close(RpcConnectionClose.NORMAL, reason="Signed out")
```

A `NORMAL` or `SHUTDOWN` close first sends the responses and notifications
already queued for the connection, within `RpcLimits.send_timeout` in total.
Other close codes close at once. Requests still running are cancelled, so their
responses are not sent.

## Map failures to rejections

`rejections=` turns domain failures into connection-level refusals. It takes a
mapping from exception types to `RpcRejection` values, using the exception text
as reason, or a callable that returns an `RpcReject` or `None`:

```python
from rpckit import RpcReject, RpcRejection

rejections = {
    TaskNotFound: RpcRejection.NOT_FOUND,
    TaskAccessDenied: RpcRejection.FORBIDDEN,
}


def reject(error: Exception) -> RpcReject | None:
    if isinstance(error, TaskAccessDenied):
        return RpcReject(RpcRejection.FORBIDDEN, "Access denied")
    return None
```

Pass it to `serve()`, `create_router()`, `serve_websocket()`, or
`RpcTestClient`. The connection state decides what a rejection becomes:

| Failure | Result |
|---|---|
| `before_accept` hook, before the handshake | Handshake rejected (with the FastAPI adapter, an HTTP status such as 403 or 404) |
| Binary stream handler, after the handshake | Close with 1008 (`UNAUTHORIZED`, `FORBIDDEN`, `NOT_FOUND`), 1002, 1013, or 1011 |
| Event source, after the handshake | Same close codes, regardless of the event's `on_error` |
| JSON-RPC method | Unaffected; methods answer with JSON-RPC errors |

Raising `RpcReject` directly has the same effect without a mapping. Failures
the mapping does not cover keep their previous behavior.

## Push typed events

An event is an async generator. Each yielded Pydantic value becomes a JSON-RPC
notification sent to every connection on that endpoint:

```python
class TaskUpdated(RpcModel):
    task_id: int
    title: str


class TaskEvents:
    async def subscribe(self) -> AsyncIterator[TaskUpdated]: ...


@tasks.server.event(summary="Publish task changes.")
async def updated(events: Inject[TaskEvents]) -> AsyncIterator[TaskUpdated]:
    async for update in events.subscribe():
        yield update
```

The wire notification name is `tasks.updated`. Events accept only injected
parameters; callers do not subscribe with request parameters. A union of
Pydantic models is supported for event families, and a literal `type` field can
serve as their discriminator in generated clients.

The payload type comes from `AsyncIterator[T]`, so events do not need
`payload=`. The optional `@channel.server.event(payload=...)` only asserts that type
and fails at definition time when it differs from the yielded type.

Events expect no answer. When the server needs the client's result, declare a
[client method](client-methods.md) instead.

### Finite event sockets

By default a socket stays open after its event sources finish. For a socket
that only reports a finite job, let the endpoint close it:

```python
@exports.server.event()
async def progress(job: Inject[ExportJob]) -> AsyncIterator[ExportProgress]:
    async for percent in job.run():
        yield ExportProgress(percent=percent)
    yield ExportProgress(percent=100, done=True)


app.socket(
    "/exports/{job_id}",
    channels=(exports,),
    close_when_events_complete=True,
)
```

Once every event source of the socket has finished, the queued notifications
are sent and the connection closes with `NORMAL`. A failing source closes it as
described in [Map failures to rejections](#map-failures-to-rejections). The
service rejects the option on a socket whose channels declare no events.

An event source can also close earlier with `await connection.close()`; the
notifications it yielded before are still delivered.

## Subscriptions with parameters

Declare an async generator when each connection needs its own filtered event
stream:

```python
class TaskFilter(RpcModel):
    project_id: str


@tasks.server.subscription()
async def changes(params: TaskFilter) -> AsyncIterator[TaskUpdated]:
    async for update in task_bus.listen(params.project_id):
        yield update
```

The generated Python client exposes `client.tasks.changes(project_id="demo")`
as an async iterator. Close the iterator promptly when leaving a loop early:

```python
from contextlib import aclosing


async with aclosing(client.tasks.changes(project_id="demo")) as changes:
    async for update in changes:
        if update.title == "Done":
            break
```

The generated TypeScript client accepts a typed params object. Breaking a
`for await` loop sends the unsubscribe request automatically.

On the wire, `tasks.changes.subscribe` returns a `subscriptionId`,
`tasks.changes` notifications carry `{subscriptionId, payload}`, and
`tasks.changes.unsubscribe` stops that generator. The generator's `finally`
block runs on unsubscribe and disconnect. `RpcLimits.max_subscriptions`
limits active subscriptions per connection (default 100). The OpenRPC document
describes these streams in `x-rpc-subscriptions`. Existing server events remain
broadcast notifications without subscription parameters.

## Observe requests

Configure one observer on the service or override it on an endpoint. Observer
state belongs to that service instance, so tests and parallel apps remain
isolated:

```python
from rpckit import RpcObserver


class GatewayObserver(RpcObserver):
    async def request_started(self, context: RpcRequestContext) -> None:
        logger.info("rpc started method=%s", context.method)

    async def request_finished(self, context: RpcResponseContext) -> None:
        logger.info(
            "rpc method=%s id=%s duration=%f success=%s",
            context.request.method,
            context.request.request_id,
            context.duration,
            isinstance(context.response, RpcSuccess),
        )

    async def connection_opened(self, connection: RpcConnection) -> None:
        logger.info("rpc connected endpoint=%s", connection.endpoint)

    async def connection_closed(self, context: RpcConnectionContext) -> None:
        logger.info("rpc disconnected code=%s", context.close_code)


app = RpcService(observer=GatewayObserver())
```

Observer failures are logged without replacing the RPC result. There is no
module-level observer or global configuration.
`RpcObserver` supplies no-op callbacks, so subclasses only override what they
need. `RpcObserverLike` remains available for structural typing. Request
contexts expose `connection` for socket requests and `None` when using a
standalone `RpcServer`. The optional callbacks `notification_sent(name, size)`,
`stream_frame_sent(connection, size)`,
`stream_frame_received(connection, size)`, and
`slow_consumer_closed(connection)` report activity and backpressure; `size` is
the number of bytes sent or received.

## Runtime limits

Configure per-connection backpressure and message limits with `RpcLimits`:

```python
from rpckit import RpcLimits

limits = RpcLimits(
    max_concurrency=16,
    max_queue_size=64,
    max_message_bytes=512_000,
)
```

`max_concurrency` bounds in-flight calls, `max_pending_requests` bounds calls
that are running or waiting for a slot (further calls are answered with a
`pending_limit` error), `max_queue_size` bounds outgoing responses and events,
and `max_message_bytes` rejects oversized incoming frames. Calls on one connection may finish out of order; set concurrency to
`1` when strict arrival order is required. Pass `None` only when intentionally
disabling the byte limit.

[Back to documentation](README.md)

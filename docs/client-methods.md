# Client methods

Server methods let the client call the server, and events let the server push
notifications without an answer. Client methods cover the third direction: the
server sends a request to a connected client and waits for its result or
error.

A channel groups its declarations by the side that implements them. The client
opens the connection, but either side can initiate a JSON-RPC request on it:

```python
room_channel.server.method(...)  # client calls server, server responds
room_channel.server.event(...)  # server sends, no response
room_channel.client.method(...)  # server calls client, client responds
```

The two request directions have different jobs in the Python API:

| Declaration | Implementation | Caller |
| --- | --- | --- |
| `@channel.server.method(...)` | Decorated function on the server | Generated client method |
| `channel.client.method(...)` | Generated client handler | `RpcConnectedClient.call(...)` on the server |

`channel.client.method(...)` describes the request and returns the typed token
used by `RpcConnectedClient.call(...)`. It does not implement the client method;
the generated client's `*Handler` class provides that implementation.

Server streams are declared the same way, with `channel.server.stream(...)`.

Both sides can send JSON-RPC requests on the same socket. The Language
Server Protocol relies on this pattern (`workspace/applyEdit`), and OpenAPI
calls it `callbacks`.

A typical use is a client that dials out to the server and owns local
hardware, such as a room process that controls a speaker. The client stays
unreachable from outside, and the socket gives the server presence for free.
The server can still send it commands and learn the outcome.

## Declare a client method

A client method is a typed declaration, not a decorated function, because the
server has nothing to implement:

```python
from rpckit import RpcChannel, RpcError, RpcModel


class MediaPlayParams(RpcModel):
    uri: str


class MediaPlayResult(RpcModel):
    started: bool


class MediaUnavailableError(RpcError):
    rpc_code = -32010


room = RpcChannel("room")
media = room.child("media")

media_play = media.client.method(
    "play",
    params=MediaPlayParams,
    result=MediaPlayResult,
    raises=(MediaUnavailableError,),
    summary="Play a media URI on the room's speaker.",
)
```

`client.method()` returns an `RpcClientMethod[MediaPlayParams, MediaPlayResult]`
named `room.media.play`. Like server methods, client method names are
relative to the channel namespace, so `room.client.method("media.play", ...)`
declares the same name without a child channel. Omit `params=` for a client method without params, and omit
`result=` for one that answers `null`. A client method name must not collide
with a server method, event, or stream name. Channel-level `raises=` apply to
server methods only, because client method errors come from the client.

## Call the client

`RpcConnectedClient` is the server's handle for one connected client. It is
injectable on JSON-RPC socket endpoints. Keep these handles in your own registry
to call a client from anywhere, not only while handling one of its requests:

```python
from rpckit import Inject, RpcConnectedClient


class RoomConnections:
    def __init__(self) -> None:
        self.clients: dict[str, RpcConnectedClient] = {}


class HelloParams(RpcModel):
    room_id: str


@room.server.method("hello")
async def hello(
    params: HelloParams,
    client: Inject[RpcConnectedClient],
    rooms: Inject[RoomConnections],
) -> None:
    rooms.clients[params.room_id] = client


async def play(rooms: RoomConnections, room_id: str, uri: str) -> bool:
    client = rooms.clients[room_id]
    result = await client.call(media_play, MediaPlayParams(uri=uri), timeout=5.0)
    return result.started
```

`client.call()` validates the params against the declared model and the answer
against the declared result model. Server-originated requests use string IDs
(`"server:1"`, `"server:2"`, ...), and the reader routes responses to the
connected client without ever answering them. `client.call()` fails as follows:

| Situation | Raised |
| --- | --- |
| The client answers with a declared error code | That `RpcError` subclass, with its details |
| The client answers with any other error | `RpcClientMethodFailedError` (`rpc_code`, `code`, `message`, `details`) |
| The result does not match the declared model | `RpcClientMethodResultError` |
| `timeout` elapses; a late response is dropped | `RpcClientMethodTimeoutError`, also a `TimeoutError` |
| The connection closes before the answer | `RpcClientClosedError` |

All of these except the declared errors derive from `RpcClientMethodError`. A
connected client is bound to its endpoint: calling a client method that the endpoint does
not mount
raises `ValueError`. `client.closed` reports whether the connection has ended,
and `client.connection` returns its `RpcConnection`. Remove clients from your
registry in a connection-scoped finalizer or when a call raises
`RpcClientClosedError`.

Outgoing calls respect the endpoint's `RpcLimits`: `max_concurrency` bounds
unanswered client method calls per connection, and a request larger than
`max_message_bytes` raises `ValueError` before it is sent.

## Contract

Client methods appear under the `x-rpc-client-methods` extension of the OpenRPC
document, next to `x-rpc-notifications`. Entries have the same shape as regular
`methods`: params, result, errors, `x-rpc-params-schema`, and a request schema
named after the client method, such as `RoomMediaPlayClientMethod`.

## Implement client methods in a generated Python client

For every client method namespace, the generated Python client contains an
abstract handler class named after the namespace path, such as
`RoomMediaHandler`. Implement the class and pass an instance to `connect()`:

```python
from rooms_client import (
    MediaPlayParams,
    MediaPlayResult,
    MediaUnavailableError,
    RoomMediaHandler,
    RoomsClient,
    SpeakerDetails,
)


class Media(RoomMediaHandler):
    async def play(self, params: MediaPlayParams) -> MediaPlayResult:
        if not speaker.online:
            raise MediaUnavailableError.create(SpeakerDetails(speaker_id="s1"))
        await speaker.play(params.uri)
        return MediaPlayResult(started=True)


async with RoomsClient.connect(url=url, handlers=Media()) as client:
    ...
```

`handlers=` accepts one handler or an iterable of handlers. One object may
implement several namespace classes. Registration works per namespace: a client
method whose class has no handler is answered with `-32601 Method not found`, as
is every request sent to a client generated without client methods. Invalid
params are answered with `-32602`. Raise a declared error with its generated
`create()` to answer with that error. Any other exception is logged and answered
with `-32603`. Handlers run concurrently, so a slow handler never delays
responses to the client's own requests.

With custom transports, build the dispatcher yourself and pass it to the
generated transport:
`WebSocketTransport(socket, request_handler=handler_dispatcher(Media()))`.
Generated TypeScript clients do not implement client methods yet.

[Back to documentation](README.md)

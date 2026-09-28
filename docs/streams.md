# Binary streams

Binary streams carry raw bytes on a dedicated socket. They are a good fit for
audio, video frames, uploads, screenshots, or other data that should not be
base64-encoded inside JSON-RPC messages.

The handler's signature determines the stream's direction:

| Handler | Direction |
|---|---|
| async generator returning `AsyncIterator[bytes]` | `server-to-client` |
| async function injecting `RpcBinaryOutput` | `server-to-client`, push-based |
| async function injecting `RpcBinaryInput` | `client-to-server` |
| async function injecting both | `bidirectional` |

## Define and mount a stream

```python
from collections.abc import AsyncIterator

from rpckit import Inject, RpcChannel, RpcService


class FrameSource:
    async def frames(self) -> AsyncIterator[bytes]:
        yield b"frame"


media = RpcChannel("media")


@media.server.stream(content_type="image/jpeg", summary="Live preview frames.")
async def preview(source: Inject[FrameSource]) -> AsyncIterator[bytes]:
    async for frame in source.frames():
        yield frame


app = RpcService()
app.stream("/projects/{project_id}/preview", preview)
```

A generator stream must be annotated as `AsyncIterator[bytes]` or
`AsyncGenerator[bytes, ...]`. Each decorated stream can be mounted once.

The stream's operation name is `media.preview`; the mounted path becomes its
own WebSocket endpoint. Path variables are filled from the contract's server
variables by generated clients.

## Receive and send frames

Inject `RpcBinaryInput` to receive the client's frames. Inject `RpcBinaryOutput`
to send frames whenever the application produces them:

```python
import asyncio

from rpckit import RpcBinaryInput, RpcBinaryOutput


uploads = RpcChannel("uploads")


@uploads.server.stream("audio", input_content_type="audio/pcm")
async def upload_audio(
    frames: Inject[RpcBinaryInput],
    store: Inject[RecordingStore],
) -> None:
    async with store.open() as recording:
        async for frame in frames:
            await recording.write(frame)


@media.server.stream("talk", content_type="audio/opus", input_content_type="audio/pcm")
async def talk(
    frames: Inject[RpcBinaryInput],
    output: Inject[RpcBinaryOutput],
    voice: Inject[VoiceService],
) -> None:
    async with asyncio.TaskGroup() as group:
        group.create_task(voice.consume(frames))
        async for chunk in voice.replies():
            await output.send(chunk)
```

A handler that injects `RpcBinaryInput` or `RpcBinaryOutput` is a regular async
function that returns `None`. An async generator cannot inject them; call
`await output.send(frame)` instead of yielding. `content_type=` describes the
output and `input_content_type=` describes the input. If you omit
`input_content_type=`, the input uses `content_type`.

Iterating `RpcBinaryInput` ends when the client ends its input. After that,
`receive()` raises `RpcInputEnded` and `ended` is true. Output keeps flowing
until the handler returns, so a bidirectional stream can reply after the
input has ended. `RpcBinaryOutput.send()` waits while the socket applies
backpressure. The application decides whether to buffer or drop frames, for
example for real-time audio.

Every WebSocket binary message is one opaque frame. rpckit adds no framing.
Put sequence numbers, turn markers, and other metadata in a small header inside
your payload, or send them with a regular RPC call.

## Path variables

Handler parameters without `Inject[...]` are variables of the mounted path.
Pydantic validates them before the WebSocket is accepted:

```python
@voice.server.stream("media", content_type="audio/pcm")
async def media(
    voice_session_id: UUID,
    frames: Inject[RpcBinaryInput],
    output: Inject[RpcBinaryOutput],
) -> None: ...


app.stream("/v1/voice-sessions/{voice_session_id}/media", media)
```

Use scalar types such as `str`, `int`, `UUID`, or an `Enum`. Mounting fails if a
parameter is not a variable of the path. If a value fails validation, the
handshake is rejected with `RpcRejection.NOT_FOUND` (HTTP 404, or close code
1008), because the path does not name a resource. Path variables without a
parameter remain available through `RpcConnection.path_params`.

## Lifecycle

The dependency lifecycle works exactly as it does for a JSON-RPC endpoint. The
socket closes normally when the generator or handler finishes. An exception
closes the stream with an internal error (1011), unless it is an `RpcReject` or
[mapped to a rejection](connections-and-events.md#map-failures-to-rejections).
A frame larger than `RpcLimits.max_message_bytes` closes it with 1009.

The client ends its input with the text message `{"type":"end"}`. Any other
text message, binary input after the end, or input on a server-to-client stream
closes the stream with a protocol error (1002). If the client disconnects
before it ends its input, the upload counts as aborted, even with the normal
close code. rpckit cancels the handler, and its `finally` blocks and context
managers run. A completed upload therefore always ends with `{"type":"end"}`.

Incoming frames wait in a queue that holds up to `RpcLimits.max_queue_size`
frames. When the handler falls behind, rpckit stops reading from the socket.
Memory stays bounded, and the transport slows the client down.

## Exclusive ownership

rpckit has no connect hook, so the handler claims its resources itself. An
`async with` block holds the claim for exactly as long as the handler runs. The
claim is released on return, disconnect, cancellation, or error:

```python
class MediaOwners:
    def __init__(self) -> None:
        self._owners: set[UUID] = set()
        self._lock = asyncio.Lock()

    @asynccontextmanager
    async def claim(self, key: UUID) -> AsyncIterator[bool]:
        async with self._lock:
            claimed = key not in self._owners
            self._owners.add(key)
        try:
            yield claimed
        finally:
            if claimed:
                self._owners.discard(key)


@voice.server.stream("media", content_type="audio/pcm")
async def media(
    voice_session_id: UUID,
    frames: Inject[RpcBinaryInput],
    output: Inject[RpcBinaryOutput],
    connection: Inject[RpcConnection],
    owners: Inject[MediaOwners],
) -> None:
    async with owners.claim(voice_session_id) as claimed:
        if not claimed:
            await connection.close(
                RpcConnectionClose.POLICY_VIOLATION,
                reason="Voice session already has a media owner",
            )
            return
        ...
```

Provide `MediaOwners` as an application-scoped dependency. The refusal happens
after the handshake, as close code 1008, and generated clients raise
`RpcStreamRefused`. If a check must reject the handshake itself, put it in your
framework route before `serve()`, as you would for authentication.

## Contract and generated clients

OpenRPC has no standard binary-stream primitive, so rpckit writes stream
metadata to `x-rpckit-binary-streams`. Both generators consume that extension
and expose stream operations beside the regular namespace APIs. With the
generated WebSocket transport, the client also receives a ready-to-use binary
stream opener.

The stream is placed on the same namespace tree as its wire name. For the
`media.preview` stream above:

```python
async with MediaClient.connect(host="api.example.com") as client:
    async with client.media.preview() as frames:
        async for frame in frames:
            render(frame)
```

A stream call returns an async context manager, and `async with` is the only
way to open it. Leaving the block closes the dedicated stream socket, so a
stream cannot leak. To keep a stream open across calls, enter it on an
`AsyncExitStack`.

TypeScript opens the stream asynchronously and supports explicit resource
management:

```ts
await using client = await MediaClient.connect({ host: "api.example.com" });
await using frames = await client.media.preview();

for await (const frame of frames) {
  render(frame);
}
```

Stream URL templates inherit the variables supplied to `connect()`, such as
`host`. A stream method only takes the stream's own path variables, such as
`client.voice.media(session_id=...)`, which no server declares. To redirect a
stream, for example to a test server, wrap the `stream_opener` or
`stream_socket_factory`: both receive the resolved endpoint.

A client-to-server stream returns a `BinarySender`. Leaving the
`async with` block normally ends the input and waits for the server to close
the stream. Leaving it with an exception aborts the upload instead:

```python
async with client.uploads.audio() as upload:
    async for pcm in microphone.frames():
        await upload.send(pcm)
```

To finish explicitly, call `await upload.end()`. If the server does not close
normally, it raises `RpcStreamFailed` with the close `code` and `reason`. If
the server has not closed the stream within 30 seconds, `end()` raises
`TimeoutError`. Pass `end(timeout=...)` to change the limit, or `timeout=None`
to wait without one.

A bidirectional stream returns a `BinaryChannel`. It receives like a
server-to-client stream and can also send:

```python
async with client.voice.media(voice_session_id=session_id) as media:
    async with asyncio.TaskGroup() as group:
        group.create_task(pump_microphone(media))
        async for chunk in media:
            speaker.play(chunk)


async def pump_microphone(media: BinaryChannel) -> None:
    async for pcm in microphone.frames():
        await media.send(pcm)
    await media.end_input()
```

A stream closed with code 1008 raises `RpcStreamRefused`. Other error codes
raise `RpcStreamFailed`. TypeScript mirrors this API with `send()`,
`endInput()`, and `end()`. Disposal in TypeScript cannot see exceptions, so a
TypeScript sender completes only through `await upload.end()`. Disposing it
without `end()` aborts the upload. There, `end({ timeoutMs })` rejects with
`RpcStreamTimeout` after 30 000 ms by default; `timeoutMs: Infinity` waits
without a limit.

Iteration ends normally when the server closes a stream. Calling `receive()`
directly after a regular close raises `RpcStreamClosed`, which lets code that
does not iterate distinguish end-of-stream from an empty frame. A client built
with custom transports needs a `stream_opener` / `streamOpener`; otherwise a
stream call raises `RpcStreamsUnavailableError`. For client-to-server and
bidirectional streams, the transport that the opener returns must also
implement `send()` and `end_input()` / `endInput()`.

Starting an operation and opening its byte stream remain two explicit actions.
For example, call a regular `start()` RPC method first and then open its sibling
stream. The contract does not currently link a stream to a start method.

[Back to documentation](README.md)

from collections.abc import AsyncIterator

import pytest

from rpckit import (
    ProtocolDefinitionError,
    RpcChannel,
    RpcError,
    RpcModel,
    RpcServer,
    RpcService,
)


class SharedError(RpcError):
    pass


class LocalError(RpcError):
    pass


class Event(RpcModel):
    value: str


def test_channel_defaults_and_bare_decorators() -> None:
    channel = RpcChannel("control", raises=[SharedError])

    @channel.server.method
    async def ping() -> None: ...

    @channel.server.method(raises=[LocalError])
    async def other() -> None: ...

    assert channel.namespace == "control"
    assert channel.routes[0].name == "control.ping"
    assert channel.routes[1].raises == (SharedError, LocalError)


def test_event_and_stream_inference() -> None:
    channel = RpcChannel("events")

    @channel.server.event
    async def changed() -> AsyncIterator[Event]:
        yield Event(value="x")

    @channel.server.stream(content_type="image/png")
    async def frames() -> AsyncIterator[bytes]:
        """Published frames."""
        yield b"x"

    assert channel.events[0].payload is Event
    assert channel.streams[0].name == "events.frames"
    assert channel.streams[0].summary == "Published frames."


def test_freeze_prevents_registration() -> None:
    channel = RpcChannel("control", namespace="")
    channel.freeze()
    with pytest.raises(ProtocolDefinitionError, match="frozen"):
        channel.server.method()


def test_invalid_stream_type_is_rejected() -> None:
    channel = RpcChannel("files")
    with pytest.raises(ProtocolDefinitionError, match="must yield bytes"):

        @channel.server.stream()
        async def text() -> AsyncIterator[str]:
            yield "x"


def test_child_channels_inherit_namespace_errors_and_scope() -> None:
    root = RpcChannel("voice", raises=(SharedError,))
    turn = root.child("turn", raises=(LocalError,))

    @root.server.event()
    async def event() -> AsyncIterator[Event]:
        yield Event(value="root")

    @turn.server.method()
    async def start() -> None: ...

    protocol = root.freeze()

    assert turn.name == "voice.turn"
    assert turn.namespace == "voice.turn"
    assert turn.raises == (SharedError, LocalError)
    assert protocol.methods[0].name == "voice.turn.start"
    assert protocol.methods[0].raises == (SharedError, LocalError)
    assert protocol.notifications[0].name == "voice.event"


def test_channel_name_defaults_to_namespace() -> None:
    channel = RpcChannel(namespace="voice.turn")

    assert channel.name == "voice.turn"
    assert channel.namespace == "voice.turn"


def test_dotted_names_are_relative_to_the_channel_namespace() -> None:
    channel = RpcChannel("input", namespace="browser.input", raises=[SharedError])
    nested = channel.child("pointer.touch")

    @channel.server.method("text.insert")
    async def insert_text() -> None: ...

    @channel.server.event("text.changed")
    async def text_changed() -> AsyncIterator[Event]:
        yield Event(value="x")

    @channel.server.subscription("text.watch")
    async def watch_text() -> AsyncIterator[Event]:
        yield Event(value="x")

    @channel.server.stream("screen.frames")
    async def screen_frames() -> AsyncIterator[bytes]:
        yield b""

    channel.client.method("clipboard.read")
    protocol = channel.freeze()

    assert protocol.methods[0].name == "browser.input.text.insert"
    assert protocol.methods[0].raises == (SharedError,)
    assert protocol.methods[0].request_name == "InsertTextRequest"
    assert protocol.notifications[0].name == "browser.input.text.changed"
    assert protocol.subscriptions[0].name == "browser.input.text.watch"
    assert protocol.streams[0].name == "browser.input.screen.frames"
    assert protocol.client_methods[0].name == "browser.input.clipboard.read"
    assert nested.namespace == "browser.input.pointer.touch"


@pytest.mark.parametrize("name", ["text..insert", ".insert", "text.", "text.1st", ""])
def test_each_segment_of_a_dotted_name_is_validated(name: str) -> None:
    channel = RpcChannel("input")

    with pytest.raises(ProtocolDefinitionError, match="Invalid RPC method name"):
        channel.server.method(name)


def test_dotted_name_and_child_route_cannot_share_a_wire_name() -> None:
    channel = RpcChannel("input")

    @channel.server.method("text.insert")
    async def insert_text() -> None: ...

    @channel.child("text").server.method("insert")
    async def insert() -> None: ...

    with pytest.raises(ProtocolDefinitionError, match="Duplicate RPC method"):
        channel.freeze()


def test_dotted_name_cannot_turn_an_operation_into_a_namespace() -> None:
    channel = RpcChannel("input")

    @channel.server.method
    async def text() -> None: ...

    @channel.server.method("text.insert")
    async def insert_text() -> None: ...

    service = RpcService()
    service.socket("/rpc", channels=(channel,))
    with pytest.raises(ProtocolDefinitionError, match="both an operation"):
        service.freeze()


class PlayParams(RpcModel):
    uri: str


class PlayResult(RpcModel):
    started: bool


class MediaUnavailableError(RpcError):
    rpc_code = -32010


def test_channel_groups_declarations_by_the_implementing_side() -> None:
    channel = RpcChannel("room")

    @channel.server.method
    async def join() -> None: ...

    @channel.server.event("joined")
    async def joined_events() -> AsyncIterator[Event]:
        yield Event(value="x")

    assert [route.name for route in channel.routes] == ["room.join"]
    assert [event.name for event in channel.events] == ["room.joined"]
    assert isinstance(channel.create_server(), RpcServer)
    assert not hasattr(channel, "method")


def test_client_method_declares_a_typed_server_to_client_request() -> None:
    channel = RpcChannel("room")
    media = channel.child("media")

    play = media.client.method(
        "play",
        params=PlayParams,
        result=PlayResult,
        raises=(MediaUnavailableError,),
        summary="Play a media URI.",
    )

    assert play.name == "room.media.play"
    assert play.params is PlayParams
    assert play.result is PlayResult
    assert play.raises == (MediaUnavailableError,)
    assert play.summary == "Play a media URI."
    assert channel.protocol.client_methods == (play,)


def test_client_method_without_params_or_result_answers_null() -> None:
    channel = RpcChannel("room")

    ping = channel.client.method("ping")

    assert ping.params is None
    assert ping.result is type(None)


def test_client_method_does_not_inherit_channel_errors() -> None:
    channel = RpcChannel("room", raises=[SharedError])

    ping = channel.client.method("ping")

    assert ping.raises == ()


@pytest.mark.parametrize("declare", ["method", "event", "stream"])
def test_client_method_names_collide_with_other_operations(declare: str) -> None:
    channel = RpcChannel("room")

    async def play() -> None: ...

    async def play_events() -> AsyncIterator[Event]:
        yield Event(value="x")

    async def play_frames() -> AsyncIterator[bytes]:
        yield b"x"

    if declare == "method":
        channel.server.method("play")(play)
    elif declare == "event":
        channel.server.event("play")(play_events)
    else:
        channel.server.stream("play")(play_frames)

    with pytest.raises(ProtocolDefinitionError, match="Duplicate RPC route"):
        channel.client.method("play")


def test_client_method_rejects_non_model_params() -> None:
    channel = RpcChannel("room")

    with pytest.raises(ProtocolDefinitionError, match="Pydantic model"):
        channel.client.method("play", params=dict)  # type: ignore[arg-type]

import pytest

from rpckit import ProtocolDefinitionError, RpcChannel, RpcLimits, RpcService


def test_endpoint_matching_and_static_priority() -> None:
    dynamic = RpcChannel("dynamic")
    static = RpcChannel("static")
    service = RpcService()
    service.socket("/items/{item_id}", channels=(dynamic,), name="item")
    expected = service.socket("/items/special", channels=(static,), name="special")
    endpoint, values = service.match("/items/special")
    assert endpoint is expected
    assert values == {}
    assert service.match("/items/a%20b")[1] == {"item_id": "a b"}


@pytest.mark.parametrize("path", ["rpc", "/rpc/", "/a//b", "/rpc?q=1", "/rpc#x"])
def test_invalid_paths(path: str) -> None:
    with pytest.raises(ProtocolDefinitionError):
        RpcService().socket(path, channels=(RpcChannel("channel"),))


def test_channel_cannot_be_mounted_twice() -> None:
    channel = RpcChannel("control")
    service = RpcService()
    service.socket("/one", channels=(channel,))
    with pytest.raises(ProtocolDefinitionError, match="only be mounted once"):
        service.socket("/two", channels=(channel,))


def test_endpoints_record_their_declared_context() -> None:
    class Session:
        pass

    service = RpcService()
    session = service.socket(
        "/session", channels=(RpcChannel("session"),), context=Session
    )
    plain = service.socket("/plain", channels=(RpcChannel("plain"),))

    assert session.context is Session
    assert plain.context is None
    with pytest.raises(ProtocolDefinitionError, match="context must be a class"):
        service.socket(
            "/invalid",
            channels=(RpcChannel("invalid"),),
            context="Session",  # type: ignore[arg-type]
        )


def test_service_freezes_channels() -> None:
    channel = RpcChannel("control")
    service = RpcService()
    service.socket("/rpc", channels=(channel,))
    service.freeze()
    with pytest.raises(ProtocolDefinitionError, match="frozen"):
        service.socket("/other", channels=(RpcChannel("other"),))


def test_endpoints_inherit_and_override_serve_configuration() -> None:
    def default_mapper(error: Exception):
        return None

    def endpoint_mapper(error: Exception):
        return None

    default_limits = RpcLimits(max_concurrency=4)
    endpoint_limits = RpcLimits(max_concurrency=2)
    service = RpcService(error_mapper=default_mapper, limits=default_limits)
    inherited = service.socket("/one", channels=(RpcChannel("one"),), name="one")
    overridden = service.socket(
        "/two",
        channels=(RpcChannel("two"),),
        name="two",
        error_mapper=endpoint_mapper,
        limits=endpoint_limits,
    )

    assert inherited.error_mapper is default_mapper
    assert inherited.limits is default_limits
    assert overridden.error_mapper is endpoint_mapper
    assert overridden.limits is endpoint_limits


def test_mounting_a_root_channel_includes_its_children() -> None:
    root = RpcChannel("voice")
    turn = root.child("turn")

    @turn.server.method()
    async def start() -> None: ...

    service = RpcService()
    service.socket("/rpc", channels=(root,))

    assert service.protocol.methods[0].name == "voice.turn.start"


def test_client_methods_belong_to_the_endpoint_that_mounts_their_channel() -> None:
    room = RpcChannel("room")
    other = RpcChannel("other")
    play = room.client.method("play")
    service = RpcService()
    service.socket("/rooms", channels=(room,), name="rooms")
    service.socket("/other", channels=(other,), name="other")

    (client_method,) = service.endpoint("rooms").protocol.client_methods

    assert client_method.name == play.name
    assert client_method.server == "rooms"
    assert service.endpoint("other").protocol.client_methods == ()


def test_client_method_names_collide_across_channels() -> None:
    first = RpcChannel("first", namespace="room")
    second = RpcChannel("second", namespace="room")

    @first.server.method("play")
    async def play() -> None: ...

    second.client.method("play")
    service = RpcService()
    service.socket("/rpc", channels=(first, second))

    with pytest.raises(ProtocolDefinitionError, match="Duplicate RPC name"):
        service.freeze()


def test_close_when_events_complete_requires_events() -> None:
    service = RpcService()
    service.socket(
        "/rpc", channels=(RpcChannel("plain"),), close_when_events_complete=True
    )
    with pytest.raises(ProtocolDefinitionError, match="declare no events"):
        service.freeze()

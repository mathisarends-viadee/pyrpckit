import importlib.util

import pytest

import rpckit
from rpckit import RpcRejection, RpcServer


@pytest.mark.parametrize(
    "name",
    (
        "RpcFeatureDefinition",
        "RpcHandler",
        "RpcNotificationDefinition",
        "RpcApp",
        "RpcRouter",
        "OpenRpcContract",
        "OpenRpcServer",
        "ConnectionRejected",
        "feature",
        "event",
        "method",
        "notification",
    ),
)
def test_removed_pre_v1_composition_symbols_are_not_public(name: str) -> None:
    assert not hasattr(rpckit, name)


def test_servers_are_created_by_an_app() -> None:
    with pytest.raises(TypeError, match="RpcChannel.create_server"):
        RpcServer()


def test_authentication_rejections_are_public() -> None:
    assert RpcRejection.UNAUTHORIZED.value == "unauthorized"
    assert RpcRejection.FORBIDDEN.value == "forbidden"


def test_test_helpers_are_distributed() -> None:
    assert importlib.util.find_spec("rpckit.testing") is not None


def test_public_signature_types_are_exported() -> None:
    for name in (
        "RpcResolverLike",
        "RpcResponseMessage",
        "RpcProtocol",
        "RpcErrorBinding",
        "RpcErrorDeclaration",
    ):
        assert name in rpckit.__all__

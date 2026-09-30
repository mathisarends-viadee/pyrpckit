# Services and channels

JSON-RPC methods accept parameters by name as a JSON object. Positional
parameter arrays are not supported.

A channel owns related operations and their wire namespace. A service mounts
one or more channels on concrete socket paths. Keeping those concepts separate
lets one application expose several independently addressable APIs while still
producing one contract.

## Define a method

Methods are async free functions. Their public input is either absent or one
Pydantic model, and their return annotation describes the result.

```python
from rpckit import RpcChannel, RpcModel, RpcService


class CreateTask(RpcModel):
    title: str


class Task(RpcModel):
    id: int
    title: str


tasks = RpcChannel("tasks")


@tasks.server.method()
async def create(params: CreateTask) -> Task:
    return Task(id=1, title=params.title)


@tasks.server.method()
async def health() -> None:
    return None


app = RpcService(version=1)
app.socket("/rpc", channels=(tasks,))
```

The methods are exposed as `tasks.create` and `tasks.health`. Use an explicit
decorator name when the Python and wire names should differ:

```python
@tasks.server.method("list", summary="List the current tasks.")
async def list_tasks() -> list[Task]:
    return []
```

If `summary` is omitted, the first line of the function docstring becomes the
contract summary. Handlers may add any number of injected parameters after the
optional params model; see [Dependency injection](dependencies.md).

## Namespaces

By default, the channel name is also its namespace. Set a different namespace,
or use an empty one for root-level method names:

```python
admin = RpcChannel("admin", namespace="internal.admin")
system = RpcChannel("system", namespace="")
```

Channels, operations, and namespaces must remain unambiguous across a service.
rpckit rejects duplicate names and cases where an operation is also the
prefix of another operation when the protocol is materialized.

## Mount endpoints

Mount several channels on one JSON-RPC socket:

```python
app = RpcService(version=2)
endpoint = app.socket(
    "/projects/{project_id}/rpc",
    channels=(tasks, admin),
    name="project-rpc",
    subprotocol="rpc.v2",
    summary="Project control API.",
)
```

Path variables are available from `RpcConnection.path_params`. Endpoint names
identify servers in OpenRPC and generated clients; when omitted, the last
static path segment is used. A channel can be mounted only once in a service.

Use child channels for nested wire namespaces. Mount only the root; its children
inherit declared errors and the resolver scope:

```python
voice = RpcChannel("voice", raises=(ResourceNotFoundError,))
turn = voice.child("turn")


@turn.server.method()
async def start() -> None: ...  # voice.turn.start


app.socket("/rpc", channels=(voice,))
```

A single nested operation does not need a child channel. Names passed to the
decorators are relative to the channel namespace and may contain dots; each
segment is validated:

```python
@voice.server.method("settings.update")
async def update_settings() -> None: ...  # voice.settings.update
```

Use a child channel when several operations share a namespace or need their
own `raises=` or `resolver_scope=`; local bindings override inherited bindings
for the same domain exception. A dotted name uses its channel's
configuration.

For a standalone dotted namespace, the channel name can default to it:
`RpcChannel(namespace="voice.turn")`.

The service stays mutable until `freeze()`, `protocol`, `contract()`, or an
adapter materializes its protocol. Add all channels and endpoints before that
point.

## Protocol version

`RpcService(version=...)` takes a positive integer. The version is emitted as
`<version>.0.0` in OpenRPC, so changing a wire contract can be reflected in the
service definition and generated artifacts together.

Configure shared serving defaults once and override them only where an endpoint
differs:

```python
app = RpcService(error_mapper=map_error, limits=RpcLimits(max_concurrency=16))
app.socket(
    "/rpc",
    channels=(tasks,),
    limits=RpcLimits(max_concurrency=4),
)
```

`serve()` and `create_router()` use these defaults unless an explicit call-site
override is supplied.

## Use Pydantic directly

`RpcModel` is rpckit's strict Pydantic base model. Existing Pydantic
`BaseModel` classes are supported too, so domain models do not need to inherit
from `RpcModel` merely to appear in an RPC signature.

[Back to documentation](README.md)

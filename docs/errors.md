# Typed errors

Expected application failures should be part of the contract. An `RpcError`
subclass defines a stable application code, a JSON-RPC integer code, a default
message, and optional typed details.

```python
from rpckit import RpcError, RpcModel


class MissingTaskDetails(RpcModel):
    task_id: int


class MissingTaskError(RpcError):
    code = "task_missing"
    rpc_code = -32004
    message = "Task not found"
    details: MissingTaskDetails
```

Declare expected errors on a method and raise them with a details model or its
fields:

```python
@tasks.server.method(raises=(MissingTaskError,))
async def get(params: GetTask) -> Task:
    task = await find_task(params.task_id)
    if task is None:
        raise MissingTaskError(task_id=params.task_id)
    return task
```

On the wire, JSON-RPC's numeric code remains in `error.code`. The stable
application code and validated details are placed in `error.data`. Generated
clients turn declared failures back into concrete exception classes.

## Defaults and rules

When omitted, `code` is derived from the class name: `MissingTaskError` becomes
`missing_task`. The message is derived from that code, and `rpc_code` defaults
to `-32000`.

Both `Error` and `RpcError` suffixes are removed during code derivation, so
`VoiceTurnAlreadyActiveRpcError` becomes `voice_turn_already_active`.

Application codes must be lowercase identifiers using letters, digits, and
underscores. JSON-RPC's standard and reserved numeric codes cannot be claimed
by application errors. Details, when declared, must be a Pydantic model and
are required when constructing the error.

Errors may be declared for every method in a channel:

```python
tasks = RpcChannel("tasks", raises=(PermissionDeniedError,))
```

Method-level declarations are added to the channel-level set. With
`RpcService(strict_errors=True)`, an application error absent from the
method's `raises=` declaration becomes an internal error and is logged.

Clients should identify application errors by `error.data.code`. The numeric
`error.code` is optional for application-specific identity; distinct errors may
share it. The service warns when explicitly assigned numeric codes collide.

## Bind domain exceptions

Domain code should not have to import rpckit. An `RpcErrorContract` binds an
exception it raises to the RPC error that answers it, so the exception stays
free of transport concerns:

```python
# tasks/exceptions.py
class TaskNotFound(Exception):
    def __init__(self, task_id: int) -> None:
        self.task_id = task_id
```

```python
# tasks/rpc_errors.py
from rpckit import RpcErrorContract, RpcModel, RpcRejection


class TaskRef(RpcModel):
    task_id: int


task_not_found = RpcErrorContract(
    TaskNotFound,
    message="Task not found",
    details=TaskRef,
    rejection=RpcRejection.NOT_FOUND,
)
```

Declare the contract in `raises=` like an `RpcError` subclass. A method that
lets `TaskNotFound` escape answers with the `task_not_found` error:

```python
@tasks.server.method(raises=[task_not_found])
async def get(params: GetTask, repository: Inject[TaskRepository]) -> Task:
    return await repository.get(params.task_id)
```

The contract accepts the same settings as an `RpcError` subclass, with the same
defaults: `code` is derived from the exception name (`TaskNotFound` becomes
`task_not_found`), `message` from that code, and `rpc_code` defaults to
`-32000`. The dynamic parts are read from the exception:

- `details=` takes a Pydantic model filled from the exception's attributes, or
  a function annotated to return one, such as
  `def lock_details(error: TaskLocked) -> LockDetails`.
- `message=` takes a string or a function of the exception. `message=str`
  passes the exception text; do that only when the text is written for
  clients.
- `rejection=` is the `RpcRejection` the failure becomes when it ends a
  connection instead of a call. See
  [Map failures to rejections](connections-and-events.md#map-failures-to-rejections).

The contract matches subclasses of its exception, and the most specific
contract wins. OpenRPC documents and generated clients describe a contract
exactly like an `RpcError` subclass with the same settings. An exception binds
to one contract per service. Client methods keep declaring `RpcError`
subclasses, because the server receives these errors and cannot rebuild a
domain exception from them.

`RpcService(errors={TaskNotFound: TaskNotFoundRpcError})` is deprecated in
favor of contracts. `error_mapper` remains available for failures that need
code to decide.

## Unexpected exceptions

Undeclared implementation failures are returned as an internal JSON-RPC error
without exposing their exception text. A transport may supply an
`error_mapper` to map selected exceptions to application errors, while logs
retain the original failure for operators.

Validation, parse, invalid-request, and unknown-method failures use rpckit's
built-in JSON-RPC errors and do not need to be declared.

## Generated client behavior

Declared error classes are exported from generated Python and TypeScript
packages. Catching the concrete class preserves the stable application code and
gives typed access to declared details:

```python
from tasks_client import MissingTaskError

try:
    await client.tasks.get(task_id=42)
except MissingTaskError as error:
    print(error.details.task_id)
```

```ts
import { MissingTaskError } from "./tasks-client";

try {
  await client.tasks.get({ taskId: 42 });
} catch (error) {
  if (error instanceof MissingTaskError) {
    console.log(error.details.taskId);
  }
}
```

An undeclared application code becomes `RpcRemoteError`, retaining its numeric
`rpc_code` / `rpcCode`, string `code`, message, and raw details. Python also
validates declared error details, responses, and notifications against their
generated Pydantic models. Invalid payloads fall back to `RpcRemoteError` for
error details or raise `RpcResponseValidationError` /
`RpcNotificationValidationError` for successful data. TypeScript types these
payloads at compile time but does not perform runtime schema validation.

Connection and stream lifecycle failures are separate from remote application
errors. TypeScript exports `RpcConnectionClosed`; Python transport failures are
available through `RpcClientError` and its exported subclasses. Direct binary
stream reads use `RpcStreamClosed` for a regular remote close, while async
iteration treats that condition as normal completion.

[Back to documentation](README.md)

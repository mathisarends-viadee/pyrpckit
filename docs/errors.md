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

Operation-level declarations extend the channel-level set. A local binding
replaces an inherited binding for the same domain exception class. With
`RpcService(strict_errors=True)`, an application error absent from the
method's `raises=` declaration becomes an internal error and is logged.
Subclasses of a declared `RpcError` are accepted. Error bindings always require
an operation declaration, regardless of `strict_errors`.

Clients should identify application errors by `error.data.code`. The numeric
`error.code` is optional for application-specific identity; distinct errors may
share it. The service warns when explicitly assigned numeric codes collide.

## Bind domain exceptions

Domain code should not have to import rpckit. An `RpcErrorBinding` binds an
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
from rpckit import RpcErrorBinding, RpcModel, RpcRejection


class TaskRef(RpcModel):
    task_id: int


task_not_found = RpcErrorBinding(
    TaskNotFound,
    message="Task not found",
    details=TaskRef,
    rejection=RpcRejection.NOT_FOUND,
)
```

Declare the binding in `raises=` like an `RpcError` subclass. A method that
lets `TaskNotFound` escape answers with the `task_not_found` error:

```python
@tasks.server.method(raises=[task_not_found])
async def get(params: GetTask, repository: Inject[TaskRepository]) -> Task:
    return await repository.get(params.task_id)
```

The binding accepts the same settings as an `RpcError` subclass, with the same
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

Bindings are resolved only from the operation's `raises=`, including inherited
channel declarations. The most specific exception class wins. An undeclared
domain exception becomes a logged `internal_error` even with `strict_errors=False`.
Different methods and channels can bind the same exception differently; two
conflicting bindings for the same exception within one declaration are rejected
immediately, with the channel or operation name in the error. A child channel's
bindings override its parent's, and a method, event, or subscription's bindings
override its channel's. Other inherited errors remain in effect, including
bindings for more specific exception subclasses. Overrides do not mutate the
parent channel or affect sibling operations.

```python
permission_denied = RpcErrorBinding(PermissionError, code="permission_denied")
admin_required = RpcErrorBinding(PermissionError, code="admin_required")

tasks = RpcChannel("tasks", raises=[permission_denied])


@tasks.server.method(raises=[admin_required])
async def delete(params: TaskRef) -> None:
    raise PermissionError()
```

`tasks.delete` declares and returns `admin_required`; other operations in
`tasks` keep `permission_denied`. Repeating the same binding is idempotent;
different bindings for the same exception on the same level are an error.

OpenRPC documents and generated clients describe a binding exactly like an
`RpcError` subclass with the same settings. Client methods keep declaring `RpcError`
subclasses, because the server receives these errors and cannot rebuild a
domain exception from them.

`RpcService(errors=...)`, `RpcChannel.create_server(errors=...)`, and
`RpcService.errors` have been removed. Declare bindings on the operations that
can return them. `error_mapper` remains available for failures that need code
to decide.

For `details=Model`, required model fields must appear in the exception's
annotations, attributes/properties, or `__init__` parameters. Missing names
raise `ProtocolDefinitionError` when the binding is constructed. Fields are
read by their Python names, so wire aliases such as `taskId` need no matching
exception attribute. For custom mappings, supply an annotated function instead.
Unresolvable or invalid callback annotations raise `ProtocolDefinitionError`
at binding definition, identifying the exception and preserving the original
cause.
Static checks cannot prove that a constructor stores its arguments or that a
callback returns valid values. Validate representative instances in tests:

```python
task_not_found.check(TaskNotFound(1))
```

`check()` validates the actual details and dynamic message, raising the original
validation error on failure. At runtime, a failed binding or `error_mapper` is
logged and answers with `internal_error`. The binding's server-side error class
uses an `RpcError` suffix (`TaskNotFoundRpcError`) to distinguish it from the
domain exception. Access this class through `task_not_found.error_type`;
there is no `binding.error` alias. Generated client exception names keep their
`Error` suffix.

Events and subscriptions also accept `raises=` and inherit channel declarations.
Subscription failures send a terminal notification with `complete: false` and
an `error` object containing JSON-RPC `code`, `message`, and `data` (application
code and typed details). Generated Python and TypeScript iterators raise the
declared client error. Undeclared failures send `internal_error` and are logged.
An event binding with `rejection=` closes the connection with that rejection;
otherwise `on_error="close"` uses its mapped message as the close reason.

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

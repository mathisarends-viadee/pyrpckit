import logging
from collections.abc import AsyncIterator

import pytest
from pydantic import BaseModel, ValidationError

from rpckit import (
    ProtocolDefinitionError,
    RpcChannel,
    RpcError,
    RpcErrorBinding,
    RpcErrorCode,
    RpcInvalidParamsError,
    RpcModel,
    RpcRejection,
    RpcService,
)
from rpckit.envelopes import RpcFailure


class MissingDetails(RpcModel):
    project_id: str


class ProjectNotFoundError(RpcError):
    details: MissingDetails


class HTTPTimeoutError(RpcError):
    rpc_code = -32010


class VoiceTurnAlreadyActiveRpcError(RpcError):
    pass


class _PrivateError(RpcError):
    pass


def test_error_metadata_is_derived() -> None:
    assert ProjectNotFoundError.code == "project_not_found"
    assert ProjectNotFoundError.message == "Project not found"
    assert HTTPTimeoutError.code == "http_timeout"
    assert HTTPTimeoutError.rpc_code == -32010
    assert VoiceTurnAlreadyActiveRpcError.code == "voice_turn_already_active"
    assert _PrivateError.code == "private"
    assert _PrivateError.message == "Private"


def test_details_accept_fields_or_model() -> None:
    expected = MissingDetails(project_id="p-1")
    assert ProjectNotFoundError(project_id="p-1").details == expected
    assert ProjectNotFoundError(expected).details == expected
    with pytest.raises(TypeError):
        ProjectNotFoundError()


def test_wire_error_has_string_code_and_details() -> None:
    failure = RpcFailure.from_error(7, ProjectNotFoundError(project_id="p-1"))
    assert failure.model_dump(mode="json", exclude_none=True) == {
        "jsonrpc": "2.0",
        "id": 7,
        "error": {
            "code": -32000,
            "message": "Project not found",
            "data": {
                "code": "project_not_found",
                "details": {"projectId": "p-1"},
            },
        },
    }


def test_direct_error_and_invalid_declarations_are_rejected() -> None:
    with pytest.raises(TypeError):
        RpcError()
    with pytest.raises(ProtocolDefinitionError):

        class BadCode(RpcError):
            code = "Bad-Code"

    with pytest.raises(ProtocolDefinitionError):

        class BadDetails(RpcError):
            details: str


def test_invalid_params_has_structured_issues() -> None:
    class Input(BaseModel):
        name: str

    with pytest.raises(ValidationError) as caught:
        Input.model_validate({})
    error = RpcInvalidParamsError.from_validation_error(caught.value)
    assert error.code == "invalid_params"
    assert error.details.issues[0].loc == ["name"]

    manual = RpcInvalidParamsError(message="Invalid domain value")
    assert manual.details.issues == []
    assert manual.message == "Invalid domain value"


def test_positional_message_mistake_has_a_targeted_error() -> None:
    class ResourceNotFoundError(RpcError):
        pass

    with pytest.raises(TypeError, match=r"did you mean.*message="):
        ResourceNotFoundError("Not found: x")  # type: ignore[arg-type]


class TaskNotFound(Exception):
    def __init__(self, task_id: int) -> None:
        super().__init__(f"task {task_id} is not in the database")
        self.task_id = task_id


class ArchivedTaskNotFound(TaskNotFound):
    pass


class TaskLocked(Exception):
    def __init__(self, owner: str) -> None:
        self.owner = owner


class TaskRef(RpcModel):
    task_id: int


class LockDetails(RpcModel):
    locked_by: str


def lock_details(error: TaskLocked) -> LockDetails:
    return LockDetails(locked_by=error.owner)


task_not_found = RpcErrorBinding(
    TaskNotFound, details=TaskRef, rejection=RpcRejection.NOT_FOUND
)
task_locked = RpcErrorBinding(
    TaskLocked,
    code="task_locked",
    message=lambda error: f"Locked by {error.owner}",
    details=lock_details,
    rpc_code=-32009,
)


def _task_service(*, strict_errors: bool = False) -> RpcService:
    tasks = RpcChannel("tasks", raises=[task_not_found])

    @tasks.server.method(raises=[task_locked])
    async def update(params: TaskRef) -> None:
        if params.task_id == 1:
            raise TaskLocked("ada")
        raise ArchivedTaskNotFound(params.task_id)

    @tasks.server.method()
    async def lock(params: TaskRef) -> None:
        raise TaskLocked("grace")

    service = RpcService(strict_errors=strict_errors)
    service.socket("/tasks", channels=(tasks,))
    return service


async def _call(service: RpcService, method: str, task_id: int) -> RpcFailure:
    server = service.endpoint("tasks").create_server()
    response = await server.handle(
        {"jsonrpc": "2.0", "id": 1, "method": method, "params": {"task_id": task_id}}
    )
    assert isinstance(response, RpcFailure)
    return response


def test_contract_metadata_is_derived_like_rpc_errors() -> None:
    assert task_not_found.code == "task_not_found"
    assert task_not_found.error_type.message == "Task not found"
    assert task_not_found.error_type.rpc_code == RpcErrorCode.SERVER_ERROR
    assert task_not_found.error_type.details_type is TaskRef
    assert task_locked.error_type.details_type is LockDetails
    assert task_locked.error_type.__name__ == "TaskLockedRpcError"
    assert not hasattr(task_not_found, "error")


async def test_contracts_answer_the_domain_exceptions_they_bind() -> None:
    service = _task_service()

    missing = await _call(service, "tasks.update", 7)
    locked = await _call(service, "tasks.update", 1)

    assert missing.error.code == RpcErrorCode.SERVER_ERROR
    assert missing.error.message == "Task not found"
    assert missing.error.data.code == "task_not_found"
    assert missing.error.data.details == TaskRef(task_id=7)
    assert locked.error.code == -32009
    assert locked.error.message == "Locked by ada"
    assert locked.error.data.details == LockDetails(locked_by="ada")


async def test_bindings_are_always_scoped_to_the_method(caplog) -> None:
    lenient = await _call(_task_service(), "tasks.lock", 2)
    strict = await _call(_task_service(strict_errors=True), "tasks.lock", 2)

    assert lenient.error.code == RpcErrorCode.INTERNAL_ERROR
    assert strict.error.code == RpcErrorCode.INTERNAL_ERROR
    assert "tasks.lock failed" in caplog.text


async def test_a_failing_contract_becomes_an_internal_error(caplog) -> None:
    def broken_details(error: TaskLocked) -> TaskRef:
        raise ValueError("bad details")

    broken = RpcErrorBinding(TaskLocked, details=broken_details)
    tasks = RpcChannel("tasks")

    @tasks.server.method(raises=[broken])
    async def update(params: TaskRef) -> None:
        raise TaskLocked("ada")

    service = RpcService()
    service.socket("/tasks", channels=(tasks,))

    with caplog.at_level(logging.ERROR):
        failure = await _call(service, "tasks.update", 1)

    assert failure.error.code == RpcErrorCode.INTERNAL_ERROR
    assert "task_locked" in caplog.text


def test_contracts_are_described_like_rpc_errors() -> None:
    document = (
        _task_service()
        .contract(title="Tasks", base_url="wss://example.com")
        .to_openrpc()
    )

    update = next(m for m in document["methods"] if m["name"] == "tasks.update")
    assert update["errors"] == [
        {
            "code": -32000,
            "message": "Task not found",
            "x-rpckit-code": "task_not_found",
            "x-rpckit-details-schema": {"$ref": "#/components/schemas/TaskRef"},
        },
        {
            "code": -32009,
            "message": "Task locked",
            "x-rpckit-code": "task_locked",
            "x-rpckit-details-schema": {"$ref": "#/components/schemas/LockDetails"},
        },
    ]


async def test_an_exception_can_have_different_bindings_per_method() -> None:
    tasks = RpcChannel("tasks")

    @tasks.server.method(raises=[task_not_found])
    async def read() -> None:
        raise TaskNotFound(1)

    @tasks.server.method(raises=[RpcErrorBinding(TaskNotFound, code="gone")])
    async def delete() -> None:
        raise TaskNotFound(1)

    service = RpcService()
    service.socket("/tasks", channels=(tasks,))
    service.freeze()
    server = service.endpoint("tasks").create_server()
    for name, code in [("read", "task_not_found"), ("delete", "gone")]:
        result = await server.handle(
            {"jsonrpc": "2.0", "id": 1, "method": f"tasks.{name}"}
        )
        assert result.error.data.code == code


async def test_method_binding_overrides_channel_default() -> None:
    channel = RpcChannel("tasks", raises=[task_not_found])
    gone = RpcErrorBinding(TaskNotFound, code="gone")

    @channel.server.method(raises=[gone])
    async def read() -> None:
        raise TaskNotFound(1)

    @channel.server.method()
    async def lookup() -> None:
        raise TaskNotFound(1)

    assert channel.raises == (task_not_found.error_type,)
    server = channel.create_server()
    assert server.protocol.method("tasks.read").raises == (gone.error_type,)
    assert server.protocol.method("tasks.lookup").raises == (task_not_found.error_type,)
    service = RpcService()
    service.socket("/tasks", channels=[channel])
    document = service.contract(title="Tasks", base_url="ws://localhost").to_openrpc()
    read_document = next(
        method for method in document["methods"] if method["name"] == "tasks.read"
    )
    assert [error["x-rpckit-code"] for error in read_document["errors"]] == ["gone"]
    for name, code in [("read", "gone"), ("lookup", "task_not_found")]:
        result = await server.handle(
            {"jsonrpc": "2.0", "id": 1, "method": f"tasks.{name}"}
        )
        assert result.error.data.code == code


@pytest.mark.parametrize("kind", ["method", "event", "subscription"])
def test_ambiguous_bindings_are_rejected_during_registration(kind) -> None:
    channel = RpcChannel("tasks")
    gone = RpcErrorBinding(TaskNotFound, code="gone")

    async def read() -> None: ...

    async def events() -> AsyncIterator[TaskRef]:
        yield TaskRef(task_id=1)

    function = read if kind == "method" else events
    decorate = getattr(channel.server, kind)
    with pytest.raises(
        ProtocolDefinitionError,
        match=rf"RPC {kind} tasks.custom.read: TaskNotFound.*task_not_found.*gone",
    ):
        decorate("custom.read", raises=[task_not_found, gone])(function)

    # A rejected definition must not reserve its name.
    decorate("custom.read", raises=[gone])(function)
    definition = getattr(
        channel.protocol,
        {
            "method": "methods",
            "event": "notifications",
            "subscription": "subscriptions",
        }[kind],
    )[0]
    assert definition.raises == (gone.error_type,)


def test_ambiguous_channel_bindings_are_rejected_at_creation() -> None:
    with pytest.raises(
        ProtocolDefinitionError, match=r"RPC channel tasks: TaskNotFound"
    ):
        RpcChannel(
            "tasks", raises=[task_not_found, RpcErrorBinding(TaskNotFound, code="gone")]
        )


async def test_child_channel_overrides_defaults_without_changing_parent() -> None:
    archived = RpcErrorBinding(ArchivedTaskNotFound, code="archived_task_missing")
    child_binding = RpcErrorBinding(TaskNotFound, code="child_task_missing")
    local_binding = RpcErrorBinding(TaskNotFound, code="local_task_missing")
    root = RpcChannel("tasks", raises=[task_not_found, archived, HTTPTimeoutError])
    child = root.child("admin", raises=[child_binding])

    @root.server.method()
    async def read() -> None:
        raise TaskNotFound(1)

    @child.server.method()
    async def read_child() -> None:
        raise TaskNotFound(1)

    @child.server.method(raises=[local_binding])
    async def read_local() -> None:
        raise TaskNotFound(1)

    @child.server.method(raises=[local_binding])
    async def read_archived() -> None:
        raise ArchivedTaskNotFound(1)

    assert root.raises == (
        task_not_found.error_type,
        archived.error_type,
        HTTPTimeoutError,
    )
    assert child.raises == (
        archived.error_type,
        HTTPTimeoutError,
        child_binding.error_type,
    )
    service = RpcService()
    endpoint = service.socket("/rpc", channels=[root])
    server = endpoint.create_server()
    for method, code in [
        ("tasks.read", "task_not_found"),
        ("tasks.admin.read_child", "child_task_missing"),
        ("tasks.admin.read_local", "local_task_missing"),
        ("tasks.admin.read_archived", "archived_task_missing"),
    ]:
        response = await server.handle({"jsonrpc": "2.0", "id": 1, "method": method})
        assert response.error.data.code == code


def test_conflicting_child_bindings_are_rejected_before_registration() -> None:
    root = RpcChannel("tasks", raises=[task_not_found])
    with pytest.raises(
        ProtocolDefinitionError, match=r"RPC channel tasks.admin: TaskNotFound"
    ):
        root.child(
            "admin", raises=[task_not_found, RpcErrorBinding(TaskNotFound, code="gone")]
        )
    assert root.children == ()


@pytest.mark.parametrize("return_annotation", ["UnknownDetailsModel", "list["])
def test_details_annotation_failures_are_definition_errors(return_annotation) -> None:
    def details(error: TaskNotFound) -> TaskRef:
        return TaskRef(task_id=error.task_id)

    details.__annotations__["return"] = return_annotation
    with pytest.raises(
        ProtocolDefinitionError,
        match=r"RPC error binding TaskNotFound: cannot resolve details annotations",
    ) as caught:
        RpcErrorBinding(TaskNotFound, details=details)
    assert isinstance(caught.value.__cause__, (NameError, SyntaxError))


def test_details_fields_are_checked_at_definition() -> None:
    with pytest.raises(ProtocolDefinitionError, match=r"task_id.*TaskLocked"):
        RpcErrorBinding(TaskLocked, details=TaskRef)


def test_binding_check_validates_runtime_attributes_and_message() -> None:
    task_not_found.check(TaskNotFound(1))
    task_locked.check(TaskLocked("ada"))
    error = TaskNotFound(1)
    error.task_id = "invalid"
    with pytest.raises(ValidationError):
        task_not_found.check(error)
    with pytest.raises(TypeError, match="Expected TaskNotFound"):
        task_not_found.check(TaskLocked("ada"))
    broken = RpcErrorBinding(TaskNotFound, message=lambda error: None)
    with pytest.raises(TypeError, match="message must return a string"):
        broken.check(TaskNotFound(1))


def test_generated_binding_error_does_not_collide_with_domain_exception() -> None:
    class TaskNotFoundError(Exception):
        pass

    binding = RpcErrorBinding(TaskNotFoundError)
    assert binding.error_type.__name__ == "TaskNotFoundRpcError"
    assert binding.error_type.__name__ != TaskNotFoundError.__name__


async def test_strict_errors_accept_subclasses_of_declared_rpc_errors() -> None:
    class MissingError(RpcError):
        pass

    class MissingChildError(MissingError):
        pass

    channel = RpcChannel("tasks")

    @channel.server.method(raises=[MissingError])
    async def read() -> None:
        raise MissingChildError()

    response = await channel.create_server(strict_errors=True).handle(
        {"jsonrpc": "2.0", "id": 1, "method": "tasks.read"}
    )
    assert response.error.data.code == MissingChildError.code


@pytest.mark.parametrize("notification", [False, True])
async def test_failing_error_mapper_is_logged_and_contained(
    caplog, notification
) -> None:
    channel = RpcChannel("tasks")

    @channel.server.method()
    async def read() -> None:
        raise ValueError("private failure")

    def mapper(error: Exception) -> RpcError | None:
        raise RuntimeError("mapper failure")

    request = {"jsonrpc": "2.0", "method": "tasks.read"}
    if not notification:
        request["id"] = 1
    result = await channel.create_server(error_mapper=mapper).handle(request)
    if notification:
        assert result is None
    else:
        assert result.error.data.code == "internal_error"
        assert result.error.message == "Internal error"
    assert "RPC error mapper failed" in caplog.text


async def test_channels_can_bind_permission_error_differently() -> None:
    service = RpcService()
    channels = []
    for name, code in [("tasks", "task_denied"), ("users", "user_denied")]:
        channel = RpcChannel(name, raises=[RpcErrorBinding(PermissionError, code=code)])

        @channel.server.method()
        async def read() -> None:
            raise PermissionError("private")

        channels.append(channel)
    endpoint = service.socket("/rpc", channels=channels)
    service.freeze()
    server = endpoint.create_server()
    for name, code in [("tasks", "task_denied"), ("users", "user_denied")]:
        response = await server.handle(
            {"jsonrpc": "2.0", "id": 1, "method": f"{name}.read"}
        )
        assert response.error.data.code == code


def test_details_support_annotated_attributes_properties_and_defaults() -> None:
    class Details(RpcModel):
        task_id: int
        kind: str = "task"

    class Missing(Exception):
        task_id: int

        def __init__(self, value: int) -> None:
            self.task_id = value

    binding = RpcErrorBinding(Missing, details=Details)
    binding.check(Missing(1))
    assert binding.to_error(Missing(1)).details == Details(task_id=1)

    class WithProperty(Exception):
        @property
        def task_id(self) -> int:
            return 1

    RpcErrorBinding(WithProperty, details=TaskRef).check(WithProperty())


@pytest.mark.parametrize(
    "arguments",
    [
        {"exception": ProjectNotFoundError},
        {"exception": TaskNotFound, "details": lambda error: TaskRef(task_id=1)},
        {"exception": TaskNotFound, "details": dict},
        {"exception": TaskNotFound, "message": 404},
        {"exception": TaskNotFound, "rejection": "not_found"},
        {"exception": TaskNotFound, "code": "Task-Not-Found"},
        {"exception": TaskNotFound, "rpc_code": -32601},
    ],
)
def test_invalid_contracts_are_rejected(arguments) -> None:
    exception = arguments.pop("exception")
    with pytest.raises(ProtocolDefinitionError):
        RpcErrorBinding(exception, **arguments)


def test_client_methods_declare_rpc_errors_only() -> None:
    with pytest.raises(ProtocolDefinitionError, match="Client methods"):
        RpcChannel("room").client.method("ping", raises=[task_not_found])


def test_legacy_errors_mapping_is_removed() -> None:
    with pytest.raises(TypeError, match="errors"):
        RpcService(errors={TaskNotFound: HTTPTimeoutError})
    with pytest.raises(TypeError, match="errors"):
        RpcChannel("tasks").create_server(errors={TaskNotFound: HTTPTimeoutError})
    assert not hasattr(RpcService(), "errors")

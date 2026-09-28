import logging

import pytest
from pydantic import BaseModel, ValidationError

from rpckit import (
    ProtocolDefinitionError,
    RpcChannel,
    RpcError,
    RpcErrorCode,
    RpcErrorContract,
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


task_not_found = RpcErrorContract(
    TaskNotFound, details=TaskRef, rejection=RpcRejection.NOT_FOUND
)
task_locked = RpcErrorContract(
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
    assert task_not_found.error.message == "Task not found"
    assert task_not_found.error.rpc_code == RpcErrorCode.SERVER_ERROR
    assert task_not_found.error.details_type is TaskRef
    assert task_locked.error.details_type is LockDetails
    assert task_locked.error.__name__ == "TaskLockedError"


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


async def test_strict_errors_need_the_contract_on_the_method() -> None:
    lenient = await _call(_task_service(), "tasks.lock", 2)
    strict = await _call(_task_service(strict_errors=True), "tasks.lock", 2)

    assert lenient.error.data.code == "task_locked"
    assert strict.error.code == RpcErrorCode.INTERNAL_ERROR


async def test_a_failing_contract_becomes_an_internal_error(caplog) -> None:
    broken = RpcErrorContract(TaskLocked, details=TaskRef)
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


def test_an_exception_binds_to_one_contract_per_service() -> None:
    tasks = RpcChannel("tasks")

    @tasks.server.method(raises=[task_not_found])
    async def read() -> None: ...

    @tasks.server.method(raises=[RpcErrorContract(TaskNotFound, code="gone")])
    async def delete() -> None: ...

    service = RpcService()
    service.socket("/tasks", channels=(tasks,))
    with pytest.raises(ProtocolDefinitionError, match="bound to RPC error contracts"):
        service.freeze()


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
        RpcErrorContract(exception, **arguments)


def test_client_methods_declare_rpc_errors_only() -> None:
    with pytest.raises(ProtocolDefinitionError, match="Client methods"):
        RpcChannel("room").client.method("ping", raises=[task_not_found])


def test_errors_mapping_is_deprecated() -> None:
    with pytest.warns(DeprecationWarning, match="RpcErrorContract"):
        RpcService(errors={TaskNotFound: HTTPTimeoutError})

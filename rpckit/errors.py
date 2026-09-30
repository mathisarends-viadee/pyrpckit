import inspect
import re
from collections.abc import Callable, Iterable, Mapping
from contextlib import suppress
from enum import IntEnum
from typing import Any, ClassVar, get_type_hints

from pydantic import BaseModel, ValidationError

from rpckit.connection import RpcReject, RpcRejection, RpcRejectionMapper
from rpckit.models import RpcModel


def _error_code(name: str) -> str:
    name = name.lstrip("_").removesuffix("RpcError").removesuffix("Error")
    return re.sub(r"(?<=[a-z0-9])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])", "_", name).lower()


class RpcErrorCode(IntEnum):
    PARSE_ERROR = -32700
    INVALID_REQUEST = -32600
    METHOD_NOT_FOUND = -32601
    INVALID_PARAMS = -32602
    INTERNAL_ERROR = -32603
    SERVER_ERROR = -32000


class ProtocolDefinitionError(Exception):
    """A handler, notification, or feature is not a valid protocol definition."""


class RpcValidationIssue(RpcModel):
    loc: list[str | int]
    message: str
    type: str


class RpcMethodNotFound(RpcModel):
    method: str


class RpcInvalidParams(RpcModel):
    issues: list[RpcValidationIssue]


class RpcError(Exception):
    code: ClassVar[str]
    message: ClassVar[str]
    details: Any
    rpc_code: ClassVar[int] = RpcErrorCode.SERVER_ERROR
    details_type: ClassVar[type[BaseModel] | None] = None
    _builtin: ClassVar[bool] = False

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        code = cls.__dict__.get("code", _error_code(cls.__name__))
        if not isinstance(code, str) or re.fullmatch(r"[a-z][a-z0-9_]*", code) is None:
            raise ProtocolDefinitionError(f"Invalid RPC error code: {code!r}")
        rpc_code = cls.__dict__.get("rpc_code", RpcErrorCode.SERVER_ERROR)
        if not isinstance(rpc_code, int):
            raise ProtocolDefinitionError("RPC error rpc_code must be an integer")
        standard = {-32700, -32600, -32601, -32602, -32603}
        if int(rpc_code) in standard and not cls.__dict__.get("_builtin", False):
            raise ProtocolDefinitionError(
                "Reserved JSON-RPC codes are only for built-in errors"
            )
        if -32768 <= int(rpc_code) < -32099 and not cls.__dict__.get("_builtin", False):
            raise ProtocolDefinitionError(f"RPC error code {rpc_code} is reserved")
        details_type = cls.__dict__.get("details_type", None)
        annotation = inspect.get_annotations(cls, eval_str=True).get("details")
        if annotation is not None:
            with suppress(NameError, TypeError):
                annotation = get_type_hints(cls).get("details", annotation)
            if not isinstance(annotation, type) or not issubclass(
                annotation, BaseModel
            ):
                raise ProtocolDefinitionError(
                    "RPC error details must be a Pydantic model"
                )
            details_type = annotation
        elif "details_type" not in cls.__dict__:
            details_type = getattr(cls, "details_type", None)
        cls.code = code
        cls.message = cls.__dict__.get("message", code.replace("_", " ").capitalize())
        cls.rpc_code = int(rpc_code)
        cls.details_type = details_type

    def __init__(
        self,
        details: BaseModel | None = None,
        /,
        *,
        message: str | None = None,
        **fields: Any,
    ) -> None:
        if type(self) is RpcError:
            raise TypeError("RpcError cannot be instantiated directly")
        details_type = type(self).details_type
        if details_type is None:
            if details is not None or fields:
                suggestion = (
                    "; did you mean to pass message=...?"
                    if isinstance(details, str)
                    else ""
                )
                raise TypeError(
                    f"{type(self).__name__} does not accept details{suggestion}"
                )
            self.details = None
        else:
            if details is not None and fields:
                raise TypeError("Pass either a details instance or fields, not both")
            if details is None and not fields:
                raise TypeError(f"{type(self).__name__} requires details")
            if details is not None and not isinstance(details, details_type):
                raise TypeError(f"details must be {details_type.__name__}")
            self.details = details if details is not None else details_type(**fields)
        self.message = message or type(self).message
        super().__init__(self.message)


class RpcParseError(RpcError):
    _builtin = True
    code = "parse_error"
    rpc_code = RpcErrorCode.PARSE_ERROR


class RpcInvalidRequestError(RpcError):
    _builtin = True
    code = "invalid_request"
    rpc_code = RpcErrorCode.INVALID_REQUEST


class RpcMethodNotFoundError(RpcError):
    _builtin = True
    code = "method_not_found"
    rpc_code = RpcErrorCode.METHOD_NOT_FOUND
    details: RpcMethodNotFound

    def __init__(self, name: str) -> None:
        super().__init__(method=name)
        self.name = name


class RpcInvalidParamsError(RpcError):
    _builtin = True
    code = "invalid_params"
    rpc_code = RpcErrorCode.INVALID_PARAMS
    details: RpcInvalidParams

    @classmethod
    def from_validation_error(cls, error: ValidationError) -> "RpcInvalidParamsError":
        issues = [
            RpcValidationIssue(
                loc=list(issue["loc"]), message=issue["msg"], type=issue["type"]
            )
            for issue in error.errors(include_url=False)
        ]
        first = issues[0]
        location = ".".join(str(part) for part in ("params", *first.loc))
        return cls(
            issues=issues, message=f"Invalid params at {location}: {first.message}"
        )

    def __init__(self, error: ValidationError | None = None, **fields: Any) -> None:
        if error is not None:
            converted = type(self).from_validation_error(error)
            self.details = converted.details
            self.message = converted.message
            Exception.__init__(self, self.message)
            return
        fields.setdefault("issues", [])
        super().__init__(**fields)


class RpcInternalError(RpcError):
    _builtin = True
    code = "internal_error"
    rpc_code = RpcErrorCode.INTERNAL_ERROR


def error_message(code: str | int) -> str:
    if isinstance(code, str):
        return code.replace("_", " ").capitalize()
    try:
        return RpcErrorCode(code).name.replace("_", " ").capitalize()
    except ValueError:
        return f"Error {int(code)}"


def declared_error(error: Any) -> type[RpcError]:
    if isinstance(error, RpcErrorBinding):
        return error.error_type
    if not (isinstance(error, type) and issubclass(error, RpcError)):
        raise ProtocolDefinitionError(
            f"Declared RPC error must be an RpcError subclass: {error!r}"
        )
    if error is RpcError or getattr(error, "_builtin", False):
        raise ProtocolDefinitionError(
            "Built-in RPC errors are implicit and cannot be declared"
        )
    return error


class RpcErrorBinding[ExceptionT: Exception]:
    """Bind a domain exception to the RPC error and rejection it becomes."""

    __slots__ = ("_details", "_message", "error_type", "exception", "rejection")

    def __init__(
        self,
        exception: type[ExceptionT],
        /,
        *,
        code: str | None = None,
        message: str | Callable[[ExceptionT], str] | None = None,
        details: type[BaseModel] | Callable[[ExceptionT], BaseModel] | None = None,
        rpc_code: int = RpcErrorCode.SERVER_ERROR,
        rejection: RpcRejection | None = None,
    ) -> None:
        if (
            not isinstance(exception, type)
            or not issubclass(exception, Exception)
            or issubclass(exception, RpcError)
        ):
            raise ProtocolDefinitionError(
                "RpcErrorBinding binds an Exception subclass that is not an "
                f"RpcError, got {exception!r}"
            )
        if (
            message is not None
            and not isinstance(message, str)
            and not callable(message)
        ):
            raise ProtocolDefinitionError(
                "RPC error binding message must be a string or callable"
            )
        if rejection is not None and not isinstance(rejection, RpcRejection):
            raise ProtocolDefinitionError(
                "RPC error binding rejection must be an RpcRejection"
            )
        code = _error_code(exception.__name__) if code is None else code
        name = "".join(part.capitalize() for part in str(code).split("_")) + "RpcError"
        details_type = _details_type(details, exception)
        if isinstance(details, type) and details_type is not None:
            try:
                _check_details_fields(exception, details_type)
            except (NameError, TypeError, ValueError) as error:
                raise ProtocolDefinitionError(
                    f"RPC error binding {exception.__name__}: cannot inspect "
                    f"details fields of {details_type.__name__}: {error}"
                ) from error
        namespace: dict[str, Any] = {
            "__module__": exception.__module__,
            "__qualname__": name,
            "__rpckit_contract__": self,
            "code": code,
            "rpc_code": rpc_code,
            "details_type": details_type,
        }
        if isinstance(message, str):
            namespace["message"] = message
        self.exception: type[ExceptionT] = exception
        self.rejection: RpcRejection | None = rejection
        self.error_type: type[RpcError] = type(name, (RpcError,), namespace)
        self._message = message
        self._details = details

    @property
    def code(self) -> str:
        return self.error_type.code

    def __repr__(self) -> str:
        return f"RpcErrorBinding({self.exception.__name__}, code={self.code!r})"

    def message_for(self, error: ExceptionT) -> str:
        if callable(self._message):
            message = self._message(error)
            if not isinstance(message, str):
                raise TypeError("RPC error binding message must return a string")
            return message
        return self.error_type.message

    def to_error(self, error: ExceptionT) -> RpcError:
        """Build the RPC error that answers a call failing with ``error``."""
        if not isinstance(error, self.exception):
            raise TypeError(
                f"Expected {self.exception.__name__}, got {type(error).__name__}"
            )
        details = self._details
        if isinstance(details, type):
            data = details.model_validate(
                error, from_attributes=True, by_name=True, by_alias=False
            )
        else:
            data = None if details is None else details(error)
        return self.error_type(data, message=self.message_for(error))

    def check(self, error: ExceptionT) -> None:
        """Validate the details and message of a representative exception."""
        self.to_error(error)

    def reject(self, error: ExceptionT) -> RpcReject | None:
        """Build the rejection for a connection failing with ``error``."""
        if self.rejection is None:
            return None
        return RpcReject(self.rejection, self.message_for(error))


type RpcErrorDeclaration = type[RpcError] | RpcErrorBinding[Any]


def _check_details_fields(exception: type[Exception], details: type[BaseModel]) -> None:
    attributes = set().union(
        *(getattr(cls, "__annotations__", {}) for cls in exception.__mro__)
    )
    attributes.update(name for cls in exception.__mro__ for name in vars(cls))
    with suppress(TypeError, ValueError):
        attributes.update(
            name
            for name, parameter in inspect.signature(
                exception.__init__
            ).parameters.items()
            if name != "self"
            and parameter.kind
            not in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD)
        )
    missing = [
        name
        for name, field in details.model_fields.items()
        if field.is_required() and name not in attributes
    ]
    if missing:
        raise ProtocolDefinitionError(
            f"RPC error binding details {details.__name__} fields {missing!r} "
            f"are absent from {exception.__name__} annotations and init parameters; "
            "use an annotated details function for custom mappings"
        )


def _details_type(
    details: object, exception: type[Exception]
) -> type[BaseModel] | None:
    if details is None:
        return None
    if isinstance(details, type):
        if issubclass(details, BaseModel):
            return details
    elif callable(details):
        try:
            returned = get_type_hints(details).get("return")
        except (NameError, TypeError, ValueError, SyntaxError) as error:
            raise ProtocolDefinitionError(
                f"RPC error binding {exception.__name__}: cannot resolve "
                f"details annotations: {error}"
            ) from error
        if isinstance(returned, type) and issubclass(returned, BaseModel):
            return returned
    raise ProtocolDefinitionError(
        f"RPC error binding {exception.__name__} details must be a Pydantic model "
        "or a function "
        f"annotated to return one, got {details!r}"
    )


def contract_of(error: type[RpcError]) -> RpcErrorBinding[Any] | None:
    return error.__dict__.get("__rpckit_contract__")


def bind_contracts(
    contracts: Iterable[RpcErrorBinding[Any]],
    *,
    owner: str = "RPC error bindings",
) -> dict[type[Exception], RpcErrorBinding[Any]]:
    """Index bindings within one operation or rejection level."""
    bound: dict[type[Exception], RpcErrorBinding[Any]] = {}
    for contract in contracts:
        previous = bound.setdefault(contract.exception, contract)
        if previous is not contract:
            raise ProtocolDefinitionError(
                f"{owner}: {contract.exception.__name__} is bound to RPC error "
                "bindings "
                f"{previous.code!r} and {contract.code!r}"
            )
    return bound


def error_declarations(
    raises: Iterable[RpcErrorDeclaration], *, owner: str
) -> tuple[type[RpcError], ...]:
    try:
        errors = tuple(dict.fromkeys(declared_error(error) for error in raises))
    except ProtocolDefinitionError as error:
        raise ProtocolDefinitionError(f"{owner}: {error}") from error
    bind_contracts(
        (binding for error in errors if (binding := contract_of(error)) is not None),
        owner=owner,
    )
    return errors


def merge_error_declarations(
    inherited: tuple[type[RpcError], ...],
    raises: Iterable[RpcErrorDeclaration],
    *,
    owner: str,
) -> tuple[type[RpcError], ...]:
    local = error_declarations(raises, owner=owner)
    overridden = {
        binding.exception
        for error in local
        if (binding := contract_of(error)) is not None
    }
    return tuple(
        dict.fromkeys(
            (
                *(
                    error
                    for error in inherited
                    if (binding := contract_of(error)) is None
                    or binding.exception not in overridden
                ),
                *local,
            )
        )
    )


def contract_for(
    error: Exception, contracts: Mapping[type[Exception], RpcErrorBinding[Any]]
) -> RpcErrorBinding[Any] | None:
    """Find the contract bound to the most specific class of ``error``."""
    return next(
        (contracts[cls] for cls in type(error).__mro__ if cls in contracts), None
    )


def rejecting_contracts(
    rejects: Iterable[object], owner: str
) -> tuple[RpcErrorBinding[Any], ...]:
    """Validate the ``rejects=`` of a connection: bindings with a rejection."""
    contracts: list[RpcErrorBinding[Any]] = []
    for contract in dict.fromkeys(rejects):
        if not isinstance(contract, RpcErrorBinding) or contract.rejection is None:
            raise ProtocolDefinitionError(
                f"{owner} rejects= takes RpcErrorBinding values with a rejection, "
                f"got {contract!r}"
            )
        contracts.append(contract)
    bind_contracts(contracts, owner=owner)
    return tuple(contracts)


def contract_rejections(
    contracts: Iterable[RpcErrorBinding[Any]],
) -> RpcRejectionMapper | None:
    bound = bind_contracts(contracts)
    if not bound:
        return None

    def reject(error: Exception) -> RpcReject | None:
        contract = contract_for(error, bound)
        return None if contract is None else contract.reject(error)

    return reject

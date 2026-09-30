import asyncio
import logging
from collections.abc import Awaitable, Callable
from typing import Any

from pydantic import BaseModel, ValidationError

from rpckit._adapter import adapter
from rpckit.codec import RpcCodec
from rpckit.envelopes import (
    RpcErrorData,
    RpcFailure,
    RpcNotification,
    RpcRequestEnvelope,
    RpcSuccess,
)
from rpckit.errors import RpcError, RpcInvalidParamsError
from rpckit.observer import RpcObserverLike, notify_observer
from rpckit.protocol import RpcSubscriptionDefinition
from rpckit.server import RpcServer

logger = logging.getLogger(__name__)


class RpcSubscriptionLimitError(RpcError):
    code = "subscription_limit"
    message = "Too many subscriptions"


class SubscriptionSession:
    def __init__(
        self,
        definitions: tuple[RpcSubscriptionDefinition, ...],
        resolver: Any,
        send: Callable[[str], Awaitable[None]],
        *,
        limit: int,
        server: RpcServer,
        observer: RpcObserverLike | None = None,
    ) -> None:
        self._definitions = {item.name: item for item in definitions}
        self._resolver = resolver
        self._send = send
        self._limit = limit
        self._observer = observer
        self._server = server
        self._next_id = 1
        self._tasks: dict[str, tuple[str, asyncio.Task[None]]] = {}
        self._codec = RpcCodec()

    async def handle(self, request: RpcRequestEnvelope) -> None:
        if not request.expects_response:
            return
        if isinstance(request.params, list):
            await self._respond(
                request,
                RpcFailure.from_error(
                    request.id,
                    RpcInvalidParamsError(message="Subscriptions require named params"),
                ),
            )
            return
        name = request.method
        if name.endswith(".subscribe"):
            definition = self._definitions[name[: -len(".subscribe")]]
            try:
                if len(self._tasks) >= self._limit:
                    raise RpcSubscriptionLimitError()
                params = _params(definition, request.params)
            except RpcError as error:
                await self._respond(request, RpcFailure.from_error(request.id, error))
                return
            subscription_id = str(self._next_id)
            self._next_id += 1
            await self._respond(
                request,
                RpcSuccess(id=request.id, result={"subscriptionId": subscription_id}),
            )
            task = asyncio.create_task(self._run(definition, subscription_id, params))
            self._tasks[subscription_id] = (definition.name, task)
            task.add_done_callback(lambda _: self._tasks.pop(subscription_id, None))
            return
        subscription_id = request.params.get("subscriptionId")
        if (
            not isinstance(subscription_id, str)
            or subscription_id not in self._tasks
            or self._tasks[subscription_id][0] != name[: -len(".unsubscribe")]
        ):
            await self._respond(
                request,
                RpcFailure.from_error(
                    request.id,
                    RpcInvalidParamsError(message="Unknown subscriptionId"),
                ),
            )
            return
        _, task = self._tasks.pop(subscription_id)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await self._respond(request, RpcSuccess(id=request.id, result=None))

    def owns(self, method: str) -> bool:
        return any(
            method in (f"{name}.subscribe", f"{name}.unsubscribe")
            for name in self._definitions
        )

    async def close(self) -> None:
        tasks = tuple(task for _, task in self._tasks.values())
        self._tasks.clear()
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _respond(self, request: RpcRequestEnvelope, response: Any) -> None:
        if request.expects_response:
            await self._send(self._codec.encode(response))

    async def _run(
        self,
        definition: RpcSubscriptionDefinition,
        subscription_id: str,
        params: BaseModel | None,
    ) -> None:
        generator = None
        try:
            async with definition.resolver_scope(self._resolver) as resolver:
                arguments = {
                    item.name: await resolver.resolve(item.dependency)
                    for item in definition.injected_parameters
                }
                if definition.params_parameter is not None:
                    arguments[definition.params_parameter] = params
                generator = definition.function(**arguments)
                async for payload in generator:
                    value = adapter(definition.payload).validate_python(payload)
                    message = self._codec.encode(
                        RpcNotification(
                            method=definition.name,
                            params={
                                "subscriptionId": subscription_id,
                                "payload": adapter(definition.payload).dump_python(
                                    value, mode="json", by_alias=True
                                ),
                            },
                        )
                    )
                    await self._send_notification(definition.name, message)
            await self._terminal(definition.name, subscription_id, complete=True)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            failure = self._server.failure(
                None, error, method=definition.name, declared=definition.raises
            )
            await self._terminal(
                definition.name, subscription_id, complete=False, error=failure.error
            )
        finally:
            if generator is not None:
                await generator.aclose()

    async def _terminal(
        self,
        name: str,
        subscription_id: str,
        *,
        complete: bool,
        error: RpcErrorData | None = None,
    ) -> None:
        await self._send_notification(
            name,
            self._codec.encode(
                RpcNotification(
                    method=name,
                    params={
                        "subscriptionId": subscription_id,
                        "complete": complete,
                        **(
                            {}
                            if error is None
                            else {
                                "error": error.model_dump(
                                    mode="json", exclude_none=True
                                )
                            }
                        ),
                    },
                )
            ),
        )

    async def _send_notification(self, name: str, message: str) -> None:
        await self._send(message)
        await notify_observer(
            self._observer, "notification_sent", name, len(message.encode())
        )


def _params(
    definition: RpcSubscriptionDefinition, raw: dict[str, Any]
) -> BaseModel | None:
    if definition.params is None:
        if raw:
            raise RpcInvalidParamsError(message="Subscription takes no params")
        return None
    try:
        validated = adapter(definition.params).validate_python(raw)
        if (
            definition.params_origin is not None
            and type(validated) is not definition.params_origin
        ):
            return definition.params_origin.model_validate(
                validated.model_dump(by_alias=False), by_name=True
            )
        return validated
    except ValidationError as error:
        raise RpcInvalidParamsError.from_validation_error(error) from error

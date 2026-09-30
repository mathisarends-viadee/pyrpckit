# Changelog

## 0.10.0 - Unreleased

### Migration from 0.9

- `RpcService(errors=...)`, `RpcChannel.create_server(errors=...)`, and
  `RpcService.errors` are removed. Replace the global exception mapping with
  `RpcErrorBinding` declarations in the relevant operations' `raises=`.
  `error_mapper` remains available for custom mapping logic.
- Regenerate committed Python and TypeScript clients so
  `rpckit generate --check` passes and subscription failures are decoded as
  structured RPC errors.
- Normal and shutdown closes now wait until queued messages are sent, bounded by
  `RpcLimits.send_timeout`. Remove workarounds that tracked socket sends before
  calling `connection.close()`, and use `close_when_events_complete=True` for
  sockets that should end with their event sources.
- Users of earlier 0.10 PR revisions must replace `RpcErrorContract` with
  `RpcErrorBinding`, `binding.error` with `binding.error_type`, and connection
  `raises=` with `rejects=`. These names have no compatibility aliases.
- Child-channel and operation bindings override inherited bindings for the
  same exception class. Other inherited errors are kept. Conflicting bindings
  within one declaration raise `ProtocolDefinitionError` when that channel or
  operation is registered, with its name in the diagnostic.
- `rejections=` passed to `serve()`, `create_router()`, `serve_websocket()`,
  `RpcRoutes`, or `RpcTestClient` keeps working and is consulted first. Move
  mappings repeated at several call sites to bindings in
  `RpcService(rejects=...)` or in the endpoint's `socket(rejects=...)` or
  `stream(rejects=...)`.
- Child channels that only hold a single operation can become a dotted name on
  the parent channel, for example `@channel.server.method("text.insert")`. The
  wire name stays the same.

### Added

- Close finite event sockets with
  `RpcService.socket(..., close_when_events_complete=True)`. Once every event
  source of the socket has finished, the queued notifications are sent and the
  connection closes with `NORMAL`. Sockets whose channels declare no events
  reject the option.
- Bind domain exceptions to RPC errors with `RpcErrorBinding`, so domain code
  no longer imports rpckit. A binding sets the same `code`, `message`,
  `rpc_code`, and `details` as an `RpcError` subclass, reads dynamic messages
  and details from the exception, and may name the `RpcRejection` the
  exception becomes when it ends a connection. Declare it in `raises=` of
  methods, events, subscriptions, and channels, where it appears in OpenRPC and
  generated clients like an `RpcError` subclass. Lookup is scoped to each
  operation regardless of `strict_errors`; the same domain exception can have
  different bindings on different operations. The most specific binding matches
  subclasses. Required `details=Model` field names are checked at definition,
  and `binding.check(exception)` validates a representative instance. Server
  error classes use an `RpcError` suffix to avoid domain exception collisions;
  `binding.error_type` exposes the generated class. Export `RpcErrorBinding`
  and `RpcErrorDeclaration`.
- Override a channel's binding at a child channel, method, event, or
  subscription for the same domain exception. Overrides affect only that
  operation or child channel, including its OpenRPC and generated client
  declarations. Bindings for more specific exception subclasses are retained.
- Declare connection failures next to the endpoints with bindings in
  `RpcService(rejects=...)`, `RpcService.socket(..., rejects=...)`,
  `RpcService.stream(..., rejects=...)`, and `RpcRoutes(..., rejects=...)`.
  The binding's message becomes the rejection reason. A failure is looked up
  in the call's `rejections=` first (`serve()`, `create_router()`,
  `serve_websocket()`, `RpcRoutes`, `RpcTestClient`), then the endpoint's
  `rejects=`, then the service's; the first level that maps it wins.
- Pass dotted names to `method()`, `event()`, `subscription()`, `stream()`,
  `client.method()`, and `child()`. Names are relative to the channel
  namespace and each segment is validated, so
  `@channel.server.method("text.insert")` no longer needs a child channel.
  Wire names, contracts, and generated clients match the child-channel
  equivalent.

### Removed

- The service-wide `errors=` mapping, the channel server's `errors=` parameter,
  and the `RpcService.errors` property. No deprecated fallback is retained.
- The earlier 0.10 draft's `RpcErrorBinding.error` attribute; use `error_type`.

### Fixed

- Dishka context injection through `RpcRoutes` works on the supported minimum
  Dishka 1.7 and FastAPI 0.115 versions: the injector resolves from the
  WebSocket's container instead of requiring an HTTP request.
- Conflicting error bindings are rejected during channel or operation
  registration instead of waiting for freeze. Diagnostics identify the channel,
  method, event, or subscription and both conflicting error codes.
- Unresolvable or invalid `details=` callback annotations raise
  `ProtocolDefinitionError` with the domain exception's name and the original
  cause. Error-binding diagnostics consistently use "binding" terminology.
- `strict_errors=True` accepts subclasses of declared `RpcError` types.
- Exceptions raised by an `error_mapper` are logged and contained as
  `internal_error`, including for notifications.
- Positional JSON-RPC parameter arrays are rejected with `-32602`
  (`invalid_params`) instead of `-32600` (`invalid_request`).
- Aborted subscriptions send the declared RPC error payload instead of the
  fixed text `Subscription failed`; generated clients expose typed failures.
- `RpcConnection.close()` with `NORMAL` or `SHUTDOWN` no longer drops the
  message being sent when the close arrives. Closes wait until the queued
  messages are sent, within `RpcLimits.send_timeout` in total, so an event
  source can close right after its final `yield`.
- Cancelling a served JSON-RPC connection also cancels its reader, writer,
  event sources, and pending requests instead of leaving them running.

## 0.9.0 - 2026-09-25

### Added

- Declare the connection context of an endpoint with
  `RpcService.socket(..., context=T)` and `RpcService.stream(..., context=T)`.
  `RpcEndpoint` and `RpcStreamEndpoint` are generic in that type; it stays out
  of OpenRPC and generated clients.
- Mount endpoints individually on an existing FastAPI router with
  `rpckit.fastapi.RpcRoutes`. Its `context=` function runs as a FastAPI
  dependency per connection, path parameters included, and handlers receive the
  result as `Inject[T]`. `RpcRoutes` takes its type from the function's return
  annotation, so type checkers and `mount()` reject endpoints that declare
  another context. Routes use the endpoint's declared path.
- Integrate DI libraries with `RpcRoutes` through the `FastApiResolver`
  protocol. `rpckit.dishka.Dishka` resolves from the app's root container per
  connection and lets the context function declare `FromDishka[T]` without
  `@inject`.
- Map exceptions to connection rejections with `rejections=`, either a mapping
  from exception types to `RpcRejection` or a callable returning an
  `RpcReject`, on `serve()`, `create_router()`, `serve_websocket()`,
  `RpcRoutes`, and `RpcTestClient`. Mapped failures of `before_accept`
  hooks and `RpcRoutes` context functions reject the handshake; mapped failures
  of event sources and binary streams close the accepted socket with the
  matching code. Raising `RpcReject` behaves the same without a mapping, and
  unmapped failures keep their current behavior. Export `RpcRejections` and
  `RpcRejectionMapper`.
- Add `RpcConnectionClose.TRY_AGAIN_LATER` (WebSocket code 1013), used when an
  accepted connection is closed with `RpcRejection.UNAVAILABLE`.

## 0.8.0 - 2026-09-24

### Migration from 0.7

- Rename the Python package, CLI command, and logger from `pyrpckit` to
  `rpckit`. The PyPI distribution stays `pyrpckit`. Replace `pyrpckit` imports
  and CLI calls with `rpckit`, and update logger filters. Regenerate clients because
  generator metadata, generated headers, and the `rpckit.jsonrpc` subprotocol
  now use the new name. Old `.pyrpckit` generator manifests are no longer read.
- Regenerate and commit Python and TypeScript clients with this version's
  generator. Generated `connect()` now opens every declared server; pass
  `lazy=True` in Python or `lazy: true` in TypeScript to defer connections.
- Replace `RpcPeer` with `RpcConnectedClient` and `RpcPeerClosedError` with
  `RpcClientClosedError`. Import from `rpckit` or the documented public
  integration modules.
- Review the new 30-second request and client-method timeouts. Pass `None`
  explicitly where a call is intended to wait indefinitely.
- Handler results are validated before sending, and Pydantic validation errors
  raised inside handlers now become internal errors. Fix handler return values
  and raise declared RPC errors for expected failures.

### Added

- Give observers a `RpcObserver` base class with optional connection, notification,
  stream-frame, and slow-consumer callbacks; retain structural typing through
  `RpcObserverLike`. Request contexts now include the live connection, when
  available.
- Expose Python `client.closed` and TypeScript `onDisconnect` for unexpected
  WebSocket loss. Python and TypeScript clients can retry connections with
  bounded backoff and resume active subscriptions using `reconnect=True` or
  `reconnect: true`.
- Add parameterized `@channel.server.subscription()` streams with per-connection
  subscribe/unsubscribe lifecycle, generated Python and TypeScript iterators,
  `x-rpc-subscriptions` in OpenRPC, and `RpcLimits.max_subscriptions`.
- Bound running and waiting calls per connection with
  `RpcLimits.max_pending_requests` (default 1024). Further calls are answered
  with a `pending_limit` error instead of being queued without limit.
- Allow configured extra Python client files and exports, refresh handshake
  headers through an async factory, and accept HTTP(S) WebSocket URL overrides.
- Restore `rpckit.testing.RpcTestClient` for in-memory service tests, with
  client-method handlers, notification timeouts, and binary stream helpers.
  Add `RpcTestStream` for stream endpoints.
- Add `fastapi.serve_websocket()`, pre-accept hooks for socket and stream
  endpoints, and 401/403 handshake rejections with optional response headers.
- Validate typed socket path variables before accepting the handshake with
  `RpcService.socket(..., path_model=Model)` and inject the parsed model.
- Let binary stream handlers raise `RpcStreamClose`, map stream exceptions to
  policy-violation closes, and serialize concurrent `RpcBinaryOutput.send()` calls.
- Map domain exceptions declaratively with `RpcService(errors=...)`, optionally
  enforce method `raises=` declarations with `strict_errors=True`, and warn on
  collisions between explicitly assigned numeric RPC error codes.
- Cache endpoint protocols, warn about unknown client-method response IDs,
  simplify client-method serialization, and share request-name derivation.
- Export contracts with canonical UTF-8 JSON through `RpcContract.to_json()`
  and `RpcContract.write(path)`; CLI rendering uses the same serializer.
- Export public signature types from the package root, type service match and
  contract methods, and provide typed event and stream decorator overloads.
- Check public type completeness in CI with Pyright and guard its current
  baseline against regressions.
- Use postponed annotations for the optional Dishka and FastAPI types in
  `dishka_router` and `DishkaResolver`.
- **Breaking:** Pass handlers the exact Pydantic params class they declared,
  while retaining camel-case wire validation and serialization.
- **Breaking:** Align `RpcDisconnect(close, reason)` with connection closing,
  expose unknown peer codes as `RpcConnectionClose.OTHER` plus `raw_close_code`,
  and accept `observer=` on endpoint servers. Duplicate-channel errors name
  the colliding channels.

### Fixed

- Generated Python and TypeScript requests fail with a transport error once the
  request timeout elapses while a reconnecting client cannot reach the server,
  instead of waiting indefinitely. Subscriptions keep waiting for the reconnect.
- `rpckit generate` without the `codegen` extra and `import rpckit.fastapi`
  without the `fastapi` extra report which extra to install.
- Forward `variables` through generated `with_transports()` and
  `withTransports()` when binary streams are present, and retain the first
  notification sent immediately after a subscription is accepted.
- **Breaking:** Generated Python and TypeScript `connect()` now open all
  declared servers by default. Use `lazy=True` or `lazy: true` for on-demand
  connections; the old `eager` option is removed.
- Log unexpected RPC handler exceptions with the method name and traceback, and
  expose the original exception to response observers.
- **Breaking:** Treat Pydantic validation errors raised inside handlers as
  internal server errors. Invalid request envelopes and params retain their
  JSON-RPC error codes.
- **Breaking:** Validate handler results before sending them. Invalid results
  now produce logged internal errors instead of malformed success responses.
- Generated WebSocket clients discard notifications until a listener starts and
  drop the oldest queued notification on overflow, preserving active requests.
  Python clients can select the previous close behavior with
  `notification_overflow="close"`; TypeScript uses `notificationOverflow`.
- Close RPC socket connections and notify observers when the writer fails.
- Close slow consumer connections when queueing or sending exceeds
  `RpcLimits.send_timeout` (10 seconds by default).
- Reuse Pydantic adapters for params, results, events, and JSON-RPC envelopes;
  reuse the event codec for each event source.
- Bound JSON-RPC batches with `RpcLimits.max_batch_size` (32 by default) and
  execute batch items concurrently within `max_concurrency`.
- Isolate event source failures and skip invalid event payloads so other events
  and requests continue. `@channel.server.event(on_error="close")` restores
  connection closure for an event source.

### Changed

- **Breaking:** Default RPC request and client-method timeouts to 30 seconds.
  Configure client-method defaults with `RpcLimits.client_method_timeout`; pass
  `None` explicitly to disable a timeout for a call.
- **Breaking:** Rename `RpcPeer` to `RpcConnectedClient` and
  `RpcPeerClosedError` to `RpcClientClosedError`, so the server-side handle of
  a connection uses the same client/server vocabulary as client methods. The
  module `rpckit.peer` is now `rpckit.connected_client`.

## 0.7.0 - 2026-09-19

### Migration from 0.6

- Move server declarations to `channel.server.method`, `.event`, and `.stream`,
  and rename `channel.server()` / `endpoint.server()` to `create_server()`.
- Regenerate clients for the new client-method contract and generated layout.

### Added

- Group channel declarations by the side that implements them:
  `channel.server.method`, `channel.server.event`, and `channel.server.stream`
  for the server, `channel.client.method` for the connected client.
- Add client methods, requests the server sends to a connected client.
  `channel.client.method(name, params=..., result=..., raises=...)` declares a
  typed `RpcClientMethod`. Client method names share the name space of server
  methods, events, and streams.
- Make `RpcPeer` injectable on socket endpoints. `peer.call(client_method,
  params, timeout=...)` validates params and results, raises declared errors as
  their typed exceptions, and raises `RpcClientMethodFailedError`,
  `RpcClientMethodResultError`, `RpcClientMethodTimeoutError`, or
  `RpcPeerClosedError` otherwise. Server-originated requests use `server:<n>`
  string IDs, and outgoing calls respect `RpcLimits`.
- Describe client methods under the `x-rpc-client-methods` OpenRPC extension,
  in the same shape as `methods`.
- Generate one abstract client method class per namespace in Python clients,
  such as `RoomMediaHandler`, plus `Handler` and
  `handler_dispatcher()`. `connect(handlers=...)` registers
  handlers, which run concurrently. Declared errors of client methods gain
  `create()`.

### Changed

- **Breaking:** Remove `channel.method`, `channel.event`, and `channel.stream`.
  Declare server operations with `@channel.server.method`,
  `@channel.server.event`, and `@channel.server.stream` instead.
- **Breaking:** Rename `RpcChannel.server(...)` and `RpcEndpoint.server(...)` to
  `create_server(...)`, since `channel.server` now names the server side.
- Generated Python WebSocket transports answer incoming requests. Requests
  without a registered handler get `-32601 Method not found`, where they used to
  fail the transport.
- **Breaking:** Remove the internal in-memory test client from the distributed
  `pyrpckit.testing` module. Repository integration tests keep their transport
  helpers under `tests/`.
- Raise the generated client layout version to 10.

## 0.6.0 - 2026-09-19

### Migration from 0.5

- Regenerate binary stream clients. Open Python streams with `async with` and
  use `BinarySender` / `BinaryChannel` for input or bidirectional streams.
- Move authentication checks to the hosting adapter before `serve()` and
  configure limits and error mapping on `RpcService`.

### Added

- Add client-to-server and bidirectional binary streams. The handler signature
  determines the direction: async functions inject `RpcBinaryInput` and/or
  `RpcBinaryOutput`, and async generators remain server-to-client. The client
  ends its input with the text message `{"type":"end"}`. A disconnect before the
  end aborts the handler. Incoming frames use a bounded queue, which applies
  backpressure. `RpcInputEndMessage` and the generated `BinaryInputEnd` model
  describe the end message.
- Treat stream handler parameters without `Inject[...]` as typed path
  variables. They are validated before the handshake is accepted and rejected
  as `NOT_FOUND`.
- Add `input_content_type=` to `@channel.stream()`. Write `direction` and
  `inputContentType` to `x-rpckit-binary-streams`.
- Generate `BinarySender` and `BinaryChannel` stream clients with `send()`,
  `end_input()` / `endInput()`, and `end()` in Python and TypeScript. `end()`
  gives up after 30 seconds by default (`TimeoutError` in Python,
  `RpcStreamTimeout` in TypeScript). Error closes now raise `RpcStreamFailed`, and policy violations
  (1008) raise `RpcStreamRefused`.
- Add `send_frame()`, `end_input()`, and `closed()` to `RpcTestClient`.

### Fixed

- Prevent Starlette's normal WebSocket teardown cancellation from escaping
  FastAPI handlers as `concurrent.futures.CancelledError`.
- Keep generated Python clients `ruff check` and `ruff format` clean for
  contracts without events or without methods, servers without variables, and
  single-letter model names.
- Omit the `./models` export from generated TypeScript clients whose contract
  declares no models, so they compile.
- Ignore leading underscores when deriving error codes, so private error
  classes such as `_MissingError` produce `missing`.

### Changed

- Make generated route metadata carry its request and response types. Python
  now serializes generated Pydantic parameter models centrally in the client
  runtime, while TypeScript infers request results and notification payloads
  from the selected route instead of repeating manually supplied generics.
- Rename the generated `BinaryStreamConnection` to `BinaryReceiver`.
- Open generated Python streams only with `async with`. Stream methods now
  return an async context manager; `BinaryStreamOpening` and its `open()`
  method are gone, so an opened stream can no longer be left unclosed.
- Generated stream methods only accept the stream's own path variables, such
  as a session ID. Variables that a server also declares, such as `host`, come
  from `connect()`. The per-call `url` override is gone; to redirect a stream,
  wrap the `stream_opener` or `stream_socket_factory`. In turn, `connect()`
  only accepts server variables, not the path variables of a single stream.

- Keep authentication in the hosting framework before `serve()` instead of
  coupling it to the RPC service lifecycle. `RpcConnection` remains injectable
  in handlers for connection metadata and controlled closure.
- Configure default error mapping and runtime limits on `RpcService`, with
  optional endpoint overrides, so production routes and `RpcTestClient` share
  the same serving behavior.
- Pass mounted channels through the explicit `channels=` sequence argument.
- Add nested channels with `channel.child()`. Children inherit their parent's
  namespace, declared errors, and resolver scope, and are included when the root
  channel is mounted. Channel names may default to a dotted `namespace=`.
- Explain dotted operation-name errors with the nested-channel solution.
- Derive application error codes by stripping either `Error` or `RpcError`,
  allow `RpcInvalidParamsError(message=...)` without synthetic validation
  issues, and suggest `message=` for accidental positional strings.
- Add a service-bound `observer=` for request start, request finish, and
  connection close events with structured outcome and duration contexts.
- Expose `close_code` and `close_reason` on `RpcConnection`, including close
  information received from the peer.
- Let generated Python WebSocket clients accept `headers=`, make their
  connection object directly awaitable, and eagerly open single-server clients
  by default. Multi-server clients remain lazy unless requested otherwise.
- Accept HTTP base URLs when building contracts and translate them to WebSocket
  schemes. Add `RpcContract.to_openrpc()` and render committed JSON with Unicode
  characters intact.
- Resolve FastAPI dependencies per connection with `resolver_factory=`. Add
  `dishka_router()` to read Dishka's APP container from `app.state` and reject
  accidentally supplied SESSION containers with a targeted error.

### Removed

- Remove server-side connect hooks, `ConnectionRejected`, and the
  authentication-specific `RpcRejection.UNAUTHORIZED` and `FORBIDDEN` values.

## 0.5.0 - 2026-09-18

### Added

- Add `RpcService`, socket endpoints, connection hooks, transport-independent
  serving, and an in-memory `RpcTestClient`.
- Add receive-only binary stream definitions and endpoints, represented by the
  `x-rpckit-binary-streams` OpenRPC extension.
- Add stable string error codes and typed error details alongside numeric
  JSON-RPC codes.
- Generate concrete typed errors and namespaced stream clients for Python and
  TypeScript. Bundled WebSocket clients open stream sockets automatically.
- Export generated models, namespace classes, errors, routes, transport types,
  and stream types from each client package root.
- Add request hooks to both generated clients and async disposal support to the
  TypeScript client.
- Add `create_router()` and `FastApiSocket` as the FastAPI integration.
- Accept typed `context` values in `RpcChannel.server()` for lightweight tests,
  scripts, and custom transports without a bespoke resolver.
- Add an optional `[contract]` source and output to codegen TOML configs. A
  single `pyrpckit generate --config ...` invocation now renders the OpenRPC
  contract and every client, and `--check` verifies that complete pipeline.
- Let generated TypeScript WebSocket transports and endpoint overrides accept
  `URL` objects. Endpoint `subprotocols` are optional and inherit their contract
  default; single-server clients also accept `connect({ url })`.

### Changed

- Configure FastAPI prefixes and dependencies through `include_router()` so
  `create_router()` only exposes pyrpckit runtime options.
- Rename generated `RpcRemoteError.code` from the numeric JSON-RPC code to the
  stable string application code; the numeric value is now `rpc_code` in Python
  and `rpcCode` in TypeScript.
- Include `data.code` and `data.details` in every error envelope, including
  built-in errors whose details are `null`.
- Process requests concurrently up to `RpcLimits.max_concurrency`; responses
  may complete out of order even when requests arrive on the same connection.
- Keep FastAPI WebSocket handler signatures free of captured endpoint
  parameters and preserve unexpected WebSocket state errors.
- Make `RpcChannel` a lightweight group of methods, events, and streams; the
  channel name is positional and supplies the default namespace.
- Generate contracts from mounted services, including endpoint paths,
  variables, subprotocols, protocol version, errors, and streams.
- Derive an endpoint's default server name from the final static path segment
  (for example, `/v1/gateway` becomes `gateway`).
- Rename generated `media.py` / `media.ts` to `streams.py` / `streams.ts`.
- Rename method `errors=` to `raises=` and error extensions to
  `x-rpckit-code` / `x-rpckit-details-schema`.
- Redesign generated `connect()` around one option set, shared server variables,
  per-server overrides, injectable socket factories, and lazy sockets. Pass
  `eager=True` / `eager: true` to open every declared server in parallel.
- Place binary stream operations on their declared namespace and let their URL
  templates inherit variables supplied to `connect()`.
- Let optional Python request parameters remain unset so server-side schema
  defaults still apply.
- Treat regular binary stream closure as normal async-iteration completion and
  report it as `RpcStreamClosed` only for direct reads.
- Preserve notification pump completion so subscriptions created after a
  transport ends finish or fail immediately instead of waiting forever.

### Removed

- Remove the old app/router/module composition API, channel inclusion, and
  `RpcContract.from_channels()`.
- Remove authoring-side tags and the old FastAPI `serve()` helper.
- Remove client-to-server and bidirectional binary streams.
- Replace generated `from_transport*` / `fromTransport*` constructors with
  `with_transports()` / `withTransports()`.
- Stop exporting the generated TypeScript `ConnectOptions` and
  `<Name>Transports` types; the options are declared inline on `connect()` and
  `withTransports()`.
- Remove awaiting a Python `BinaryStreamOpening` directly; use `async with` or
  call `.open()` and close the returned connection explicitly.

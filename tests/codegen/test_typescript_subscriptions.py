import shutil
import subprocess
from collections.abc import AsyncIterator
from pathlib import Path
from textwrap import dedent

import pytest

from rpckit import RpcChannel, RpcErrorBinding, RpcModel, RpcService
from rpckit.codegen import generate_typescript_client
from rpckit.codegen.typescript import TypeScriptClientOptions


class SessionParams(RpcModel):
    session_id: str


class SessionEvent(RpcModel):
    value: str


class SessionMissing(Exception):
    def __init__(self, session_id: str) -> None:
        self.session_id = session_id


def test_generated_typescript_subscription(tmp_path: Path) -> None:
    npx = shutil.which("npx.cmd") or shutil.which("npx")
    node = shutil.which("node.exe") or shutil.which("node")
    if npx is None or node is None:
        pytest.skip("Node.js and npx are required")
    default = RpcErrorBinding(SessionMissing, code="session_unavailable")
    channel = RpcChannel("session", raises=[default])

    @channel.server.subscription(
        raises=[RpcErrorBinding(SessionMissing, details=SessionParams)]
    )
    async def events(params: SessionParams) -> AsyncIterator[SessionEvent]:
        yield SessionEvent(value=params.session_id)

    service = RpcService()
    service.socket("/rpc", channels=(channel,))
    generate_typescript_client(
        service.contract(title="Session", base_url="ws://localhost").to_openrpc(),
        tmp_path / "generated",
        TypeScriptClientOptions(
            client_name="SessionClient", with_transport="websocket"
        ),
    )
    formatted = subprocess.run(
        [
            npx,
            "prettier",
            "--check",
            str(tmp_path / "generated"),
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert formatted.returncode == 0, formatted.stdout + formatted.stderr
    runner = tmp_path / "subscription.test.ts"
    runner.write_text(
        dedent(
            """
            import { SessionClient, SessionMissingError } from "./generated";

            let notify: ((value: unknown) => void) | undefined;
            let unsubscribed = false;
            const transport = {
              async request(method: string, params?: object): Promise<unknown> {
                if (method === "session.events.subscribe") {
                  if (JSON.stringify(params) !== '{"sessionId":"abc"}')
                    throw new Error("wrong params");
                  setTimeout(() => notify?.({
                    method: "session.events",
                    params: { subscriptionId: "1", payload: { value: "abc" } },
                  }), 0);
                  return { subscriptionId: "1" };
                }
                if (method === "session.events.unsubscribe") {
                  unsubscribed = true;
                  return null;
                }
                throw new Error(method);
              },
              async *notifications(): AsyncIterable<unknown> {
                while (true) yield await new Promise<unknown>((resolve) => {
                  notify = resolve;
                });
              },
              async close(): Promise<void> {},
            };

            async function main(): Promise<void> {
              const client = SessionClient.withTransports(transport);
              for await (const event of client.session.events({ sessionId: "abc" })) {
                if (event.value !== "abc") throw new Error("wrong payload");
                break;
              }
              if (!unsubscribed) throw new Error("unsubscribe was not sent");
              await client.close();

              const failingTransport = {
                async request(method: string): Promise<unknown> {
                  return method.endsWith(".subscribe") ? { subscriptionId: "1" } : null;
                },
                async *notifications(): AsyncIterable<unknown> {
                  await new Promise((resolve) => setTimeout(resolve, 0));
                  yield {
                    method: "session.events",
                    params: {
                      subscriptionId: "1", complete: false,
                      error: {
                        code: -32000, message: "Session missing",
                        data: {
                          code: "session_missing", details: { sessionId: "abc" },
                        },
                      },
                    },
                  };
                },
                async close(): Promise<void> {},
              };
              const failingClient = SessionClient.withTransports(failingTransport);
              let caught = false;
              try {
                const stream = failingClient.session.events({ sessionId: "abc" });
                for await (const event of stream) {
                  throw new Error(`unexpected event: ${event.value}`);
                }
              } catch (error) {
                if (!(error instanceof SessionMissingError)) throw error;
                if (error.details.sessionId !== "abc") {
                  throw new Error("wrong error details");
                }
                caught = true;
              } finally {
                await failingClient.close();
              }
              if (!caught) throw new Error("missing subscription failure");

              type Listener = (...args: any[]) => void;
              class FakeSocket {
                readyState = 1;
                readonly #listeners = new Map<string, Listener[]>();
                constructor(readonly generation: number) {}

                addEventListener(
                  type: "open" | "close" | "error", listener: () => void,
                ): void;
                addEventListener(
                  type: "message",
                  listener: (event: { readonly data: unknown }) => void,
                ): void;
                addEventListener(type: string, listener: Listener): void {
                  const listeners = this.#listeners.get(type) ?? [];
                  listeners.push(listener);
                  this.#listeners.set(type, listeners);
                }

                send(raw: string): void {
                  const request = JSON.parse(raw) as { id: number; method: string };
                  this.#emit("message", { data: JSON.stringify({
                    jsonrpc: "2.0", id: request.id,
                    result: request.method.endsWith(".subscribe")
                      ? { subscriptionId: "1" } : null,
                  }) });
                  if (request.method.endsWith(".subscribe")) {
                    this.#emit("message", { data: JSON.stringify({
                      jsonrpc: "2.0", method: "session.events",
                      params: {
                        subscriptionId: "1",
                        payload: { value: String(this.generation) },
                      },
                    }) });
                  }
                }

                close(): void { this.readyState = 3; }
                disconnect(): void {
                  this.readyState = 3;
                  this.#emit("close", {});
                }
                #emit(type: string, event: object): void {
                  for (const listener of this.#listeners.get(type) ?? []) {
                    listener(event);
                  }
                }
              }

              const sockets: FakeSocket[] = [];
              const reconnecting = await SessionClient.connect({
                reconnect: true,
                reconnectInitialDelayMs: 1,
                socketFactory: () => {
                  const socket = new FakeSocket(sockets.length + 1);
                  sockets.push(socket);
                  return socket;
                },
              });
              const events = reconnecting.session.events({ sessionId: "abc" })[
                Symbol.asyncIterator
              ]();
              if ((await events.next()).value?.value !== "1") {
                throw new Error("first event missing");
              }
              sockets[0].disconnect();
              if ((await events.next()).value?.value !== "2") {
                throw new Error("reconnected event missing");
              }
              await events.return?.();
              await reconnecting.close();
              if (sockets.length !== 2) {
                throw new Error("subscription was not reconnected");
              }
            }
            void main();
            """
        ),
        encoding="utf-8",
        newline="\n",
    )
    compiled = tmp_path / "compiled"
    checked = subprocess.run(
        [
            npx,
            "--yes",
            "--package",
            "typescript",
            "tsc",
            "--strict",
            "--target",
            "ES2022",
            "--module",
            "Node16",
            "--moduleResolution",
            "Node16",
            "--lib",
            "ES2022,DOM,ESNext.Disposable",
            "--skipLibCheck",
            "--outDir",
            str(compiled),
            str(runner),
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert checked.returncode == 0, checked.stdout + checked.stderr
    behavior = subprocess.run(
        [node, str(compiled / "subscription.test.js")],
        capture_output=True,
        text=True,
        check=False,
    )
    assert behavior.returncode == 0, behavior.stdout + behavior.stderr

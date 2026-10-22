"""TCP forwarder for the browser pod (#830).

The frontend's config.json points the SPA at http://localhost:8081 (snowplow),
:8082 (authn), :8083 (events) — run 10's frontend-config.json, made for an
operator who port-forwards. The browser pod shares one network namespace with
this sidecar, so listening on those localhost ports and piping to the cluster
Services gives Chrome exactly the operator's view, with the platform's own
config untouched.

    python3 forward.py 8080=frontend.krateo-system.svc:8080 8081=snowplow...:8081 ...

`--probe SPEC...` (the sidecar's startupProbe, run 11's race): exit 0 only
when every local port accepts a connection — the browser container starts
after it (a native sidecar), so Chrome never meets a refused localhost port.
The targets are not probed here: a startupProbe that never passes holds the
pod in Init with no bound but the run's deadline; the browser wrapper's
bounded wait (an HTTP answer THROUGH each forward) covers them and fails the
run with its reason.
"""
import asyncio
import socket
import sys


async def pipe(reader, writer):
    try:
        while True:
            data = await reader.read(65536)
            if not data:
                break
            writer.write(data)
            await writer.drain()
    except (ConnectionError, OSError):
        pass
    finally:
        try:
            writer.close()
        except Exception:  # noqa: BLE001
            pass


def handler(host, port):
    async def handle(r1, w1):
        try:
            r2, w2 = await asyncio.open_connection(host, port)
        except OSError:
            w1.close()
            return
        await asyncio.gather(pipe(r1, w2), pipe(r2, w1))
    return handle


async def main(specs):
    servers = []
    for spec in specs:
        local, target = spec.split("=", 1)
        host, port = target.rsplit(":", 1)
        servers.append(await asyncio.start_server(handler(host, int(port)), "127.0.0.1", int(local)))
        print(f"forward 127.0.0.1:{local} -> {host}:{port}", flush=True)
    await asyncio.gather(*(s.serve_forever() for s in servers))


def probe(specs, timeout=2.0) -> int:
    for spec in specs:
        local = int(spec.split("=", 1)[0])
        try:
            socket.create_connection(("127.0.0.1", local), timeout).close()
        except OSError as e:
            print(f"probe: 127.0.0.1:{local} not accepting: {e}", file=sys.stderr)
            return 1
    return 0


if __name__ == "__main__":
    if sys.argv[1:2] == ["--probe"]:
        sys.exit(probe(sys.argv[2:]))
    asyncio.run(main(sys.argv[1:]))

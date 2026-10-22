"""TCP forwarder for the browser pod (#830).

The frontend's config.json points the SPA at http://localhost:8081 (snowplow),
:8082 (authn), :8083 (events) — run 10's frontend-config.json, made for an
operator who port-forwards. The browser pod shares one network namespace with
this sidecar, so listening on those localhost ports and piping to the cluster
Services gives Chrome exactly the operator's view, with the platform's own
config untouched.

    python3 forward.py 8080=frontend.krateo-system.svc:8080 8081=snowplow...:8081 ...
"""
import asyncio
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


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1:]))

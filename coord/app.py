"""Runnable entry point: python3 -m coord.app [--port 8080] [--db data/coord.db]

Pairs the coordinator with a *callback-receiving* downstream adapter skeleton.
For real deployments replace ``MockDownstream`` with an HTTP/RPC adapter that
implements the :class:`~coord.coordinator.DownstreamGateway` contract.
"""

from __future__ import annotations

import argparse
import os

from .coordinator import Coordinator
from .mock_downstream import MockDownstream
from .server import build_server
from .storage import Storage


def main() -> None:
    parser = argparse.ArgumentParser(description="Persistent coordination service")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--db", default=os.path.join("data", "coord.db"))
    parser.add_argument("--down-db", default=os.path.join("data", "downstream.db"))
    parser.add_argument("--timeout", type=float, default=2.0)
    parser.add_argument("--max-attempts", type=int, default=3)
    args = parser.parse_args()

    os.makedirs(os.path.dirname(os.path.abspath(args.db)), exist_ok=True)
    storage = Storage(args.db)

    # The mock's delivery sink feeds callbacks back over HTTP in a real
    # deployment; in-process it simply has no sink until one is wired.
    downstream = MockDownstream(args.down_db, delivery=lambda env: None)
    coord = Coordinator(storage, downstream,
                        timeout_seconds=args.timeout,
                        max_attempts=args.max_attempts)
    server = build_server(coord, host=args.host, port=args.port)
    print(f"coordination service listening on http://{args.host}:{args.port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.worker.stop()
        server.shutdown()


if __name__ == "__main__":
    main()

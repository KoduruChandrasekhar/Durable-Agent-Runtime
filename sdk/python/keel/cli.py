"""keel-py: Python-side tooling.

    keel-py lint [paths...]            determinism lint for @workflow code
    keel-py events <run_id>            print a run's event log (REST API)
    keel-py worker module:attr ...     run a worker for the given workflow modules
"""

from __future__ import annotations

import asyncio
import importlib
import json
import sys


def main() -> None:
    args = sys.argv[1:]
    if not args:
        print(__doc__)
        sys.exit(2)
    cmd, rest = args[0], args[1:]
    if cmd == "lint":
        from .lint import main as lint_main
        sys.exit(lint_main(rest))
    if cmd == "events":
        from .client import KeelClient

        async def show() -> None:
            async with KeelClient() as c:
                for e in await c.events(rest[0]):
                    p = json.dumps(e["payload"])
                    print(f"{e['seq']:4d} e{e['epoch']:<3d} {e['type']:22s} {e.get('step_id') or '':28s} {p[:140]}")
        asyncio.run(show())
        return
    if cmd == "worker":
        for mod in rest:
            importlib.import_module(mod)
        from .worker import run_worker
        run_worker()
        return
    print(__doc__)
    sys.exit(2)


if __name__ == "__main__":
    main()

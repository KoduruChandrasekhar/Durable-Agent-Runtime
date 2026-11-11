"""Sandbox escape tests: every attempt must fail or be killed.

    python sandbox/tests/escape_test.py [--url http://localhost:8095] [--json out.json]
Also measures cold vs warm start latency.
"""

from __future__ import annotations

import argparse
import json
import statistics
import sys
import time

import httpx

ATTEMPTS = {
    "network_access": ("import urllib.request\n"
                       "urllib.request.urlopen('http://1.1.1.1', timeout=3)\nprint('ESCAPED')",
                       lambda r: "ESCAPED" not in r["stdout"] and r["exit_code"] != 0),
    "dns_lookup": ("import socket\nprint(socket.gethostbyname('example.com'))\nprint('ESCAPED')",
                   lambda r: "ESCAPED" not in r["stdout"]),
    "fork_bomb": ("import os\nn=0\ntry:\n    while True:\n        if os.fork()==0:\n            import time; time.sleep(30)\n        n+=1\nexcept OSError as e:\n    print('blocked after', n)\n",
                  lambda r: r["timed_out"] or "blocked after" in r["stdout"] or r["exit_code"] != 0),
    "write_outside_tmp": ("open('/etc/keel_pwned','w').write('x')\nprint('ESCAPED')",
                          lambda r: "ESCAPED" not in r["stdout"] and r["exit_code"] != 0),
    "write_root_home": ("import os\nopen('/root/x','w').write('x')\nprint('ESCAPED')",
                        lambda r: "ESCAPED" not in r["stdout"]),
    "read_env_secrets": ("import os\nleak=[k for k in os.environ if any(s in k for s in ('KEY','SECRET','TOKEN','PASSWORD','DATABASE'))]\n"
                         "print('LEAK' if leak else 'clean', leak)",
                         lambda r: "LEAK" not in r["stdout"]),
    "fill_disk": ("f=open('/tmp/big','wb')\nimport sys\ntry:\n    for _ in range(1024):\n        f.write(b'x'*1024*1024)\n    f.flush()\n    print('ESCAPED wrote 1GB')\nexcept OSError as e:\n    print('blocked', e.errno)",
                  lambda r: "ESCAPED" not in r["stdout"]),
    "exceed_memory": ("x=[]\nfor _ in range(4096):\n    x.append(bytearray(1024*1024))\nprint('ESCAPED allocated 4GB')",
                      lambda r: "ESCAPED" not in r["stdout"] and (r["oom_killed"] or r["exit_code"] != 0)),
    "become_root": ("import os\nos.setuid(0)\nprint('ESCAPED')",
                    lambda r: "ESCAPED" not in r["stdout"]),
    "docker_socket": ("import os\nprint('ESCAPED' if os.path.exists('/var/run/docker.sock') else 'no socket')",
                      lambda r: "ESCAPED" not in r["stdout"]),
    "infinite_loop": ("while True: pass", lambda r: r["timed_out"]),
    "huge_output": ("print('A'*10_000_000)", lambda r: r["truncated"] and len(r["stdout"]) < 100_000),
}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default="http://localhost:8095")
    ap.add_argument("--json", default="")
    ap.add_argument("--latency-samples", type=int, default=8)
    a = ap.parse_args()
    c = httpx.Client(base_url=a.url, timeout=120)
    results, ok = {}, True
    for name, (code, check) in ATTEMPTS.items():
        r = c.post("/execute", json={"code": code, "timeout": 8 if name in ("infinite_loop", "fork_bomb") else 20}).json()
        passed = bool(check(r))
        ok &= passed
        results[name] = {"blocked": passed, "exit_code": r.get("exit_code"), "timed_out": r.get("timed_out"),
                         "oom_killed": r.get("oom_killed"), "stdout": r.get("stdout", "")[:120],
                         "stderr_tail": r.get("stderr", "")[-160:]}
        print(f"{'BLOCKED' if passed else 'ESCAPED':8s} {name:20s} exit={r.get('exit_code')} timeout={r.get('timed_out')} "
              f"oom={r.get('oom_killed')} out={r.get('stdout','')[:60]!r}")
    lat: dict[str, list[float]] = {"warm": [], "cold": []}
    for mode in ("cold", "warm"):
        for _ in range(a.latency_samples):
            t = time.perf_counter()
            r = c.post("/execute", json={"code": "print(1)", "warm": mode == "warm"}).json()
            lat[mode].append((time.perf_counter() - t) * 1000)
            time.sleep(0.3 if mode == "warm" else 0)  # let the pool refill
    summary = {m: {"p50_ms": statistics.median(v), "max_ms": max(v)} for m, v in lat.items()}
    print("start latency:", json.dumps(summary))
    if a.json:
        json.dump({"escape_tests": results, "all_blocked": ok, "latency": summary, "samples": lat}, open(a.json, "w"), indent=2)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())

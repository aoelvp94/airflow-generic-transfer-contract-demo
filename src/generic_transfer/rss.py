"""Peak RSS of this process, read from the OS.

Linux reports ``VmHWM`` (high-water mark of resident set size) from
``/proc/self/status``. macOS, and Linux without ``/proc``, fall back to
``resource.ru_maxrss``. The returned label says which source was used.
``ru_maxrss`` is bytes on macOS and kilobytes on Linux; the Linux fallback
multiplies by 1024.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any


def read_peak_rss() -> tuple[int, str]:
    """Return ``(peak_bytes, source)`` for this process.

    ``source`` is ``VmHWM`` or ``ru_maxrss``.
    """
    status = Path("/proc/self/status")
    if status.exists():
        for line in status.read_text().splitlines():
            if line.startswith("VmHWM:"):
                kb = int(line.split()[1])
                return kb * 1024, "VmHWM"
    import resource

    raw = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    if sys.platform == "darwin":
        return int(raw), "ru_maxrss"
    return int(raw) * 1024, "ru_maxrss"


def run_in_child(module: str, func: str, payload: dict[str, Any]) -> dict[str, Any]:
    """Call ``module.func(payload)`` in a fresh interpreter and return its dict.

    The child prints one JSON object on stdout. Peak RSS measured inside the
    child starts from a new process, so ``VmHWM`` is that run and not the
    parent's high-water mark.
    """
    code = (
        "import json, sys\n"
        "from importlib import import_module\n"
        "mod = import_module(sys.argv[1])\n"
        "fn = getattr(mod, sys.argv[2])\n"
        "spec = json.load(sys.stdin)\n"
        "json.dump(fn(spec), sys.stdout)\n"
    )
    env = os.environ.copy()
    root = Path(__file__).resolve().parents[2]
    src = str(root / "src")
    prev = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = src + os.pathsep + str(root) + (os.pathsep + prev if prev else "")
    proc = subprocess.run(
        [sys.executable, "-c", code, module, func],
        input=json.dumps(payload),
        text=True,
        capture_output=True,
        env=env,
        cwd=str(root),
    )
    if proc.returncode != 0:
        err = (proc.stderr or proc.stdout or "").strip()
        raise RuntimeError(f"{module}.{func} failed ({proc.returncode}): {err[-4000:]}")
    try:
        return json.loads(proc.stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"{module}.{func} did not return JSON: {proc.stdout[-2000:]}") from exc

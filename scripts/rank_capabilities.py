#!/usr/bin/env python3
"""Rank file/record-transfer capabilities from PAIR_AUDIT.csv.

Reports, for concrete `needs_capability` operators (primary classification):

- sole-blocker counts: operators unlocked by exactly one capability
- total-any counts: operators that list each capability at all
- greedy set-cover order: at each step, the capability that unlocks the most
  *remaining* operators (an operator is unlocked once every one of its
  capabilities is in the selected set)
- how many operators need >= 4 capabilities

Stdlib only. No pandas.
"""
from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import Counter
from pathlib import Path


def _flags(cell: str) -> list[str]:
    return [f for f in (cell or "").split("|") if f]


# Capability names as used in the audit CSV `flags` column.
# We re-filter to this set so openlineage / special flags are ignored.
DEFAULT_CAPABILITIES = frozenset(
    {
        "wildcard_or_prefix",
        "replace",
        "compression",
        "dest_encryption",
        "acl_or_extra_args",
        "server_side_copy",
        "schema_sidecar",
        "move_semantics",
        "incremental_filter",
        "path_layout",
        "pagination",
        "bulk_load",
        "deferrable",
        "xcom_side_channel",
        "object_retention",
        "upsert",
    }
)


def load_needers(csv_path: Path, capabilities: frozenset[str]) -> list[dict]:
    rows = []
    with csv_path.open(encoding="utf-8", newline="") as fh:
        for raw in csv.DictReader(fh):
            if raw.get("is_base", "").lower() in {"true", "1"}:
                continue
            if raw.get("classification") != "needs_capability":
                continue
            caps = [f for f in _flags(raw.get("flags", "")) if f in capabilities]
            rows.append(
                {
                    "class_name": raw["class_name"],
                    "kind": raw.get("kind", ""),
                    "capabilities": caps,
                }
            )
    return rows


def sole_blockers(needers: list[dict]) -> list[tuple[str, int, list[str]]]:
    counts: Counter[str] = Counter()
    by_cap: dict[str, list[str]] = {}
    for row in needers:
        if len(row["capabilities"]) == 1:
            cap = row["capabilities"][0]
            counts[cap] += 1
            by_cap.setdefault(cap, []).append(row["class_name"])
    return [(cap, counts[cap], by_cap[cap]) for cap, _ in counts.most_common()]


def total_any(needers: list[dict]) -> list[tuple[str, int]]:
    counts: Counter[str] = Counter()
    for row in needers:
        counts.update(row["capabilities"])
    return counts.most_common()


def greedy_set_cover(needers: list[dict]) -> list[dict]:
    remaining = [set(row["capabilities"]) for row in needers if row["capabilities"]]
    selected: list[str] = []
    steps: list[dict] = []
    while remaining:
        # Count how many remaining operators would be unlocked by adding cap.
        universe = sorted({c for rec in remaining for c in rec} - set(selected))
        if not universe:
            break
        best = None
        best_score = (-1, -1)
        for cap in universe:
            unlocked = sum(1 for rec in remaining if rec <= (set(selected) | {cap}))
            appears = sum(1 for rec in remaining if cap in rec)
            score = (unlocked, appears)
            if score > best_score or (score == best_score and (best is None or cap < best)):
                best, best_score = cap, score
        if best is None:
            break
        selected.append(best)
        remaining = [rec for rec in remaining if not rec <= set(selected)]
        steps.append(
            {
                "step": len(selected),
                "capability": best,
                "newly_unlocked": best_score[0],
                "also_present_in_remaining_before": best_score[1],
                "cumulative_unlocked": sum(1 for row in needers if set(row["capabilities"]) <= set(selected)),
                "remaining": len(remaining),
            }
        )
    return steps


def render_md(needers: list[dict], *, csv_path: Path, heavy_threshold: int = 4) -> str:
    lines: list[str] = []
    a = lines.append
    sole = sole_blockers(needers)
    any_counts = total_any(needers)
    steps = greedy_set_cover(needers)
    n_heavy = sum(1 for r in needers if len(r["capabilities"]) >= heavy_threshold)
    a("# Capability ranking (primary `needs_capability` operators)")
    a("")
    a(f"Source: `{csv_path}`. Stdlib only. {len(needers)} concrete operators in the")
    a("`needs_capability` bucket. An operator is *unlocked* when every capability it")
    a("lists is in the selected set.")
    a("")
    a(f"**{n_heavy}** of these operators list **≥ {heavy_threshold}** capabilities.")
    a("")
    a("## Sole-blocker")
    a("")
    a("Operators that need exactly one capability (adding that one would unlock them).")
    a("")
    if sole:
        a("| Capability | Operators unlocked | Examples |")
        a("|---|---:|---|")
        for cap, n, names in sole:
            examples = ", ".join(f"`{x}`" for x in names[:5])
            if len(names) > 5:
                examples += f", … (+{len(names) - 5})"
            a(f"| `{cap}` | {n} | {examples} |")
        a("")
        a(f"Total sole-blocker operators: **{sum(n for _, n, _ in sole)}**.")
        a("")
    else:
        a("No operator has exactly one capability.")
        a("")
    a("## Total-any")
    a("")
    a("How many `needs_capability` operators list each capability at all.")
    a("")
    if any_counts:
        a("| Capability | Operators |")
        a("|---|---:|")
        for cap, n in any_counts:
            a(f"| `{cap}` | {n} |")
        a("")
    a("## Greedy set-cover order")
    a("")
    a("At each step pick the unused capability that unlocks the most remaining operators.")
    a("Ties break alphabetically.")
    a("")
    if steps:
        a("| Step | Capability | Newly unlocked | Cumulative unlocked | Remaining |")
        a("|---:|---|---:|---:|---:|")
        for s in steps:
            a(
                f"| {s['step']} | `{s['capability']}` | {s['newly_unlocked']} | "
                f"{s['cumulative_unlocked']} | {s['remaining']} |"
            )
        a("")
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    repo = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--csv", type=Path, default=repo / "bench-out" / "PAIR_AUDIT.csv")
    parser.add_argument("--md", type=Path, default=repo / "bench-out" / "CAPABILITY_RANK.md")
    parser.add_argument("--json", type=Path, default=repo / "bench-out" / "CAPABILITY_RANK.json")
    args = parser.parse_args(argv)
    if not args.csv.is_file():
        print(f"CSV not found: {args.csv}. Run `just audit` first.", file=sys.stderr)
        return 2
    needers = load_needers(args.csv, DEFAULT_CAPABILITIES)
    sole = sole_blockers(needers)
    any_counts = total_any(needers)
    steps = greedy_set_cover(needers)
    n_heavy = sum(1 for r in needers if len(r["capabilities"]) >= 4)
    payload = {
        "source_csv": str(args.csv),
        "needs_capability_operators": len(needers),
        "need_gte_4_capabilities": n_heavy,
        "sole_blocker": [{"capability": c, "count": n, "operators": names} for c, n, names in sole],
        "total_any": [{"capability": c, "count": n} for c, n in any_counts],
        "greedy_set_cover": steps,
    }
    args.json.parent.mkdir(parents=True, exist_ok=True)
    args.json.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    md = render_md(needers, csv_path=args.csv)
    args.md.write_text(md, encoding="utf-8")
    print(f"needs_capability operators: {len(needers)}")
    print(f"need >=4 capabilities: {n_heavy}")
    if sole:
        print("sole-blocker:")
        for c, n, _ in sole:
            print(f"  {c}: {n}")
    if steps:
        print("greedy order:")
        for s in steps:
            print(f"  {s['step']}. {s['capability']} (+{s['newly_unlocked']}, remaining {s['remaining']})")
    print(f"wrote {args.md}")
    print(f"wrote {args.json}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

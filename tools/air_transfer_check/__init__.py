"""Prototype checker for per-pair transfer operators.

A production rule would live in Ruff's Rust ``AIR`` implementation.
``AIR331`` is not an assigned Ruff code.
"""

from tools.air_transfer_check.checker import Finding, check_source, fix_source

__all__ = ["Finding", "check_source", "fix_source"]

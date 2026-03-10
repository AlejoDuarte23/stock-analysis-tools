#!/usr/bin/env python3
"""Run project-level correlation series ingestion script from this skill."""

from __future__ import annotations

import runpy
from pathlib import Path


def main() -> None:
    script_path = Path(__file__).resolve().parents[3] / "fetch_covariables_to_sqlite.py"
    runpy.run_path(str(script_path), run_name="__main__")


if __name__ == "__main__":
    main()

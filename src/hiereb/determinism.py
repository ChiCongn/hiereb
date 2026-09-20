"""Artifact-level repeatability verification."""

from __future__ import annotations

import hashlib
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

from hiereb.config import AppConfig
from hiereb.experiment import compare_modes


def verify_determinism(config: AppConfig) -> dict[str, Any]:
    """Run the same config twice and compare artifact checksums excluding metadata."""
    with TemporaryDirectory(prefix="hiereb-determinism-") as temporary:
        root = Path(temporary)
        first_config = _with_output(config, root / "first")
        second_config = _with_output(config, root / "second")
        compare_modes(first_config)
        compare_modes(second_config)
        first = _checksums(first_config.experiment.output_dir)
        second = _checksums(second_config.experiment.output_dir)
        keys = sorted(set(first) | set(second))
        mismatches = [key for key in keys if first.get(key) != second.get(key)]
        return {
            "deterministic": not mismatches,
            "artifact_count": len(keys),
            "excluded": ["run_metadata.json", "resolved_config.yaml"],
            "mismatches": mismatches,
        }


def _with_output(config: AppConfig, output: Path) -> AppConfig:
    experiment = config.experiment.model_copy(update={"output_dir": output, "overwrite": True})
    return config.model_copy(update={"experiment": experiment})


def _checksums(root: Path) -> dict[str, str]:
    checksums: dict[str, str] = {}
    for path in sorted(item for item in root.rglob("*") if item.is_file()):
        relative = path.relative_to(root).as_posix()
        if path.name in {"run_metadata.json", "resolved_config.yaml"}:
            continue
        digest = hashlib.sha256()
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
        checksums[relative] = digest.hexdigest()
    return checksums

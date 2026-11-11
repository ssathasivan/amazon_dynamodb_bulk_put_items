"""Build the Lambda code folder at synth time, without Docker.

The Lambda Python runtime ships its own boto3, which can be older than the first release with
StartBulkPutItems (1.43.124). So the asset bundles boto3 from app/requirements.txt.

boto3 and its dependencies are pure Python, so `pip install --target` works on any OS. The
--platform / --only-binary flags make pip refuse anything that would need a native build.
"""
from __future__ import annotations

import hashlib
import shutil
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
APP = REPO / "app"
BUILD = REPO / "infra" / "build" / "lambda"


def build_lambda_asset(python_version: str = "3.13") -> str:
    requirements = APP / "requirements.txt"
    stamp = BUILD / ".requirements.sha256"
    digest = hashlib.sha256(requirements.read_bytes() + python_version.encode()).hexdigest()

    if not stamp.exists() or stamp.read_text() != digest:
        shutil.rmtree(BUILD, ignore_errors=True)
        BUILD.mkdir(parents=True)
        subprocess.run(
            [
                sys.executable, "-m", "pip", "install",
                "-r", str(requirements),
                "--target", str(BUILD),
                "--platform", "manylinux2014_x86_64",
                "--implementation", "cp",
                "--python-version", python_version,
                "--only-binary=:all:",
                "--no-compile",
                "--quiet",
            ],
            check=True,
        )
        stamp.write_text(digest)

    # Always refresh our own package so code edits are picked up.
    shutil.rmtree(BUILD / "bulkput", ignore_errors=True)
    shutil.copytree(APP / "bulkput", BUILD / "bulkput", ignore=shutil.ignore_patterns("__pycache__", "*.pyc"))
    return str(BUILD)

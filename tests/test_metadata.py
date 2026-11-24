"""The package's metadata attributes, which it reads from its installed distribution, match pyproject.toml."""

from __future__ import annotations

import tomllib
from pathlib import Path

import async_anywhere


def test_metadata_attributes_match_pyproject_toml():
    # The distribution's metadata is built from pyproject.toml, so a stale editable install (not re-synced) fails here
    with (Path(__file__).parents[1] / "pyproject.toml").open("rb") as file:
        project = tomllib.load(file)["project"]

    assert async_anywhere.__version__ == project["version"]
    assert async_anywhere.__author__ == project["authors"][0]["name"]
    assert async_anywhere.__url__ == project["urls"]["Homepage"]

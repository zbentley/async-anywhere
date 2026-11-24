from __future__ import annotations

# Read documentation for this module at https://async-anywhere.readthedocs.io


def _metadata_fields():
    from importlib.metadata import metadata

    # The installed distribution's metadata, which pyproject.toml is the source of
    data = metadata("async_anywhere")
    yield data["Version"]
    yield data["Author"]
    urls = dict(url.split(", ", 1) for url in data.get_all("Project-URL", ()))
    yield urls["Homepage"]


__version__, __author__, __url__ = _metadata_fields()
del _metadata_fields

__all__ = []

"""fsql"""
from __future__ import annotations

import os
from typing import Any, NoReturn

import fsspec
from fsspec.spec import AbstractFileSystem

__all__: list[str] = []


def get_url_and_fs(url: str) -> tuple[str, AbstractFileSystem]:
    """This function standardizes url->protocol derivation, and allows for fs-specific parameter passing.

    In particular, we need it to allow minio-in-place-of-s3 functionality, which is not supported on its
    own using environment variables in vanilla fsspec.
    """

    # TODO extend signature to make it more meaningufl, that is:
    # - capture the os dependency somehow, eg by having a dict param and passing os.environ to it
    # - add the user-provided FS here, to validate protocolar compatibility
    # TODO org improvements -- better location for this, and change name and return type to better capture that
    # the resource location and file system are inherently bound

    fs_key, url_suff = url.split(":/", 1)

    env2dict = lambda mapping: {val: os.environ[key] for key, val in mapping.items() if key in os.environ}  # noqa: E731

    if fs_key == "s3":
        # boto itself supports only access key and secret key via env
        # TODO this is quite dumb, especially the max pool connections -- a more intelligent way of config is desired
        # the right way forward is probably some FsFactory singleton which reads:
        # - env variables in case where it makes sense (AWS ids)
        # - config files with rich options that allow generic passthrough
        # - defaults as in the case of max_pool_connections
        # note the programmatic override can already be done by passing Fs instance to api methods
        configurable_keys_l1 = {
            "AWS_ACCESS_KEY_ID": "key",
            "AWS_SECRET_ACCESS_KEY": "secret",
            "AWS_SESSION_TOKEN": "token",
        }
        l1kwargs: dict[str, Any] = env2dict(configurable_keys_l1)  # mypy
        configurable_keys_l2 = {
            "AWS_ENDPOINT_URL": "endpoint_url",
            "AWS_REGION_NAME": "region_name",
        }
        l2kwargs = env2dict(configurable_keys_l2)
        # in case of a lot of small files and local tests, we tend to exhaust the conn pool quickly and spawning warns
        l1kwargs["config_kwargs"] = {"max_pool_connections": 25}
        l1kwargs["client_kwargs"] = l2kwargs

        fs = fsspec.filesystem("s3", **l1kwargs)
    else:
        fs = fsspec.filesystem(fs_key)

    return url_suff, fs


def assert_exhaustive_enum(x: NoReturn) -> NoReturn:
    """Python does not sux! Call this function at the end of if-else matching of Enum, to have mypy
    ensure for you that all values are considered! Cf https://tech.preferred.jp/en/blog/python-exhaustive-union-match/
    """
    raise AssertionError(f"enum matching not exhaustive: {x}")

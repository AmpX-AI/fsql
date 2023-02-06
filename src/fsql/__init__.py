"""Place for common fsql utils"""
# TODO eventually move the fs + config to a standalone module
from __future__ import annotations

import os
from collections import UserDict
from typing import Any, NoReturn, Optional

import fsspec
from fsspec.spec import AbstractFileSystem

__all__: list[str] = []


class FsqlConfig(UserDict):
    pass


fsql_config = FsqlConfig()


def set_default_config(protocol: str, config: dict[str, Any]):
    """Sets config values to be provided to every subsequent default `fs` instance creation.
    Setting values is *NOT* thread safe, but reading is."""
    fsql_config[protocol] = config


def _get_default_config(protocol: str) -> dict[str, Any]:
    """Reads environment variables and merges with default config. Default config has precedence.

    In particular, we need it to allow minio-in-place-of-s3 functionality, which is not supported on its
    own using environment variables in vanilla fsspec."""
    env2dict = lambda mapping: {val: os.environ[key] for key, val in mapping.items() if key in os.environ}  # noqa: E731
    if protocol == "s3":
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
        config: dict[str, Any] = env2dict(configurable_keys_l1)  # mypy
        configurable_keys_l2 = {
            "AWS_ENDPOINT_URL": "endpoint_url",
            "AWS_REGION_NAME": "region_name",
        }
        l2kwargs = env2dict(configurable_keys_l2)
        # in case of a lot of small files and local tests, we tend to exhaust the conn pool quickly and spawning warns
        config["config_kwargs"] = {"max_pool_connections": 25}
        config["client_kwargs"] = l2kwargs
    else:
        config = {}
    return {**config, **fsql_config.get(protocol, {})}


def get_fs(protocol: str, config: Optional[dict[str, Any]] = None) -> AbstractFileSystem:
    """Creates `fs` instance with config from `config` arg, values provided by `set_default_config` and env variables;
    in this order."""
    config_nonnull = {} if not config else config
    config_merged = {**_get_default_config(protocol), **config_nonnull}
    return fsspec.filesystem(protocol, **config_merged)


def get_url_and_fs(url: str) -> tuple[str, AbstractFileSystem]:
    """This function standardizes `url -> (fs, base_path)` split. The `fs` instance can be configured via env vars or
    the `set_default_config` endpoint."""
    protocol, url_suff = url.split(":/", 1)
    return url_suff, get_fs(protocol)


def assert_exhaustive_enum(x: NoReturn) -> NoReturn:
    """Python does not sux! Call this function at the end of if-else matching of Enum, to have mypy
    ensure for you that all values are considered! Cf https://tech.preferred.jp/en/blog/python-exhaustive-union-match/
    """
    raise AssertionError(f"enum matching not exhaustive: {x}")

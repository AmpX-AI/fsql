"""Internal dataclass module."""

from __future__ import annotations

from copy import copy
from dataclasses import dataclass
from typing import Optional


@dataclass
class Partition:
    url: str
    columns: dict[str, str]

    def expand_by(self, url_suffix: str, key_val: Optional[tuple[str, str]]) -> Partition:
        columns_ext = copy(self.columns)
        if key_val:
            if key_val[0] in columns_ext:
                raise ValueError(f"duplicate key inserted: {key_val[0]}")
            columns_ext[key_val[0]] = key_val[1]
        return Partition(self.url + url_suffix, columns_ext)

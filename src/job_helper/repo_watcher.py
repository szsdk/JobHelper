from __future__ import annotations

import base64
import subprocess
import zlib
from pathlib import Path
from typing import Union

from pydantic import BaseModel

from .config import jhcfg


class RepoState(BaseModel):
    direcotry: Path
    commit: str
    diff: str

    @staticmethod
    def from_folder(folder: Union[str, Path]) -> RepoState:
        dir = Path(folder).resolve()
        git_diff = subprocess.check_output(f"cd {dir} && git diff HEAD", shell=True)
        compressed_diff = zlib.compress(git_diff)
        return RepoState(
            direcotry=dir,
            commit=(
                subprocess.check_output(f"cd {dir} && git rev-parse HEAD", shell=True)
                .decode()
                .strip()
            ),
            diff=base64.b64encode(compressed_diff).decode(),
        )

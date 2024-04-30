import os
import pydoc
import sys
from typing import Any

from ._tools import init, log_cmd
from .config import jhcfg
from .project_helper import Project


def console_main():
    import fire

    cmds: dict[str, Any] = {"project": Project, "init": init}

    sys.path.append(os.getcwd())
    cmds.update(
        {cmd: pydoc.locate(arg_class) for cmd, arg_class in jhcfg.commands.items()}
    )
    sys.path.pop(-1)
    if jhcfg.cli.logging_cmd:
        log_cmd()
    fire.Fire(cmds)

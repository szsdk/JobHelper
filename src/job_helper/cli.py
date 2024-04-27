import os
import pydoc
import sys

from ._tools import log_cmd
from .config import jhcfg


def console_main():
    import fire

    sys.path.append(os.getcwd())
    if jhcfg.cli.logging_cmd:
        log_cmd()
    fire.Fire(
        {cmd: pydoc.locate(arg_class) for cmd, arg_class in jhcfg.commands.items()}
    )

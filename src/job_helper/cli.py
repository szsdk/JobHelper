import copy
import logging
import os
import pydoc
import shutil
import sys
from importlib.resources import as_file, files
from pathlib import Path
from typing import Any

from . import init_example
from ._tools import Tools, log_cmd
from ._utils import dumps_toml
from .config import JobHelperConfig, jhcfg
from .project_helper import Project


def init():
    """
    Initialize the project directory.
    """
    cfg: JobHelperConfig = copy.copy(jhcfg)
    cfg.commands = {"add_one": "cli.AddOne", "tools": "job_helper.cli.tools"}
    cwd = Path().resolve()
    cfg.repo_watcher.watched_repos = [cwd]
    example_data = files(init_example)
    for f in ["cli.py", "project.yaml"]:
        with as_file(example_data / f) as p:
            shutil.copy(p, f)

    with open("jh_config.toml", "w") as f:
        print(dumps_toml(cfg), file=f)

    if not (cwd / ".git").exists():
        logging.warning(
            "This is not a git repository. It is recommended to use git for version control."
        )

    logging.info("jh_config.toml is created.")
    logging.info("""Try:
    jh --help
    jh add-one --help
    jh add-one -n 1 - run""")


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


tools = Tools()

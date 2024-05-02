import copy
import datetime
import logging
import os
import pydoc
import shlex
import shutil
import subprocess
import sys
import tarfile
from importlib.resources import as_file, files
from pathlib import Path
from typing import Any

from . import init_example
from ._utils import dumps_toml
from .config import JobHelperConfig, jhcfg
from .project_helper import Project

cmd_logger = logging.getLogger("_jb_cmd")


def compress_log(dt: float = 24) -> None:
    """
    This function compresses the '.out' and '.sh' files which are not modified more than a certain
    time, `dt` hours (default=24 hours), in the directory, `slurm.log_dir` (default='log/slurm') to a tar.gz file.
    The file name is the current date+time.
    """
    log_dir = jhcfg.slurm.log_dir
    # Get the current date+time
    now = datetime.datetime.now()
    now_str = now.strftime("%Y%m%d_%H%M%S")
    # Set the time threshold
    time_threshold = (now - datetime.timedelta(hours=dt)).timestamp()
    # Get the list of files to be compressed
    files = [f for f in log_dir.glob("*.out") if f.stat().st_mtime < time_threshold]
    files += [f for f in log_dir.glob("*.sh") if f.stat().st_mtime < time_threshold]
    # Compress the files
    if len(files) == 0:
        cmd_logger.warning("No files to compress.")
        return
    cmd_logger.info(f"Compressing {len(files)} files to {now_str}.tar.gz")
    with tarfile.open(log_dir / f"{now_str}.tar.gz", "w:gz") as tar:
        for file in files:
            tar.add(file)
            file.unlink()


def log_cmd() -> None:
    """
    log the command
    ```bash
    log_sh ls -all
    ```
    """
    cmd_logger.info(shlex.join(sys.argv), extra={"typename": "CMD"})


class Tools:
    def log_sh(self, command: str) -> None:
        """
        Run a shell command and log it if the command succeeds.
        ```
        log_sh "ls -all"
        ```
        """
        subprocess.run(command, shell=True, check=True)
        cmd_logger.info(command, extra={"typename": "SH"})

    def log_message(self, message: str, level: str = "info") -> None:
        """
        Add some logging information to `cmd.log`
        ```
        log_message "hello" warning
        ```
        """
        log_cmds = {
            "info": cmd_logger.info,
            "error": cmd_logger.error,
            "warning": cmd_logger.warning,
        }
        log_cmds[level](message, extra={"typename": "MSG"})

    def compress_log(self, dt: float = 24) -> None:
        """
        This function compresses the '.out' and '.sh' files which are not modified more than a certain
        time, `dt` hours (default=24 hours), in the directory, `slurm.log_dir` (default='log/slurm') to a tar.gz file.
        The file name is the current date+time.
        ```
        compress_log 24
        ```
        """
        compress_log(dt)


def init():
    """
    Initialize the project directory.
    """
    cfg: JobHelperConfig = copy.copy(jhcfg)
    cfg.commands = {"add_one": "cli.AddOne", "tools": "job_helper.cli.tools"}
    cwd = Path().resolve()
    cfg.repo_watcher.watched_repos = [cwd]
    example_data = files(init_example)

    if not (cwd / ".git").exists():
        print(
            "This is not a git repository. It is recommended to use git for version control.\n"
        )

    for f in ["cli.py", "project.yaml"]:
        with as_file(example_data / f) as p:
            shutil.copy(p, f)

    with open("jh_config.toml", "w") as f:
        print(dumps_toml(cfg), file=f)

    print("""Try:
    jh --help
    jh add-one --help
    jh add-one -n 1 - run""")


def console_main():
    import fire

    cmds: dict[str, Any] = {"project": Project, "init": init}

    sys.path.append(os.getcwd())
    # pre check command to avoid unnecessary import and improve performance
    if sys.argv[1] in cmds:
        pass
    elif (cmd := sys.argv[1]) in jhcfg.commands:
        cmds.update({cmd: pydoc.locate(jhcfg.commands[cmd])})
    else:
        cmds.update(
            {cmd: pydoc.locate(arg_class) for cmd, arg_class in jhcfg.commands.items()}
        )
    if jhcfg.cli.logging_cmd:
        log_cmd()
    fire.Fire(cmds)
    sys.path.pop(-1)


tools = Tools()

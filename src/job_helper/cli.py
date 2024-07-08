import copy
import datetime
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

from loguru import logger

from . import init_example
from ._utils import dumps_toml
from .config import JobHelperConfig, jhcfg
from .project_helper import Project, ProjectRunningResult, get_scheduler
from . import server


def compress_log(dt: float = 24) -> None:
    """
    This function compresses the '.out' and '.sh' files which are not modified more than a certain
    time, `dt` hours (default=24 hours), in the directory, `slurm.log_dir` (default='log/slurm') to a tar.gz file.
    The file name is the current date+time.
    """
    log_dir = get_scheduler().log_dir
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
        logger.warning("No files to compress.")
        return
    logger.info(f"Compressing {len(files)} files to {now_str}.tar.gz")
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
    logger.trace("running jh command", command=sys.argv, extra={"typename": "CMD"})


class Tools:
    def log_sh(self, command: str) -> None:
        """
        Run a shell command and log it if the command succeeds.
        ```
        log_sh "ls -all"
        ```
        """
        subprocess.run(command, shell=True, check=True)
        logger.trace("running a sh command", command=command, extra={"typename": "SH"})

    def log_message(self, message: str, level: str = "info") -> None:
        """
        Add some logging information to `cmd.log`
        ```
        log_message "hello" warning
        ```
        """
        log_cmds = {
            "info": logger.info,
            "error": logger.error,
            "warning": logger.warning,
        }
        log_cmds[level](message, extra={"typename": "MSG"})

    def compress_log(self, dt: float = 24) -> None:
        """
        This function compresses the '.out' and '.sh' files which are not modified more than a certain
        time, `dt` hours (default=24 hours), in the directory, `scheduler.config.log_dir` (default='log/slurm') to a tar.gz file.
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
    if Path("jh_config.toml").exists():
        logger.error("jh_config.toml already exists.")
        exit(1)

    cfg = JobHelperConfig(
        project={"log_dir": "log/project"},
        scheduler={"name": "slurm", "config": {"log_dir": "log/slurm"}},
        cli={"log_file": "log/cmd.log"},
        commands={"add_one": "cli.AddOne", "tools": "job_helper.cli.tools"},
    )

    if (Path() / ".git").exists():
        cfg.repo_watcher.watched_repos = [Path().resolve()]
    else:
        logger.warning(
            "This is not a git repository. It is recommended to use git for version control."
        )

    example_data = files(init_example)
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
    """
    This is the entry point of the job_helper commands.
    """
    import fire

    logger.remove()
    logger.add(sys.stdout, level="INFO")
    logger.enable("job_helper")
    if sys.argv[1] == "init":
        jhcfg.cli.log_file = Path("log/cmd.log")
        jhcfg.cli.serialize_log = True
        jhcfg.cli.logging_cmd = True

    if jhcfg.cli.logging_cmd:
        logger.add(jhcfg.cli.log_file, serialize=jhcfg.cli.serialize_log, level="TRACE")
    cmds: dict[str, Any] = {
        "project": Project,
        "init": init,
        "project-result": ProjectRunningResult,
        "server": server.run,
    }

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
    fire.Fire(cmds)
    log_cmd()
    sys.path.pop(-1)
    logger.remove()
    logger.disable("job_helper")


tools = Tools()

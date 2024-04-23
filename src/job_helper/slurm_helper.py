from __future__ import annotations

import base64
import copy
import dataclasses
import datetime
import json
import logging
import os
import shlex
import subprocess
import sys
import tarfile
import zlib
from pathlib import Path
from string import Template
from typing import Any, ClassVar, Dict, Optional, Protocol, Self, TypeVar, Union

import pydantic
import toml
import yaml
from pydantic import BaseModel, ConfigDict, validate_call
from rich.console import Console as _Console
from rich.logging import RichHandler as _RichHandler

__all__ = [
    "ArgBase",
    "JobHelperConfig",
    "Slurm",
    "compress_log",
    "log_cmd",
    "log_sh",
    "force_commit",
]


__doc__ = r"""
This module provides a simple interface for submitting jobs to a cluster (Slurm).

It is highly recommended to use this module with the [`fire` package](https://github.com/szsdk/python-fire).

## Usful commands
```bash
fd '' log/jobs -e out --changed-within=1week | fzf --preview 'tail {}'

cat log/cmd.log | fzf
```
"""

_env0 = (  # noqa: E402
    os.environ.copy()
)  # It should be before importing other modules, especially `mpi4py`.


class CmdLoggerFileFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        if hasattr(record, "typename"):
            record.label = f"{record.levelname[0]}-{record.typename}"
            self._style._fmt = "%(asctime)s %(label)-8s>> %(message)s"
        else:
            self._style._fmt = "%(asctime)s %(levelname)-8s>> %(message)s"
        return super().format(record)


class JobHelperConfig:
    """
    This is a singleton class for storing global variables.
    """

    _instance: Optional["JobHelperConfig"] = None

    def __init__(self, console_width: int = 120):
        if JobHelperConfig._instance is not None:
            raise RuntimeError("The instance is already initialized.")
        self.log_dir = Path("log")
        self.job_log_dir = Path("log/jobs")
        if not self.log_dir.exists():
            self.log_dir.mkdir()
        if not self.job_log_dir.exists():
            self.job_log_dir.mkdir()
        self.rich_console = _Console(width=console_width)
        logging.basicConfig(
            level=logging.DEBUG,
            format="%(message)s",
            handlers=[_RichHandler(console=self.rich_console)],
        )
        logging.getLogger("matplotlib").setLevel(logging.WARNING)
        logging.getLogger("h5py").setLevel(logging.WARNING)

        self.cmd_logger = logging.getLogger(f"cmd_{__file__}")
        self.cmd_logger.setLevel(logging.DEBUG)
        cmd_logger_file_handler = logging.FileHandler(self.log_dir / "cmd.log")
        cmd_logger_file_handler.setFormatter(CmdLoggerFileFormatter())
        self.cmd_logger.addHandler(cmd_logger_file_handler)
        JobHelperConfig._instance = self

    @staticmethod
    def get_instance() -> JobHelperConfig:
        if JobHelperConfig._instance is None:
            return JobHelperConfig()
        return JobHelperConfig._instance


def compress_log(dt: float = 24) -> None:
    """
    This function compresses the '.out' and '.sh' files which are not modified more than a certain
    time, `dt` (default=24 hours), in a directory, `log_dir` (default='log/slurm') to a tar.gz file.
    The file name is the current date+time.
    """
    log_dir = JobHelperConfig.get_instance().job_log_dir
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
        logging.warning("No files to compress.")
        return
    logging.info(f"Compressing {len(files)} files to {now_str}.tar.gz")
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
    JobHelperConfig.get_instance().cmd_logger.info(
        shlex.join(sys.argv), extra={"typename": "CMD"}
    )


def log_sh() -> None:
    """
    Run a shell command and log it if the command succeeds.
    ```
    log_sh ls -all
    ```
    """
    command = shlex.join(sys.argv[2:])
    subprocess.run(command, shell=True, check=True)
    JobHelperConfig.get_instance().cmd_logger.info(command, extra={"typename": "SH"})
    exit()


def log_message(message: str, level: str = "info") -> None:
    """
    Add some logging information to `cmd.log`
    ```
    log_message "hello" warning
    ```
    """
    cmd_logger = JobHelperConfig.get_instance().cmd_logger
    log_cmds = {
        "info": cmd_logger.info,
        "error": cmd_logger.error,
        "warning": cmd_logger.warning,
    }
    log_cmds[level](message, extra={"typename": "MSG"})


_T = TypeVar("_T")


class IsDataclass(Protocol):
    __dataclass_fields__: ClassVar[Dict[str, Any]]


def _multi_index(d, indices: str):
    ans = d
    if indices == "":
        return ans
    for i in indices.split("."):
        ans = ans[i]
    return ans


class ArgBase(IsDataclass):
    """
    This is a base class for the arguments. It is supposed to be used as a dataclass:
    ```python
    @dataclasses.dataclass
    class Args(ArgBase):
        ...
    ```
    """

    def to_base64(self) -> str:
        s = json.dumps(dataclasses.asdict(self))
        return base64.b64encode(s.encode()).decode()

    @classmethod
    def from_base64(cls: type[_T], s: str, substitute: bool = True) -> _T:
        s = base64.b64decode(s.encode()).decode()
        if substitute:
            s = Template(s).safe_substitute(os.environ)
        return cls(**json.loads(s))

    def __str__(self) -> str:
        return repr(self)

    def log(self) -> Self:
        logging.info(self)
        return self


def doc_from_FieldInfo(field_info: pydantic.fields.FieldInfo) -> str:
    """convert FieldInfo to docstring"""
    doc_parts = []
    if field_info.description is not None:
        doc_parts.append(f"Description: {field_info.description}")

    if field_info.annotation is not None:
        doc_parts.append(f"Type: {field_info.annotation.__name__}")

    constraints_doc = ", ".join([str(i) for i in field_info.metadata])
    if constraints_doc != "":
        doc_parts.append(f"Constraints: {constraints_doc}\n")
    return " | ".join(doc_parts)


class PDArgBase(BaseModel):
    """
    This is a base class for the arguments.
    ```python
    class Args(PDArgBase):
        ...
    ```
    """

    toml_section_name: ClassVar[Optional[str]] = None

    @classmethod
    def __pydantic_init_subclass__(cls):
        if cls.__doc__ is None:
            cls.__doc__ = ""
        param_docs = []
        for k, v in cls.model_fields.items():
            param_docs.append(f"   {k}: {doc_from_FieldInfo(v)}")
        if len(param_docs) > 0:
            cls.__doc__ = cls.__doc__ + "\n\nparameters:\n" + "\n".join(param_docs)

    def to_base64(self) -> str:
        return base64.b64encode(
            zlib.compress(self.model_dump_json().encode(), 9)
        ).decode()

    @classmethod
    def from_base64(cls, s: str, substitute: bool = True):
        s = zlib.decompress(base64.b64decode(s.encode())).decode()
        if substitute:
            s = Template(s).safe_substitute(os.environ)
        return cls.model_validate_json(s)

    def log(self) -> Self:
        logging.info(self)
        return self

    @classmethod
    @validate_call
    def from_toml(cls, path: str, toml_section_name: str = "") -> Self:
        path_split = path.split("::")
        if len(path_split) == 2:
            if toml_section_name != "":
                raise ValueError("The section name is specified twice.")
            path, sn = path_split
        elif toml_section_name == "":
            if cls.toml_section_name is None:
                raise ValueError("The section name is not specified.")
            sn = cls.toml_section_name
        else:
            if toml_section_name != cls.toml_section_name:
                logging.warning(
                    f"The tomal section name {toml_section_name} is different from the default {cls.toml_section_name}."
                )
            sn = toml_section_name
        with open(path) as fp:
            return cls.model_validate(_multi_index(toml.load(fp), sn))

    @classmethod
    @validate_call
    def from_config(cls, path: Union[str, Path]) -> Self:
        path_split = str(path).split("::")
        if len(path_split) == 2:
            path, sn = path_split
        else:
            sn = ""
        p = Path(path)
        if p.suffix == ".toml":
            with open(path) as fp:
                return cls.model_validate(_multi_index(toml.load(fp), sn))
        if p.suffix == ".yaml":
            with open(path) as fp:
                return cls.model_validate(_multi_index(yaml.safe_load(fp), sn))
        raise ValueError(f"Unsupported config file format: {p.suffix}")

    @validate_call
    def to_toml(self, path: Optional[Path] = None) -> None:
        if self.toml_section_name is None:
            raise ValueError("The section name is not specified.")
        c = {self.toml_section_name: self.model_dump(mode="json")}
        if path is None:
            print(toml.dumps(c))
        else:
            with path.open("a") as fp:
                toml.dump(c, fp)

    def setattr(self, **kargs):
        for k, v in kargs.items():
            setattr(self, k, v)
        return self

    def slurm(self) -> Slurm:
        raise NotImplementedError


class Slurm:
    """
    This class provides a simple interface for submitting jobs to a cluster (Slurm).
    """

    shell: str = "/bin/sh"
    sbatch_cmd: str = "sbatch"

    def __init__(
        self,
        run_cmd: str,
        slurm_config: dict[str, str] = {},
    ):
        # self.config = JobHelperConfig.get_instance().job_default_config.copy()
        jhcfg = JobHelperConfig.get_instance()
        self.config: dict[str, str] = {
            "output": f"{jhcfg.job_log_dir}/%j.out",
        }
        self.set_slurm(**slurm_config)
        self.run_cmd = run_cmd
        self.job_id: Optional[int] = None

    def set_slurm(self, **kwargs: str) -> Slurm:
        self.config.update({k.replace("_", "-"): v for k, v in kwargs.items()})
        return self

    def script(self) -> str:
        """
        Generate the script to be submitted to the cluster.
        """
        cmds = [f"#!{self.shell}"]
        for k, v in self.config.items():
            if v is None:
                continue
            cmds.append(f"#SBATCH --{k:<19} {v}")
        cmds.append(self.run_cmd)
        return "\n".join(cmds)

    def sbatch(self, dry: bool = True, save_script: bool = True) -> Slurm:
        """
        Submit the job to the cluster.
        If `dry` is True, it only prints the script. Otherwise (--nodry), it submits the job.
        """
        cfg = JobHelperConfig.get_instance()
        slurm_script = "\n".join([f'{self.sbatch_cmd} << "EOF"', self.script(), "EOF"])
        print(self.script())
        if dry:
            logging.info("It is a dry run.")
            return self

        result = subprocess.run(
            slurm_script, shell=True, stdout=subprocess.PIPE, env=_env0
        )
        success_msg = "Submitted batch job"
        stdout = result.stdout.decode("utf-8").strip()
        if success_msg not in stdout:
            logging.error(result.stderr)
            sys.exit(1)
        self.job_id = int(stdout.split(" ")[3])
        log_cmd()
        cfg.cmd_logger.info(stdout)
        if save_script:
            with (cfg.job_log_dir / f"{self.job_id}_slurm.sh").open("w") as fp:
                print(self.script(), file=fp)
        return self

    def __str__(self) -> str:
        return f"{type(self).__name__}(job id: {self.job_id})"


def git_status(scope: str = "folder") -> list[tuple[str, str]]:
    """
    Check the Git status of the current folder or the whole repository.
    `scope` can be either "folder" or "repository".
    """

    # Run the 'git status . -s' command
    result = subprocess.run(
        ["git", "status", "-s", "."] if scope == "folder" else ["git", "status", "-s"],
        capture_output=True,
        text=True,
    )
    ans = []
    for line in result.stdout.strip().split("\n"):
        line = line.strip()
        if len(line) > 0:
            ans.append((line[:2], line[2:].strip()))
    return ans


def git_commit_hash() -> str:
    """
    Get the commit hash of the current commit.
    """
    result = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def force_commit(scope: str = "folder") -> None:
    """
    Make sure all changes in the current folder or the whole repository are committed.
    """
    uncommitted = git_status(scope)
    if uncommitted == []:
        JobHelperConfig.get_instance().cmd_logger.info(
            f"commit: {git_commit_hash()}", extra={"typename": "GIT"}
        )
        return
    logging.warning("The following files are not committed:")
    for status, file in uncommitted:
        logging.warning(f"{status} {file}")
    if input("do you want to commit all changes? (Y/n)") in ["Y", "y", "yes", ""]:
        subprocess.run(["git", "add", "."], capture_output=True, text=True)
        # Use the current time as the commit message
        commit_message = f"Auto commit at {datetime.datetime.now()}"
        os.system(f"git commit -m '{commit_message}'")
        logging.info(f"Commit message: {commit_message}")
    raise Exception(f"Some files are not committed in the {scope}!")

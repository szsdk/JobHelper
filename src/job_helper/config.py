from __future__ import annotations

import socket
from functools import lru_cache
from pathlib import Path
from typing import Annotated, Any, ClassVar, Literal, Union

from loguru import logger as logger
from pydantic import (
    BaseModel,
    BeforeValidator,
    ConfigDict,
    DirectoryPath,
    Field,
    field_validator,
    model_validator,
)
from pydantic.networks import IPvAnyAddress

from ._utils import LogDir, LogFile, dumps_toml, init_context
from .scheduler import Scheduler


def dir_exists(v: Union[str, Path]) -> Path:
    v = Path(v)
    v.mkdir(parents=True, exist_ok=True)
    return v


DirExists = Annotated[DirectoryPath, BeforeValidator(dir_exists)]


def get_available_port(start_port, end_port) -> int:
    hostname = socket.gethostname()
    for port in range(start_port, end_port):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            if s.connect_ex((hostname, port)) != 0:
                return port
    raise ValueError(f"No available port in range {start_port} to {end_port}")


class ServerConfig(BaseModel):
    ip: Union[IPvAnyAddress, Literal["localhost"]] = "localhost"
    port: int = Field(default_factory=lambda: get_available_port(8000, 9000))


class CLIConfig(BaseModel):
    model_config = ConfigDict(validate_assignment=True)
    logging_cmd: Annotated[bool, Field(description="log the running command")] = True
    log_file: Annotated[
        LogFile, Field(description="log file", validate_default=True)
    ] = LogFile(path=Path("cmd.log"))
    log_rotation: Annotated[str | None, Field(description="log rotation")] = None
    log_compression: Annotated[str | None, Field(description="compression method")] = (
        None
    )
    serialize_log: Annotated[
        bool,
        Field(description="serialize log, set to false to get a human-readable log"),
    ] = True


class RepoWatcherConfig(BaseModel):
    watched_repos: list[DirectoryPath] = Field(default_factory=list)
    force_commit_repos: list[DirectoryPath] = Field(default_factory=list)

    @field_validator("watched_repos", "force_commit_repos", mode="after")
    def _validate_repos(cls, v: list[DirectoryPath]) -> list[DirectoryPath]:
        ans = []
        for p in v:
            if not p.is_dir():
                logger.warning(
                    f"{p} is not a directory. It is removed from repo_watcher"
                )
                continue
            if not (p / ".git").exists():
                logger.warning(
                    f"{p} is not a git repo. It is removed from repo_watcher"
                )
                continue
            ans.append(p)
        return ans

    @model_validator(mode="after")
    def no_overlap_repos_and_force_commit(self):
        if set(self.watched_repos) & set(self.force_commit_repos):
            raise ValueError("watched_repos and force_commit_repos should not overlap.")
        return self


class ProjectConfig(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True, validate_assignment=True)
    log_dir: LogDir = Field(
        default=LogDir(path=Path("log/project")),
        validate_default=True,
        description="Log directory. Defaults to the current directory. Providing an absolute path centralizes all log files into a single folder.",
    )

    def get_log_dir(self) -> Path:
        return self.log_dir.resolved_path


class SchedulerConfig(BaseModel):
    name: Annotated[
        str,
        Field(
            description="The type of scheduler. Or the python import path to a custom scheduler class"
        ),
    ] = "slurm"
    config: dict[str, Any] = Field(
        default_factory=lambda: dict(
            shell="/bin/sh", sbatch_cmd="sbatch", sacct_cmd="sacct", log_dir=Path()
        ),
        description="The configuration for the scheduler. It varies for different schedulers.",
    )


class JobHelperConfig(BaseModel):
    model_config = ConfigDict(validate_assignment=True)
    _reserved_commands: ClassVar[list[str]] = ["init", "project"]
    scheduler: SchedulerConfig = Field(
        default_factory=SchedulerConfig, description="scheduler config"
    )
    repo_watcher: RepoWatcherConfig = Field(
        default_factory=RepoWatcherConfig, description="repo watcher config"
    )
    project: ProjectConfig = Field(default_factory=ProjectConfig)
    cli: CLIConfig = Field(
        default_factory=CLIConfig,
        description="cli config,  Check https://loguru.readthedocs.io/en/stable/api/logger.html for details about log_rotation, log_compression, serialize_log",
    )
    server: ServerConfig = Field(
        default_factory=ServerConfig, description="config for web server"
    )
    source_file: Annotated[
        Path | None, Field(description="Path of the config file", exclude=True)
    ] = None

    def get_scheduler(self) -> Scheduler:
        return Scheduler.resolve_subclass(self.scheduler.name).model_validate(
            self.scheduler.config
        )

    def to_toml(self, *leading_sections: str) -> str:
        """Convert the configuration to TOML format string.

        Useful for quickly creating an inherited config file. A common usage is:
        `jh config - to-toml > jh_config.toml`

        Args:
            *leading_sections: Section names to appear first in the TOML output.

        Returns:
            A TOML-formatted string representation of the configuration.
        """
        return dumps_toml(self, list(leading_sections))


def init_jhcfg():
    if (context := init_context()) is not None:
        p, cfg = context
        logger.info(f"Load job_helper config from {p}")
        cfg["source_file"] = p
        return JobHelperConfig.model_validate(cfg)
    logger.warning(
        "jh_config.toml or JHCFG environment variable or [job_helper] in a pyproject.toml is not found. Use default settings."
    )
    return JobHelperConfig()


jhcfg = init_jhcfg()

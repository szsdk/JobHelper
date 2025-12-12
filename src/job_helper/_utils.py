import os
from functools import cached_property, lru_cache
from pathlib import Path
from typing import Annotated, Any, ClassVar, Literal, Union

import toml
from loguru import logger as logger
from pydantic import BaseModel, model_validator


class TomlDescriptionEncoder(toml.TomlEncoder):
    def dump_sections(self, o, sup):
        retstr, retdict = super().dump_sections(o, sup)
        if isinstance(o, CDict):
            retstr = f"# {o.description}\n\n{retstr}"
        return retstr, retdict

    def dump_value(self, v):
        if isinstance(v, CV):
            a = super().dump_value(v.value)
            return str(a) + f"\n# {v.description}"
        return super().dump_value(v)


class CDict(dict):
    def __init__(self, d, description):
        super().__init__(d)
        self.description = description


class CV:
    def __init__(self, value, description):
        self.value = value
        self.description = description


def add_description(arg, arg_dict):
    for k, v in type(arg).model_fields.items():
        if arg_dict.get(k, None) is None:
            continue
        sub_arg = getattr(arg, k)
        val = arg_dict.get(k, None)
        if isinstance(sub_arg, BaseModel):
            add_description(sub_arg, val)
        if (description := getattr(v, "description")) is not None:
            if isinstance(sub_arg, (dict, BaseModel)):
                arg_dict[k] = CDict(val, description)
            else:
                arg_dict[k] = CV(val, description)


def dumps_toml(arg: BaseModel, leading_sections: list) -> str:
    arg_dict = arg.model_dump(mode="json")
    add_description(arg, arg_dict)
    for section in leading_sections[::-1]:
        arg_dict = {section: arg_dict}

    return toml.dumps(arg_dict, encoder=TomlDescriptionEncoder())


@lru_cache()
def init_context() -> None | tuple[Path, Any]:
    if "JHCFG" in os.environ:
        p = Path(os.environ["JHCFG"])
        return p, toml.load(p)
    for c in [Path().resolve(), *Path().resolve().parents]:
        p = c / "pyproject.toml"
        if p.exists():
            content = toml.load(p).get("tool", {}).get("job_helper", None)
            if content is not None:
                return p, content
        p = c / "jh_config.toml"
        if p.exists():
            return p, toml.load(p)


class LogPath(BaseModel):
    unified: bool = True
    path: Path

    @model_validator(mode="before")
    def factory(cls, values):
        if isinstance(values, (Path, str)):
            values = {"path": values}
        return values

    @cached_property
    def resolved_path(self):
        if not self.unified:
            return self.path.resolve()
        context = init_context()
        if context is None:
            root_dir = Path("")
        else:
            root_dir, _ = context
            root_dir = root_dir.parent
        return (root_dir / self.path).resolve()


class LogDir(LogPath):
    @model_validator(mode="after")
    def create_log_dir(self):
        self.resolved_path.mkdir(parents=True, exist_ok=True)
        return self


class LogFile(LogPath):
    @model_validator(mode="after")
    def create_log_dir(self):
        self.resolved_path.parent.mkdir(parents=True, exist_ok=True)
        return self

import base64
import json
import os
import zlib
from pathlib import Path
from string import Template
from typing import Union

import toml
import yaml
from pydantic import BaseModel, validate_call


def _multi_index(d, indices: str):
    ans = d
    if indices == "":
        return ans
    for i in indices.split("."):
        ans = ans[i]
    return ans


def doc_from_FieldInfo(field_info) -> str:
    """convert FieldInfo to docstring"""
    doc_parts = []
    if field_info.description is not None:
        doc_parts.append(f"Description: {field_info.description}")

    if field_info.annotation is not None:
        if hasattr(field_info.annotation, "__name__"):
            doc_parts.append(f"Type: {field_info.annotation.__name__}")
        else:
            doc_parts.append(f"Type: {field_info.annotation}")

    constraints_doc = ", ".join([str(i) for i in field_info.metadata])
    if constraints_doc != "":
        doc_parts.append(f"Constraints: {constraints_doc}\n")
    return " | ".join(doc_parts)


class ArgBase(BaseModel):
    """
    This is a base class for the arguments.
    ```python
    class Args(ArgBase):
        ...
    ```
    """

    @classmethod
    def __pydantic_init_subclass__(cls, **kwargs):
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

    @classmethod
    @validate_call
    def from_config(cls, path: Union[str, Path]):
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
        if p.suffix == ".json":
            with open(path) as fp:
                return cls.model_validate(_multi_index(json.load(fp), sn))
        raise ValueError(f"Unsupported config file format: {p.suffix}")

    def setattr(self, **kargs):
        for k, v in kargs.items():
            setattr(self, k, v)
        return self


class JobArgBase(ArgBase):
    def script(self) -> str:
        raise NotImplementedError

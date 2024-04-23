from __future__ import annotations

import itertools
from typing import (
    ClassVar,
    Generic,
    TypeVar,
    Union,
    get_args,
    overload,
)

import emcfile as ef
import numpy as np
import numpy.typing as npt
from pydantic import BaseModel, ConfigDict, model_validator

T = TypeVar("T", bound=BaseModel)


class DataList(BaseModel, Generic[T]):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    indptr_map: ClassVar[dict[str, list[str]]]
    inverse_indptr_map: ClassVar[dict[str, str]]
    element_type: ClassVar[type[T]]
    shape: tuple[int]

    def __init_subclass__(cls, indptr_map={}, **kwargs):
        element_type = kwargs.pop("element_type", None)
        super().__init_subclass__(**kwargs)
        if element_type is not None:
            cls.element_type = element_type
        cls.indptr_map = indptr_map.copy()
        cls.inverse_indptr_map = {v: k for k, vs in cls.indptr_map.items() for v in vs}

    @overload
    def __getitem__(self, index: Union[int, np.integer]) -> T:
        pass

    @overload
    def __getitem__(self, index: Union[npt.NDArray, slice]) -> DataList[T]:
        pass

    def __getitem__(self, index) -> Union[T, DataList[T]]:
        if isinstance(index, (int, np.integer)):
            e_dict = dict()
            for k in self.element_type.__annotations__:
                if k in self.inverse_indptr_map:
                    e_dict[k] = getattr(self, k)[
                        getattr(self, self.inverse_indptr_map[k])[index] : getattr(
                            self, self.inverse_indptr_map[k]
                        )[index + 1]
                    ]
                else:
                    e_dict[k] = getattr(self, k)[index]
            return self.element_type(**e_dict)
        elif isinstance(index, (np.ndarray, slice)):
            if isinstance(index, slice):
                index = np.arange(
                    0 if index.start is None else index.start,
                    len(self) if index.stop is None else index.stop,
                    1 if index.step is None else index.step,
                )
            elif index.dtype == bool:
                index = np.where(index)[0]
            e_dict = dict()
            for indptr_key, vs in self.indptr_map.items():
                indptr = getattr(self, indptr_key)
                starts = indptr[index]
                ends = indptr[index + 1]
                indices = np.r_[tuple(np.arange(i, j) for i, j in zip(starts, ends))]
                for v in vs:
                    e_dict[v] = getattr(self, v)[indices]
                e_dict[indptr_key] = np.cumsum(np.r_[0, ends - starts])
            for k in self.element_type.__annotations__:
                if k not in self.inverse_indptr_map:
                    e_dict[k] = getattr(self, k)[index]
            e_dict["shape"] = (len(index),)
            return type(self).model_validate(e_dict)
        raise ValueError("Invalid index")

    def iter(self):
        for i in range(len(self)):
            yield self[i]

    def __eq__(self, other: DataList[T]) -> bool:
        if self is other:
            return True
        if self.__class__ is not other.__class__:
            return NotImplemented  # better than False
        for k in type(self).__annotations__:
            v = getattr(self, k)
            if isinstance(v, np.ndarray):
                if not np.array_equal(v, getattr(other, k)):
                    return False
            elif v != getattr(other, k):
                return False

        return True

    def __len__(self):
        return self.shape[0]

    def extend(self, other: DataList[T]) -> None:
        for k in self.element_type.__annotations__:
            v = getattr(self, k)
            if isinstance(v, np.ndarray):
                setattr(self, k, np.concatenate([v, getattr(other, k)], axis=0))
            else:
                raise NotImplementedError
        for k in self.indptr_map:
            v = getattr(self, k)
            if isinstance(v, np.ndarray):
                setattr(
                    self, k, np.concatenate([v, getattr(other, k)[1:] + v[-1]], axis=0)
                )
            else:
                raise NotImplementedError
        self.shape = (self.shape[0] + other.shape[0],)

    @model_validator(mode="after")
    def validate_indptr(self):
        for k, vs in self.indptr_map.items():
            indpter = getattr(self, k)
            length = indpter[-1]
            if not np.all(np.diff(indpter) >= 0):
                raise ValueError(f"{k} should be increasing")
            for v in vs:
                if len(getattr(self, v)) != length:
                    raise ValueError(f"Length of {v} should be one more than {k}")
        return self

    def to_h5(self, fn):
        ef.write_obj_h5(fn, self.model_dump(), overwrite=True)

    @classmethod
    def from_h5(cls, fn):
        return cls.model_validate(ef.read_obj_h5(fn))


TL = TypeVar("TL", bound=DataList)


class DataCollector(Generic[TL]):
    def __new__(cls):
        cls = super().__new__(cls)
        list_type = get_args(cls.__orig_bases__[0])[0]
        cls.list_type = list_type
        cls._inverse_list_type = list_type.inverse_indptr_map
        cls.element_type = list_type.element_type
        return cls

    def __init__(self):
        self._data = {}
        self._count = 0

    def append(self, e):
        if not isinstance(e, self.element_type):
            raise ValueError(f"Element must be of type {self.element_type}")

        added = False
        for k in self.element_type.__annotations__:
            if k not in self._data:
                self._data[k] = []
            ek = getattr(e, k)
            self._data[k].append(ek)
            if k in self._inverse_list_type:
                indptr_type = self._inverse_list_type[k]
                if indptr_type not in self._data:
                    self._data[indptr_type] = []
                if not added:
                    self._data[indptr_type].append(len(ek))
                    added = True
        self._count += 1

    def __len__(self):
        return self._count

    def finish(self) -> TL:
        ans = dict()
        for k in self.element_type.__annotations__:
            if k in self._inverse_list_type:
                ans[k] = np.concatenate(self._data[k], axis=0)
                indptr_type = self._inverse_list_type[k]
                if indptr_type not in ans:
                    indptr = self._data[indptr_type]
                    indptr.insert(0, 0)
                    ans[indptr_type] = np.cumsum(np.array(indptr))
            else:
                ans[k] = np.array(self._data[k])
        ans["shape"] = (self._count,)
        return self.list_type.model_validate(ans)

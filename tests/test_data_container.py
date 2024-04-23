import numpy as np
import numpy.typing as npt
import pytest
from pydantic import BaseModel, ConfigDict

from job_helper.data_container import DataCollector, DataList


class C(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    c: int
    x: npt.NDArray
    y: npt.NDArray
    s: npt.NDArray


class CList(
    DataList[C],
    element_type=C,
    indptr_map={"y_indptr": ["y", "s"]},
):
    c: npt.NDArray
    x: npt.NDArray
    y_indptr: npt.NDArray[np.intp]
    y: npt.NDArray
    s: npt.NDArray


class CCollector(DataCollector[CList]):
    pass


@pytest.fixture
def clist():
    mc = CCollector()
    for i in range(10):
        mc.append(
            C(
                c=i,
                x=np.array([i, i]),
                y=np.array([2] * (i + 1)),
                s=np.array([[4, 4]] * (i + 1)),
            )
        )
    return mc.finish()


def test_data_collector(clist, tmpdir):
    len1 = len(clist)
    clist.extend(clist)
    assert len(clist) == 2 * len1
    fn = tmpdir / "test.h5"
    clist.to_h5(str(fn))
    c2 = CList.from_h5(str(fn))
    assert clist == c2
    np.testing.assert_array_equal(c2[0].x, 0)
    np.testing.assert_array_equal(c2[0:4][-1].x, 3)


def test_iterator(clist):
    mc = CCollector()
    for c in clist.iter():
        mc.append(c)
    assert clist == mc.finish()

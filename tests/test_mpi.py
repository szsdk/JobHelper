import pytest
from job_helper.mpi_helper import mpi_map


@pytest.mark.mpi(min_size=2)
def test_mpi_map():
    ans = mpi_map(lambda x: (x, x**2), range(10))
    if ans is not None:
        for x, y in ans:
            assert y == x**2

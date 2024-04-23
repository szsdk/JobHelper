import enum
from typing import Callable, Iterable, Union

from mpi4py import MPI

__all__ = ["mpi_map"]


class Tag(enum.IntEnum):
    WORK = 0
    DIE = 1


def master(iterator) -> list:
    comm = MPI.COMM_WORLD
    status = MPI.Status()
    slaves = []
    for si, data in zip(range(1, comm.size), iterator):
        comm.send(obj=data, dest=si, tag=Tag.WORK)
        slaves.append(si)

    outputs = []
    for data in iterator:
        outputs.append(comm.recv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status))
        comm.send(obj=data, dest=status.source, tag=Tag.WORK)

    for _ in slaves:
        outputs.append(comm.recv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status))
        comm.send(obj=None, dest=status.source, tag=Tag.DIE)

    return outputs


def slave(func) -> None:
    comm = MPI.COMM_WORLD
    status = MPI.Status()
    while True:
        data = comm.recv(source=0, tag=MPI.ANY_TAG, status=status)
        if status.Get_tag() == Tag.DIE:
            break
        comm.send(obj=func(data), dest=0)
    return None


def mpi_map(func: Callable, iterator: Iterable) -> Union[list, None]:
    comm = MPI.COMM_WORLD
    if comm.size < 2:
        raise ValueError("mpi_map requires at least 2 processes")

    if comm.rank == 0:
        return master(iterator)
    else:
        return slave(func)

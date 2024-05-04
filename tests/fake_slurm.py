import base64
import json
import os
import subprocess
import sys
import time
import zlib
from datetime import datetime
from queue import Empty, Queue
from threading import Event, Thread
from typing import Literal, Optional, Union

import zmq
from job_helper.slurm_helper import JobInfo
from loguru import logger
from pydantic import BaseModel, Field, TypeAdapter, validate_call

PORT = int(os.environ["_TEST_PORT"]) if "_TEST_PORT" in os.environ else 5555


class SubmitCommand(BaseModel):
    script: str


class StopCommand(BaseModel):
    cmd: Literal["stop"] = "stop"


class FinishCommand(BaseModel):
    cmd: Literal["finish"] = "finish"


class QueryStateCommand(BaseModel):
    cmd: Literal["query_history"] = "query_history"


class SbatchResponse(BaseModel):
    job_id: int


class ServerStatusResponse(BaseModel):
    status: Literal["stopped", "finished"]


class ErrorResponse(BaseModel):
    error: str


class ServerState(BaseModel):
    job_id: int = 0
    jobs: dict[int, JobInfo] = Field(default_factory=dict)


Command = Union[SubmitCommand, StopCommand, FinishCommand, QueryStateCommand]

Response = Union[ServerState, SbatchResponse, ServerStatusResponse, ErrorResponse]


def client(command: Command, port=None, type_adapter=TypeAdapter(Response)) -> Response:
    # The default port should be None and set to PORT in the function. This is because the default
    # value of PORT may be mocked by pytest.
    if port is None:
        port = PORT
    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    socket.connect(f"tcp://localhost:{port}")

    socket.send(command.model_dump_json().encode())

    return type_adapter.validate_json(json.loads(socket.recv().decode()))


def to_base64(obj: BaseModel) -> str:
    return base64.b64encode(zlib.compress(obj.model_dump_json().encode(), 9)).decode()


def from_base64(s: str) -> str:
    return zlib.decompress(base64.b64decode(s.encode())).decode()


def worker(job_queue, jobs: dict[int, JobInfo], stop_event, finish_event):
    while not stop_event.is_set():
        if finish_event.is_set() and job_queue.empty():
            break
        try:
            job_id, script = job_queue.get(timeout=1)
            job = jobs[job_id]
            job.State = "RUNNING"
            job.Start = datetime.now()
            result = subprocess.run(script, shell=True, executable="/bin/bash")
            job.End = datetime.now()
            job.State = "COMPLETED" if result.returncode == 0 else "FAILED"
        except Empty:
            continue


def send_response(socket, response):
    socket.send(json.dumps(response.model_dump_json()).encode())


def server(init_state: Optional[str] = None, port=None):
    # The default port should be None and set to PORT in the function. This is because the default
    # value of PORT may be mocked by pytest.
    if port is None:
        port = PORT
    if init_state is None:
        server_state = ServerState()
    else:
        server_state = ServerState.model_validate_json(from_base64(init_state))

    job_queue = Queue()
    stop_event = Event()
    finish_event = Event()
    thread = Thread(
        target=worker, args=(job_queue, server_state.jobs, stop_event, finish_event)
    )
    thread.start()

    with zmq.Context() as context:
        socket = context.socket(zmq.REP)
        socket.bind(f"tcp://*:{port}")

        while True:
            message = socket.recv().decode()
            try:
                command = TypeAdapter(Command).validate_json(message)
            except ValueError:
                send_response(socket, ErrorResponse(error=f"Invalid input: {message}"))
                continue

            if isinstance(command, SubmitCommand):
                server_state.job_id += 1
                job_id = server_state.job_id
                logger.info(f"Received script for Job ID {job_id}")
                job_queue.put((job_id, command.script))

                server_state.jobs[job_id] = JobInfo(JobID=job_id, State="PENDING")

                send_response(socket, SbatchResponse(job_id=job_id))
            elif isinstance(command, QueryStateCommand):
                send_response(socket, server_state)
            elif isinstance(command, StopCommand):
                stop_event.set()
                thread.join()
                send_response(socket, ServerStatusResponse(status="stopped"))
                break
            elif isinstance(command, FinishCommand):
                finish_event.set()
                thread.join()
                send_response(socket, ServerStatusResponse(status="finished"))
                break
            else:
                send_response(socket, ErrorResponse(error="Invalid command"))


class SlurmServer:
    def __init__(self, init_state: ServerState = ServerState()):
        self.init_state = init_state

    def complete_all(self):
        while True:
            response = client(
                QueryStateCommand(), type_adapter=TypeAdapter(ServerState)
            )
            assert isinstance(response, ServerState)
            if any(
                job.State in {"PENDING", "RUNNING"} for job in response.jobs.values()
            ):
                time.sleep(0.1)
            else:
                break

    def __enter__(self):
        self.p = subprocess.Popen(
            [
                "python",
                "tests/fake_slurm.py",
                "server",
                "--init-state",
                to_base64(self.init_state),
            ]
        )
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        client(FinishCommand())
        self.p.wait()


def sbatch(parsable: bool = False):
    response = client(
        SubmitCommand(script="".join([line for line in sys.stdin])),
        type_adapter=TypeAdapter(SbatchResponse),
    )
    assert isinstance(response, SbatchResponse)
    if parsable:
        print(response.job_id)
    else:
        print(f"Submitted batch job {response.job_id}")


def _format_jobs(jobs):
    ans = []
    ans.append("|".join([i for i in JobInfo.__annotations__]))
    for job in jobs:
        terms = []
        for k in JobInfo.__annotations__:
            v = getattr(job, k)
            if isinstance(v, datetime):
                v = v.isoformat()
            terms.append(str(v))
        ans.append("|".join(terms))
    return "\n".join(ans)


@validate_call
def sacct(
    jobs: Union[int, list[int]],
    format: str = "jobid,jobname,start,end,state,partition,AllocCPUS,elapse",
    allocations: bool = False,
    parsable2: bool = False,
):
    response = client(QueryStateCommand(), type_adapter=TypeAdapter(ServerState))
    assert isinstance(response, ServerState)
    if isinstance(jobs, int):
        jobs = [jobs]
    print(_format_jobs(response.jobs[i] for i in jobs))
    # return response


if __name__ == "__main__":
    import fire

    fire.Fire()

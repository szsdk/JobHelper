import json
import logging
import subprocess
import sys
import time
from queue import Empty, Queue
from threading import Event, Thread
from typing import Literal, Optional, Union

import zmq
from pydantic import BaseModel, ConfigDict, TypeAdapter


class SubmitCommand(BaseModel):
    script: str


class StopCommand(BaseModel):
    cmd: Literal["stop"] = "stop"


class FinishCommand(BaseModel):
    cmd: Literal["finish"] = "finish"


class QueryHistoryCommand(BaseModel):
    cmd: Literal["query_history"] = "query_history"


class SbatchResponse(BaseModel):
    job_id: int


class ServerStatusResponse(BaseModel):
    status: Literal["stopped", "finished"]


class ErrorResponse(BaseModel):
    error: str


class JobInfo(BaseModel):
    job_id: int
    returncode: Optional[int] = None
    script: str
    status: Literal["pending", "running", "finished"]
    start_time: Optional[float] = None
    end_time: Optional[float] = None


class SacctResponse(BaseModel):
    job_history: dict[int, JobInfo]


Command = Union[SubmitCommand, StopCommand, FinishCommand, QueryHistoryCommand]

Response = Union[SbatchResponse, ServerStatusResponse, ErrorResponse, SacctResponse]


def client(command: Command) -> Response:
    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    socket.connect("tcp://localhost:5555")

    socket.send(command.model_dump_json().encode())

    return TypeAdapter(Response).validate_json(json.loads(socket.recv().decode()))


def worker(job_queue, job_history, stop_event, finish_event):
    while not stop_event.is_set():
        if finish_event.is_set() and job_queue.empty():
            break
        try:
            job_id, script = job_queue.get(timeout=1)
            job = job_history[job_id]
            job.status = "running"
            job.start_time = time.time()
            result = subprocess.run(script, shell=True, executable="/bin/bash")
            job.end_time = time.time()
            job.returncode = result.returncode
        except Empty:
            continue


def send_response(socket, response):
    socket.send(json.dumps(response.model_dump_json()).encode())


def server():
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.bind("tcp://*:5555")

    job_history = {}
    job_queue = Queue()
    stop_event = Event()
    finish_event = Event()
    thread = Thread(
        target=worker, args=(job_queue, job_history, stop_event, finish_event)
    )
    thread.start()

    job_id = 0

    while True:
        message = socket.recv().decode()
        try:
            command = TypeAdapter(Command).validate_json(message)
        except ValueError as e:
            send_response(socket, ErrorResponse(error=f"Invalid input: {message}"))
            continue

        if isinstance(command, SubmitCommand):
            job_id += 1
            logging.info(f"Received script for Job ID {job_id}")
            job_queue.put((job_id, command.script))

            job_history[job_id] = JobInfo(
                job_id=job_id, script=command.script, status="pending"
            )

            send_response(socket, SbatchResponse(job_id=job_id))
        elif isinstance(command, QueryHistoryCommand):
            send_response(socket, SacctResponse(job_history=job_history))
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

    socket.close()
    context.term()
    print(job_history)


class SlurmServer:
    def __enter__(self):
        self.p = subprocess.Popen(["python", "tests/fake_slurm.py", "server"])

    def __exit__(self, exc_type, exc_val, exc_tb):
        client(FinishCommand())
        self.p.wait()


def sbatch(parsable: bool = False):
    response = client(SubmitCommand(script="".join([line for line in sys.stdin])))
    assert isinstance(response, SbatchResponse)
    if parsable:
        print(response.job_id)
    else:
        print(f"Submitted batch job {response.job_id}")


def sacct():
    print(client(QueryHistoryCommand()))


if __name__ == "__main__":
    import fire

    fire.Fire()

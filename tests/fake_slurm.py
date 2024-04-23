import json
import subprocess
import sys
from queue import Empty, Queue
from threading import Event, Thread
from typing import Literal, Union

import zmq
from pydantic import BaseModel, TypeAdapter


class SubmitCommand(BaseModel):
    script: str


class StopCommand(BaseModel):
    cmd: Literal["stop"] = "stop"


class FinishCommand(BaseModel):
    cmd: Literal["finish"] = "finish"


class StartCommand(BaseModel):
    cmd: Literal["start"] = "start"


Command = Union[SubmitCommand, StopCommand, StartCommand, FinishCommand]


def client(command: Command):
    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    socket.connect("tcp://localhost:5555")

    socket.send(command.model_dump_json().encode())

    response = json.loads(socket.recv().decode())
    if "job_id" in response:
        print(f"Submitted batch job {response['job_id']}")


def worker(job_queue, job_history, stop_event, finish_event):
    while not stop_event.is_set():
        if finish_event.is_set() and job_queue.empty():
            break
        try:
            job_id, script = job_queue.get(timeout=1)
            return_code = subprocess.run(script, shell=True, executable="/bin/bash")
            job_history[job_id] = return_code
        except Empty:
            continue


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
            # print(f"Invalid command received: {e}")
            continue

        if isinstance(command, SubmitCommand):
            job_id += 1
            # print(f"Received script for Job ID {job_id}")
            job_queue.put((job_id, command.script))
            response = {"job_id": job_id}
        elif isinstance(command, StopCommand):
            stop_event.set()
            thread.join()
            response = {"status": "server_stopped"}
            socket.send(json.dumps(response).encode())
            break
        elif isinstance(command, FinishCommand):
            finish_event.set()
            thread.join()
            response = {"status": "server_finished"}
            socket.send(json.dumps(response).encode())
            break
        else:
            response = {"status": "unknown_command"}
        socket.send(json.dumps(response).encode())

    socket.close()
    context.term()
    print(job_history)


class SlurmServer:
    def __enter__(self):
        self.p = subprocess.Popen(["python", "tests/fake_slurm.py", "server"])

    def __exit__(self, exc_type, exc_val, exc_tb):
        client(FinishCommand())
        self.p.wait()


if __name__ == "__main__":
    if sys.argv[1] == "server":
        server()
    else:
        client(SubmitCommand(script="".join([line for line in sys.stdin])))

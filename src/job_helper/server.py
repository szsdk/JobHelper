import time
from datetime import datetime
from functools import lru_cache
from importlib import resources
from pathlib import Path
from typing import Optional

import uvicorn
from fastapi import FastAPI, Response
from loguru import logger

from .config import jhcfg
from .project_helper import ProjectRunningResult, get_scheduler

app = FastAPI()


@app.get("/", response_class=Response, responses={200: {"content": {"text/html": {}}}})
async def serve_html():
    html_content = resources.read_text("job_helper._htmls", "index.html")
    return Response(content=html_content, media_type="text/html")


@app.get("/project_result/")
async def get_project_list() -> list[int]:
    a = [int(s.stem) for s in sorted(Path("log/project/").glob("*.json"), reverse=True)]
    return a


@lru_cache
def get_job_states(prr_fn, ttl_hash: Optional[int] = None):
    del ttl_hash
    prr = ProjectRunningResult.from_config(prr_fn)
    return prr, prr._job_states()


def get_ttl_hash(seconds=2) -> int:
    """Return the same value withing `seconds` time period"""
    return int(time.time() / seconds)


@app.get("/project_result/gantt/{project_id}")
async def get_project_result(project_id: int):
    prr, job_states = get_job_states(f"log/project/{project_id}.json", get_ttl_hash())
    s = generate_mermaid_gantt_chart(job_states)
    clicks = []
    for job, state in job_states.items():
        clicks.append(f'    click {job} call copyTextToClipboard("{state.JobID}")')
    return s + "\n".join(clicks)


def generate_mermaid_gantt_chart(jobs):
    """
    Generate Mermaid Gantt chart code from a dictionary of jobs.

    Parameters:
    - jobs: A dictionary where keys are job names and values are JobInfo instances.

    Returns:
    - A string containing the formatted Mermaid Gantt chart code.
    """
    # Start the Mermaid Gantt chart code
    mermaid_code = """gantt
    dateFormat  YYYY-MM-DDTHH:mm:ss.SSS
    axisFormat  %H:%M:%S
"""
    state_map = {
        "COMPLETED": "done",
        "FAILED": "crit",
        "RUNNING": "active",
        "PENDING": "milestone",
    }
    for job_name, info in jobs.items():
        if info.State == "PENDING":
            end = datetime.now()
            start = end
        elif info.State == "RUNNING":
            start = info.Start
            end = datetime.now()
        else:
            start = datetime.now() if info.Start == "Unknown" else info.Start
            end = datetime.now() if info.End == "Unknown" else info.End

        if info.State in state_map:
            state = state_map[info.State]
        elif "CANCELLED" in info.State:
            state = "crit"
        else:
            state = "crit"
        mermaid_code += f"    {job_name} :{state}, {job_name}, {start.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]}, {end.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]} \n    %% {job_name}: {info.JobID} {info.State}\n"

    return mermaid_code


def flowchart(nodes: dict[str, str], links: dict[tuple[str, str], str]):
    node_styles = {
        "norun": "    classDef norun fill:#ddd,stroke:#aaa,stroke-width:3px,stroke-dasharray: 5 5",
        "failed": "    classDef failed fill:#eaa,stroke:#e44",
        "completed": "    classDef completed fill:#aea,stroke:#4a4",
    }
    link_styles = {
        "after": "--o",
        "afterany": "-.-o",
        "afternotok": "-.-x",
        "afterok": "-->",
    }

    flow = ["flowchart TD"]
    for (job_a, job_b), link in links.items():
        a = job_a if job_a not in nodes else f"{job_a}:::{nodes[job_a]}"
        b = job_b if job_b not in nodes else f"{job_b}:::{nodes[job_b]}"
        flow.append(f"    {a} {link_styles[link]} {b}")
    flow.extend(list(node_styles.values()))
    return "\n".join(flow)


@app.get("/project_result/jobflow/{project_id}")
async def get_project_jobflow(project_id: int):
    prr, job_states = get_job_states(f"log/project/{project_id}.json", get_ttl_hash())

    scheduler = get_scheduler()
    links = {
        (job_a, job_b): link_type
        for job_b, job in prr.config.jobs.items()
        for link_type in ["afterok", "after", "afternotok", "afterany"]
        for job_a in getattr(scheduler.dependency(job.job_preamble), link_type)
    }
    nodes = dict()
    clicks = []
    for job, state in job_states.items():
        if state.State == "COMPLETED":
            nodes[job] = "completed"
        elif state.State == "FAILED":
            nodes[job] = "failed"
        elif state.State == "RUNNING":
            pass
        else:
            nodes[job] = "norun"
        clicks.append(f'    click {job} call copyTextToClipboard("{state.JobID}")')

    s = flowchart(nodes, links)
    return "\n".join([s] + clicks)


def run():
    """
    Start the web server to display the running results of the project.

    This function employs Uvicorn, an ASGI server, to launch the application.
    The server's host and port are derived from the 'server' section of the 'jh_config.toml' configuration file.
    """
    logger.info("Starting server at http://{}:{}", jhcfg.server.ip, jhcfg.server.port)
    uvicorn.run(app, host=str(jhcfg.server.ip), port=jhcfg.server.port)

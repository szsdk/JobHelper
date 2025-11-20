import time
from functools import lru_cache
from pathlib import Path
from typing import Optional

import uvicorn
from fastapi import FastAPI, Response
from fastapi.responses import HTMLResponse
from loguru import logger

from .config import jhcfg
from .project_helper import ProjectRunningResult, generate_mermaid_gantt_chart, get_scheduler

app = FastAPI()


@app.get("/", response_class=Response, responses={200: {"content": {"text/html": {}}}})
async def serve_html():
    from importlib.resources import files
    html_file = files("job_helper._htmls") / "index.html"
    html_content = html_file.read_text()
    return Response(content=html_content, media_type="text/html")


@app.get("/project_result/")
async def get_project_list() -> list[int]:
    log_dir = Path("log/project/")
    if not log_dir.exists():
        return []
    a = [int(s.stem) for s in sorted(log_dir.glob("*.json"), reverse=True)]
    return a


@lru_cache(maxsize=128)
def get_job_states(prr_fn, ttl_hash: Optional[int] = None):
    del ttl_hash
    prr = ProjectRunningResult.from_config(prr_fn)
    return prr, prr._job_states()


def get_ttl_hash(seconds=2) -> int:
    """Return the same value withing `seconds` time period"""
    return int(time.time() / seconds)


@app.get("/project_result/gantt", response_class=HTMLResponse)
async def get_project_result(project_id: int, compact: bool = False):
    prr, job_states = get_job_states(f"log/project/{project_id}.json", get_ttl_hash())
    s = generate_mermaid_gantt_chart(job_states, compact=compact)
    clicks = []
    for job, state in job_states.items():
        clicks.append(f'    click {job} call copyTextToClipboard("{state.JobID}")')

    mermaid_code = s + "\n" + "\n".join(clicks)
    return f"""
    <div class="mermaid">
        {mermaid_code}
    </div>
    """


def flowchart(nodes: dict[str, str], links: dict[tuple[str, str], str], compact: bool):
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

    flow = ["flowchart LR" if compact else "flowchart TD"]
    for (job_a, job_b), link in links.items():
        a = job_a if job_a not in nodes else f"{job_a}:::{nodes[job_a]}"
        b = job_b if job_b not in nodes else f"{job_b}:::{nodes[job_b]}"
        flow.append(f"    {a} {link_styles[link]} {b}")
    flow.extend(list(node_styles.values()))
    return "\n".join(flow)


@app.get("/project_result/jobflow", response_class=HTMLResponse)
async def get_project_jobflow(project_id: int, compact: bool = False):
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

    s = flowchart(nodes, links, compact)

    mermaid_code = s + "\n" + "\n".join(clicks)
    return f"""
    <div class="mermaid">
        {mermaid_code}
    </div>
    """


def run():
    """
    Start the web server to display the running results of the project.

    This function employs Uvicorn, an ASGI server, to launch the application.
    The server's host and port are derived from the 'server' section of the 'jh_config.toml' configuration file.
    """
    logger.info("Starting server at http://{}:{}", jhcfg.server.ip, jhcfg.server.port)
    uvicorn.run(app, host=str(jhcfg.server.ip), port=jhcfg.server.port)

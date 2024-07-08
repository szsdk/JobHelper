from importlib import resources
from pathlib import Path

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


@app.get("/project_result/gantt/{project_id}")
async def get_project_result(project_id: int):
    return ProjectRunningResult.from_config(
        f"log/project/{project_id}.json"
    ).job_states("")


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
    prr = ProjectRunningResult.from_config(f"log/project/{project_id}.json")

    scheduler = get_scheduler()
    links = {
        (job_a, job_b): link_type
        for job_b, job in prr.config.jobs.items()
        for link_type in ["afterok", "after", "afternotok", "afterany"]
        for job_a in getattr(scheduler.dependency(job.job_preamble), link_type)
    }
    jobs_states = prr._job_states()
    nodes = dict()
    clicks = []
    for job, state in jobs_states.items():
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

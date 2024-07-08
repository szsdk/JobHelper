from importlib import resources
from pathlib import Path

import uvicorn
from fastapi import FastAPI, Response
from loguru import logger

from .config import jhcfg
from .project_helper import ProjectRunningResult

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


@app.get("/project_result/jobflow/{project_id}")
async def get_project_jobflow(project_id: int):
    return ProjectRunningResult.from_config(
        f"log/project/{project_id}.json"
    ).config.jobflow("")


def run():
    """
    Start the web server to display the running results of the project.

    This function employs Uvicorn, an ASGI server, to launch the application.
    The server's host and port are derived from the 'server' section of the 'jh_config.toml' configuration file.
    """
    logger.info("Starting server at http://{}:{}", jhcfg.server.ip, jhcfg.server.port)
    uvicorn.run(app, host=str(jhcfg.server.ip), port=jhcfg.server.port)

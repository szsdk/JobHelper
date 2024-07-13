from fastapi.testclient import TestClient
from job_helper.server import app

client = TestClient(app)


def test_simple():
    response = client.get("/")
    assert response.status_code == 200
    response = client.get("/project_result/")
    assert response.status_code == 200
    assert len(response.json()) == 0

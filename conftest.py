import pytest
import zmq

import tests.fake_slurm
# Import fixtures so they're available to all tests
from tests.utils import slurm_server, testing_jhcfg  # noqa: F401


@pytest.fixture(autouse=True)
def port_number(monkeypatch):
    """Finds and returns an unused port."""
    context = zmq.Context()
    socket = context.socket(zmq.PAIR)
    socket.bind("tcp://*:*")  # Bind to any available port
    port_selected = socket.bind_to_random_port(
        "tcp://*", min_port=5855, max_port=6100, max_tries=100
    )
    socket.close()
    context.term()
    monkeypatch.setenv("_TEST_PORT", str(port_selected))
    monkeypatch.setattr(tests.fake_slurm, "PORT", port_selected)
    yield int(port_selected)

import uvicorn
from fastapi import FastAPI
from typing import Optional

from brad.ui.models import ClientState, SetClientState
from .pause_controller import PauseController


class Manager:
    def __init__(self, pc: PauseController) -> None:
        self.pc = pc


app = FastAPI()
manager: Optional[Manager] = None


@app.get("/clients")
def get_clients() -> ClientState:
    global manager  # pylint: disable=global-variable-not-assigned
    assert manager is not None
    return ClientState(
        curr_clients=manager.pc.num_running_clients,
        max_clients=manager.pc.total_num_clients,
    )


@app.post("/clients")
def set_clients(set_state: SetClientState) -> ClientState:
    global manager  # pylint: disable=global-variable-not-assigned
    assert manager is not None
    manager.pc.adjust_num_running_clients(set_state.curr_clients, verbose=True)
    return ClientState(
        curr_clients=manager.pc.num_running_clients,
        max_clients=manager.pc.total_num_clients,
    )


def serve(
    pc: PauseController, port: int, host: str = "0.0.0.0", log_level: str = "info"
) -> None:
    try:
        global manager  # pylint: disable=global-statement
        manager = Manager(pc)
        uvicorn.run(
            "workload_utils.change_clients_api:app",
            host=host,
            port=port,
            log_level=log_level,
        )
    finally:
        manager = None

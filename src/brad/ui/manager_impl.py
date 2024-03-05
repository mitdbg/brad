import asyncio
import uvicorn
import logging
from fastapi import FastAPI
from typing import Optional

from brad.config.file import ConfigFile
from brad.ui.uvicorn_server import PatchedUvicornServer

logger = logging.getLogger(__name__)


class UiManagerImpl:
    def __init__(self, config: ConfigFile) -> None:
        self._config = config

    async def serve_forever(self) -> None:
        global manager  # pylint: disable=global-statement
        try:
            if manager is not None:  # pylint: disable=used-before-assignment
                raise RuntimeError(
                    "Cannot start UiManager while one is already running."
                )

            uvicorn_config = uvicorn.Config(
                "brad.ui.manager_impl:app",
                port=self._config.ui_port(),
                log_level="info",
            )
            server = PatchedUvicornServer(uvicorn_config)
            manager = self
            await server.serve()
        except asyncio.CancelledError:
            logger.info("Shutting down the BRAD UI webserver...")
            server.should_exit = True
        finally:
            manager = None


# Note that because this is global, we can only serve one router instance at a
# time. But this is acceptable given our use cases.
app = FastAPI()
manager: Optional[UiManagerImpl] = None


@app.get("/")
def read_root():
    return {"Hello": "World"}

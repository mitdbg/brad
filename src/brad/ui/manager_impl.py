import asyncio
import uvicorn
import logging
import importlib.resources as pkg_resources
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from typing import Optional

import brad.ui.static as brad_app
from brad.config.file import ConfigFile
from brad.daemon.monitor import Monitor
from brad.ui.uvicorn_server import PatchedUvicornServer
from brad.ui.models import MetricsData, TimestampedMetrics
from brad.daemon.front_end_metrics import FrontEndMetric

logger = logging.getLogger(__name__)


class UiManagerImpl:
    def __init__(self, config: ConfigFile, monitor: Monitor) -> None:
        self.config = config
        self.monitor = monitor

    async def serve_forever(self) -> None:
        global manager  # pylint: disable=global-statement
        try:
            if manager is not None:  # pylint: disable=used-before-assignment
                raise RuntimeError(
                    "Cannot start UiManager while one is already running."
                )

            uvicorn_config = uvicorn.Config(
                "brad.ui.manager_impl:app",
                host=self.config.ui_interface(),
                port=self.config.ui_port(),
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
manager: Optional["UiManagerImpl"] = None


@app.get("/api/1/metrics")
def get_metrics(num_values: int = 3) -> MetricsData:
    assert manager is not None
    metrics = manager.monitor.front_end_metrics().read_k_most_recent(k=num_values)
    values = metrics[FrontEndMetric.QueryLatencySecondP90.value]
    series = TimestampedMetrics(timestamps=list(values.index), values=list(values))
    return MetricsData(
        named_metrics={FrontEndMetric.QueryLatencySecondP90.value: series}
    )


# Serve the static pages.
# Note that this should go last as a "catch all" route.
static_files = pkg_resources.files(brad_app)
with pkg_resources.as_file(static_files) as static_dir:
    app.mount("/", StaticFiles(directory=static_dir, html=True), name="static")

from brad.config.file import ConfigFile
import importlib.resources as pkg_resources
from typing import Dict, List, Tuple
import json
from brad.config.engine import Engine
import brad.daemon as daemon
from datetime import timedelta


class Monitor:
    def __init__(self, config: ConfigFile) -> None:
        self._config = config
        self._epoch_length, self._metrics = self._load_monitored_metrics()

    async def run_forever(self) -> None:
        # Flesh out the monitor - maintain running averages of the underlying
        # engines' metrics.
        pass

    def _load_monitored_metrics(
        self,
    ) -> Tuple[Dict[str, Dict[str, List[str]]], timedelta]:
        # Load data.
        with pkg_resources.open_text(daemon, "monitored_metrics.json") as data:
            file_contents = json.load(data)

        epoch_length = timedelta(
            weeks=file_contents["epoch_length"]["weeks"],
            days=file_contents["epoch_length"]["days"],
            hours=file_contents["epoch_length"]["hours"],
            minutes=file_contents["epoch_length"]["minutes"],
        )

        metrics_map = {}
        for f in file_contents["monitored_metrics"]:
            try:
                eng_name = Engine.from_str(f["engine"])
            except:
                continue

            metrics_map[eng_name] = {}

            for m in f["metrics"]:
                metrics_map[eng_name][m] = f["metrics"][m]

        return epoch_length, metrics_map


if __name__ == "__main__":
    monitor = Monitor()

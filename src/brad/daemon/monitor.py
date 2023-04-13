from brad.config.file import ConfigFile


class Monitor:
    def __init__(self, config: ConfigFile) -> None:
        self._config = config

    async def run_forever(self) -> None:
        # Flesh out the monitor - maintain running averages of the underlying
        # engines' metrics.
        pass

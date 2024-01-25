from typing import Dict
from brad.config.engine import Engine


class ReestablishConnectionsReport:
    def __init__(self) -> None:
        self.connection_successes: Dict[Engine, int] = {}
        self.connection_successes[Engine.Aurora] = 0
        self.connection_successes[Engine.Athena] = 0
        self.connection_successes[Engine.Redshift] = 0

        self.connection_failures: Dict[Engine, int] = {}
        self.connection_failures[Engine.Aurora] = 0
        self.connection_failures[Engine.Athena] = 0
        self.connection_failures[Engine.Redshift] = 0

        self.still_connected: Dict[Engine, int] = {}
        self.still_connected[Engine.Aurora] = 0
        self.still_connected[Engine.Athena] = 0
        self.still_connected[Engine.Redshift] = 0

    def bump(self, engine: Engine, succeeded: bool) -> None:
        if succeeded:
            self.connection_successes[engine] += 1
        else:
            self.connection_failures[engine] += 1

    def bump_still_connected(self, engine: Engine) -> None:
        self.still_connected[engine] += 1

    def merge(
        self, other: "ReestablishConnectionsReport"
    ) -> "ReestablishConnectionsReport":
        for engine in [Engine.Aurora, Engine.Athena, Engine.Redshift]:
            self.connection_successes[engine] += other.connection_successes[engine]
            self.connection_failures[engine] += other.connection_failures[engine]
            self.still_connected[engine] += other.still_connected[engine]
        return self

    def all_succeeded(self) -> bool:
        failures = 0
        for engine in [Engine.Aurora, Engine.Athena, Engine.Redshift]:
            failures += self.connection_failures[engine]
        return failures == 0

    def __repr__(self) -> str:
        parts = []
        for engine in [Engine.Aurora, Engine.Athena, Engine.Redshift]:
            succeeded = self.connection_successes[engine]
            total = self.connection_successes[engine] + self.connection_failures[engine]
            still = self.still_connected[engine]
            parts.append(f"{engine.value}={succeeded}/{total}/{still}")
        return "".join(["ReestablishConnectionsReport(", ", ".join(parts), ")"])

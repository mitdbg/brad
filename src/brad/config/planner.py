import yaml


class PlannerConfig:
    def __init__(self, path: str):
        self._raw_path = path
        with open(path, "r", encoding="UTF-8") as file:
            self._raw = yaml.load(file, Loader=yaml.Loader)

    def max_num_table_moves(self) -> int:
        return int(self._raw["max_num_table_moves"])

    def max_provisioning_multiplier(self) -> float:
        return float(self._raw["max_provisioning_multiplier"])

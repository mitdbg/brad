from typing import Any, Dict, Optional


class HotConfig:
    """
    This class is used for any configurations that are meant to be modified
    while the daemon is running (hence "hot"). This is meant to be a singleton.

    This is meant to be used to work around invasive changes to the codebase for
    exploratory experiments. Longer-term features should not be implemented
    using this class.
    """

    @classmethod
    def instance(cls) -> "HotConfig":
        global _INSTANCE  # pylint: disable=global-statement
        if _INSTANCE is None:
            _INSTANCE = cls()
        return _INSTANCE

    def __init__(self) -> None:
        self._config: Dict[str, Any] = {}

    def set_value(self, key: str, value: Any) -> None:
        self._config[key] = value

    def get_value(self, key: str, default: Optional[Any] = None) -> Any:
        try:
            return self._config[key]
        except KeyError:
            if default is not None:
                return default
            raise


_INSTANCE: Optional[HotConfig] = None

from typing import Any

from brad.config.file import ConfigFile


class UiManager:
    """
    This class is the entrypoint to BRAD's user interface.
    """

    @staticmethod
    def is_supported() -> bool:
        """
        If BRAD was installed without the `ui` option, this function will
        return False. Otherwise, it returns True.
        """
        try:
            import fastapi  # pylint: disable=unused-import
            import uvicorn  # pylint: disable=unused-import

            return True

        except ImportError:
            return False

    @classmethod
    def create(cls, config: ConfigFile) -> "UiManager":
        from brad.ui.manager_impl import UiManagerImpl

        return cls(UiManagerImpl(config))

    # We hide away the implementation details to allow external code to import
    # `UiManager` without worrying about import errors (e.g., because the
    # user has not installed BRAD with the dashboard option enabled).

    def __init__(self, impl: Any) -> None:
        from brad.ui.manager_impl import UiManagerImpl

        self._impl: UiManagerImpl = impl

    async def serve_forever(self) -> None:
        await self._impl.serve_forever()

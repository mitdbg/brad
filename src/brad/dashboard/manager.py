from typing import Any


class DashboardManager:
    """
    This class is the entrypoint to BRAD's dashboard UI.
    """

    @staticmethod
    def is_supported() -> bool:
        """
        If BRAD was installed without the `dashboard` option, this function will
        return False. Otherwise, it returns True.
        """
        try:
            import fastapi  # pylint: disable=unused-import
            import uvicorn  # pylint: disable=unused-import

            return True

        except ImportError:
            return False

    @classmethod
    def create(cls) -> "DashboardManager":
        from brad.dashboard.manager_impl import DashboardManagerImpl

        return cls(DashboardManagerImpl())

    # We hide away the implementation details to allow external code to import
    # `DashboardManager` without worrying about import errors (e.g., because the
    # user has not installed BRAD with the dashboard option enabled).

    def __init__(self, impl: Any) -> None:
        from brad.dashboard.manager_impl import DashboardManagerImpl

        self._impl: DashboardManagerImpl = impl

    async def serve_forever(self, host: str = "", port: int = 7583) -> None:
        await self._impl.serve_forever(host, port)

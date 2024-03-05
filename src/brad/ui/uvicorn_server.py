import uvicorn


class PatchedUvicornServer(uvicorn.Server):
    # Unfortunately `uvicorn` is not designed to run gracefully within an
    # existing asyncio application (like BRAD) because it installs its own
    # signal handlers, which prevents BRAD from shutting down gracefully.
    #
    # We have our own shutdown mechanism. So this class monkey-patches the
    # existing `uvicorn.Server` and prevents it from installing signal handlers.
    # We shut down the server manually as a part of BRAD's graceful shutdown
    # workflow.
    #
    # See https://github.com/encode/uvicorn/issues/1579
    def install_signal_handlers(self) -> None:
        pass

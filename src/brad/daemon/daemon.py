import logging
import socket
from brad.config.file import ConfigFile

logger = logging.getLogger(__name__)


class BradDaemon:
    @classmethod
    def connect(cls, host: str, config: ConfigFile):
        server_socket = socket.create_connection((host, config.server_daemon_port))
        logger.info(
            "Successfully connected to the server at %s:%d",
            host,
            config.server_daemon_port,
        )
        return cls(config, server_socket)

    def __init__(self, config: ConfigFile, server_socket: socket.socket):
        self._config = config
        self._server_socket = server_socket
        self._server_socket_file = self._server_socket.makefile("r")

    def __del__(self):
        self._server_socket_file.close()
        self._server_socket.close()
        self._server_socket_file = None
        self._server_socket = None

    def run(self):
        while True:
            query = self._server_socket_file.readline().strip()
            logger.info("Received %s", query)

import select
import socket
import logging
from threading import Thread
from typing import Any, Callable
from brad.utils.sentinel import Sentinel

logger = logging.getLogger(__name__)


class ConnectionAcceptor:
    """
    Manages a "server socket" for accepting connection requests from clients.
    Each time a connection is received, the `handler_function` is called
    with the new socket and address.
    """

    def __init__(
        self,
        host: str,
        port: int,
        handler_function: Callable[[socket.socket, Any], None],
    ):
        self._host = host
        self._port = port
        self._server_socket = socket.socket(
            socket.AF_INET,
            socket.SOCK_STREAM,
        )
        self._server_socket.setsockopt(
            socket.SOL_SOCKET,
            socket.SO_REUSEADDR,
            1,
        )
        self._handler_function = handler_function
        self._acceptor = Thread(target=self._accept_connections)
        self._sentinel = Sentinel()

    def start(self):
        self._server_socket.bind((self._host, self._port))
        self._port = self._server_socket.getsockname()[1]
        self._server_socket.listen()
        self._sentinel.start()
        self._acceptor.start()
        logger.debug(
            "Listening for connections on (%s:%d).",
            self._host,
            self._port,
        )

    def stop(self):
        self._sentinel.signal_exit()
        self._acceptor.join()
        self._server_socket.close()
        self._sentinel.stop()
        logging.debug(
            "Stopped listening for connections on (%s:%d).",
            self._host,
            self._port,
        )

    @property
    def host(self):
        return self._host

    @property
    def port(self):
        return self._port

    def _accept_connections(self):
        try:
            while True:
                read_ready, _, _ = select.select(
                    [self._server_socket, self._sentinel.read_pipe], [], []
                )

                if self._sentinel.should_exit(read_ready):
                    self._sentinel.consume_exit_signal()
                    break

                client_socket, address = self._server_socket.accept()
                host, port = address
                logger.debug("Accepted a connection to (%s:%d).", host, port)
                self._handler_function(client_socket, address)
        except:  # pylint: disable=bare-except
            logging.exception("Unexpectedly stopped accepting connections.")

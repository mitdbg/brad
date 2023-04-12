import logging


def set_up_logging(filename=None, debug_mode=False):
    logging_kwargs = {
        "format": "%(asctime)s %(levelname)-8s %(message)s",
        "datefmt": "%Y-%m-%d %H:%M",
        "level": logging.DEBUG if debug_mode else logging.INFO,
    }
    if filename is not None:
        # Logs will be written to a file.
        logging_kwargs["filename"] = filename
    logging.basicConfig(**logging_kwargs)

import logging


def set_up_logging(debug_mode=False):
    logging_kwargs = {
        "format": "%(asctime)s %(levelname)-8s %(message)s",
        "datefmt": "%Y-%m-%d %H:%M",
        "level": logging.DEBUG if debug_mode else logging.INFO,
    }
    logging.basicConfig(**logging_kwargs)

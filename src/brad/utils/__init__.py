import logging

VERBOSE = 5


def log_verbose(logger: logging.Logger, message: str, *args) -> None:
    logger.log(VERBOSE, message, *args)


def set_up_logging(filename=None, debug_mode=False, also_console=False):
    logging_kwargs = {
        "format": "%(asctime)s %(levelname)-8s %(message)s",
        "datefmt": "%Y-%m-%d %H:%M",
        "level": logging.DEBUG if debug_mode else logging.INFO,
    }
    if filename is not None and not also_console:
        # Logs will be written to a file.
        logging_kwargs["filename"] = filename
    elif filename is not None and also_console:
        logging_kwargs["handlers"] = [
            logging.FileHandler(filename),
            logging.StreamHandler(),
        ]
    logging.basicConfig(**logging_kwargs)
    logging.addLevelName(VERBOSE, "VERBOSE")

    # boto3 logging is too verbose - it interferes with our debug messages. This
    # snippet disables debug logging on boto3 related modules.
    logging.getLogger("boto3").setLevel(logging.INFO)
    logging.getLogger("botocore").setLevel(logging.INFO)
    logging.getLogger("nose").setLevel(logging.INFO)
    logging.getLogger("s3transfer").setLevel(logging.INFO)
    logging.getLogger("urllib3").setLevel(logging.INFO)
    logging.getLogger("redshift_connector").setLevel(logging.INFO)
    logging.getLogger("pyathena").setLevel(logging.INFO)

    # Avoids the "Using selector: EpollSelector" (or similar) messages.
    logging.getLogger("asyncio").setLevel(logging.INFO)


def create_custom_logger(
    name: str, log_file: str, level: int = logging.DEBUG
) -> logging.Logger:
    handler = logging.FileHandler(log_file)
    formatter = logging.Formatter("%(asctime)s %(levelname)-8s %(message)s")
    handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(level)
    # We do not want the messages to propagate up to the root logger.
    logger.propagate = False
    # Remove default handlers.
    for h in logger.handlers[:]:
        logger.removeHandler(h)

    logger.addHandler(handler)
    return logger

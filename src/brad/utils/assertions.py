import logging
import traceback
from typing import Optional

logger = logging.getLogger(__name__)


def nonsilent_assert(assert_value: bool, message: Optional[str] = None) -> None:
    if assert_value:
        return
    logger.error(
        "Assertion failed. %s",
        message if message is not None else "(No message provided.)",
    )
    traceback.print_stack()
    raise AssertionError(message)

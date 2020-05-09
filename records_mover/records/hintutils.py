from .hint import LiteralHint, StringHint
import chardet
from .types import (
    RecordsHints, BootstrappingRecordsHints, HintHeaderRow, HintCompression, HintQuoting,
    HintDoublequote, HintEscape, HintEncoding, HintDateFormat, HintTimeOnlyFormat,
    HintDateTimeFormatTz, HintDateTimeFormat
)
from .csv_streamer import stream_csv, python_encoding_from_hint
import io
import logging
from .types import MutableRecordsHints
from typing import Iterable, List, IO, Optional, Dict, TYPE_CHECKING
if TYPE_CHECKING:
    from pandas.io.parsers import TextFileReader


logger = logging.getLogger(__name__)


def complain_on_unhandled_hints(fail_if_dont_understand: bool,
                                unhandled_hints: Iterable[str],
                                hints: RecordsHints) -> None:
    unhandled_bindings = [f"{k}={hints[k]}" for k in unhandled_hints]
    unhandled_bindings_str = ", ".join(unhandled_bindings)
    if len(unhandled_bindings) > 0:
        if fail_if_dont_understand:
            err = "Implement these records_format hints or try again with " +\
                f"fail_if_dont_understand=False': {unhandled_bindings_str}"
            raise NotImplementedError(err)
        else:
            logger.warning(f"Did not understand these hints: {unhandled_bindings_str}")


def cant_handle_hint(fail_if_cant_handle_hint: bool, hint_name: str, hints: RecordsHints) -> None:
    if not fail_if_cant_handle_hint:
        logger.warning("Ignoring hint {hint_name} = {hint_value}"
                       .format(hint_name=hint_name,
                               hint_value=repr(hints[hint_name])))
    else:
        raise NotImplementedError(f"Implement hint {hint_name}={repr(hints[hint_name])} " +
                                  "or try again with fail_if_cant_handle_hint=False")

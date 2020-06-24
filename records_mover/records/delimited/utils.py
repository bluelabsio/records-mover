from .types import PartialRecordsHints, UntypedRecordsHints
from .validated_records_hints import ValidatedRecordsHints
import logging
from typing import Iterable, Union


logger = logging.getLogger(__name__)


def _hint_value(hints: Union[PartialRecordsHints,
                             UntypedRecordsHints,
                             ValidatedRecordsHints],
                hint_name: str) -> object:
    if isinstance(hints, ValidatedRecordsHints):
        value = getattr(hints, hint_name.replace('-', '_'))
    else:
        value = hints[hint_name]
    return value


def complain_on_unhandled_hints(fail_if_dont_understand: bool,
                                unhandled_hints: Iterable[str],
                                hints: Union[PartialRecordsHints,
                                             UntypedRecordsHints,
                                             ValidatedRecordsHints]) -> None:
    unhandled_bindings = [f"{k}={_hint_value(hints, k)}" for k in unhandled_hints]
    unhandled_bindings_str = ", ".join(unhandled_bindings)
    if len(unhandled_bindings) > 0:
        if fail_if_dont_understand:
            err = "Implement these records_format hints or try again with " +\
                f"fail_if_dont_understand=False': {unhandled_bindings_str}"
            raise NotImplementedError(err)
        else:
            logger.warning(f"Did not understand these hints: {unhandled_bindings_str}")


def cant_handle_hint(fail_if_cant_handle_hint: bool,
                     hint_name: str,
                     hints: Union[PartialRecordsHints,
                                  UntypedRecordsHints,
                                  ValidatedRecordsHints]) -> None:
    value = _hint_value(hints, hint_name)
    if not fail_if_cant_handle_hint:
        logger.warning("Ignoring hint {hint_name} = {hint_value}"
                       .format(hint_name=hint_name,
                               hint_value=repr(value)))
    else:
        raise NotImplementedError(f"Implement hint {hint_name}="
                                  f"{repr(value)} " +
                                  "or try again with fail_if_cant_handle_hint=False")

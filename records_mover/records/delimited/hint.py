from typing_inspect import is_literal_type, get_args
from abc import ABCMeta, abstractmethod
from .types import HintName, RecordsHints
from typing import TypeVar, Generic, Type, List


HintT = TypeVar('HintT')


class Hint(Generic[HintT], metaclass=ABCMeta):
    @abstractmethod
    def validate(self,
                 hints: RecordsHints,
                 fail_if_cant_handle_hint: bool) -> HintT:
        ...


class StringHint(Hint[str]):
    def __init__(self,
                 hint_name: HintName,
                 default: str) -> None:
        self.default = default
        self.hint_name = hint_name

    def validate(self,
                 hints: RecordsHints,
                 fail_if_cant_handle_hint: bool) -> str:
        from .utils import cant_handle_hint

        x: object = hints[self.hint_name]
        if isinstance(x, str):
            return x
        else:
            cant_handle_hint(fail_if_cant_handle_hint=fail_if_cant_handle_hint,
                             hint_name=self.hint_name,
                             hints=hints)
            return self.default


LiteralHintT = TypeVar('LiteralHintT')


class LiteralHint(Hint[LiteralHintT]):
    def __init__(self,
                 type_: Type[LiteralHintT],
                 hint_name: HintName,
                 default: LiteralHintT) -> None:
        assert is_literal_type(type_), f"{hint_name} is not a Literal[]"
        self.default = default
        self.type_ = type_
        self.hint_name = hint_name
        self.valid_values: List[LiteralHintT] = list(get_args(type_))

    def validate(self,
                 hints: RecordsHints,
                 fail_if_cant_handle_hint: bool) -> LiteralHintT:
        from .utils import cant_handle_hint

        # MyPy doesn't like looking for a generic optional string
        # in a list of specific optional strings.  It's wrong;
        # that's perfectly safe
        x: object = hints[self.hint_name]
        try:
            i = self.valid_values.index(x)  # type: ignore
            return self.valid_values[i]
        except ValueError:
            # well, sort of safe.
            cant_handle_hint(fail_if_cant_handle_hint=fail_if_cant_handle_hint,
                             hint_name=self.hint_name,
                             hints=hints)
            return self.default

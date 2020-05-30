from typing_inspect import is_literal_type, get_args
from abc import ABCMeta, abstractmethod
from .types import HintName, UntypedRecordsHints
from typing import TypeVar, Generic, Type, List, TYPE_CHECKING
if TYPE_CHECKING:
    from records_mover.utils.json_schema import JsonSchemaDocument


HintT = TypeVar('HintT')


class Hint(Generic[HintT], metaclass=ABCMeta):
    def __init__(self,
                 hint_name: HintName,
                 default: HintT,
                 description: str) -> None:
        self.default = default
        self.hint_name = hint_name
        self.description = description

    @abstractmethod
    def validate(self,
                 hints: UntypedRecordsHints,
                 fail_if_cant_handle_hint: bool) -> HintT:
        ...

    @abstractmethod
    def json_schema_document(self) -> 'JsonSchemaDocument':
        ...


class StringHint(Hint[str]):
    def json_schema_document(self) -> 'JsonSchemaDocument':
        from records_mover.utils.json_schema import JsonSchemaDocument

        return JsonSchemaDocument('string',
                                  description=self.description)

    def validate(self,
                 hints: UntypedRecordsHints,
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
                 default: LiteralHintT,
                 description: str) -> None:
        assert is_literal_type(type_), f"{hint_name} is not a Literal[]"
        self.type_ = type_
        self.valid_values: List[LiteralHintT] = list(get_args(type_))
        super().__init__(hint_name=hint_name,
                         default=default,
                         description=description)

    def json_schema_document(self) -> 'JsonSchemaDocument':
        from records_mover.utils.json_schema import JsonSchemaDocument

        json_schema_types = {
            bool: 'boolean',
            str: 'string',
            # Even though Python prints the word NoneType in many
            # error messages, NoneType is not an identifier in
            # Python. It’s not in builtins. You can only reach it with
            # type(None).
            #
            # https://realpython.com/null-in-python/
            type(None): 'null',
        }

        types_set = {
            json_schema_types[type(valid_value)]
            for valid_value in self.valid_values
        }

        if all([t == 'boolean' for t in types_set]):
            # We use Literal[True, False] as a type instead of bool as
            # mypy's exhaustive type matching doesn't work with 'bool'.
            #
            # Let's translate that back to boolean here.
            return JsonSchemaDocument('boolean',
                                      description=self.description)

            pass
        else:
            return JsonSchemaDocument(list(types_set),
                                      enum=self.valid_values,
                                      description=self.description)

    def validate(self,
                 hints: UntypedRecordsHints,
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

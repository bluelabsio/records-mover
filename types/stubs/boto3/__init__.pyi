from typing import Any
from . import session as session  # noqa

# There's some hope of some more comprehensive types here - would be
# good to check in periodically and see if we can replace this with
# that: https://github.com/boto/boto3/issues/1055

client: Any = ...

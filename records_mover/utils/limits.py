from enum import Enum
import math
from typing import Optional

INT8_MAX = 127
INT8_MIN = -128
UINT8_MAX = 255
UINT8_MIN = 0
INT16_MAX = 32767
INT16_MIN = -32768
UINT16_MAX = 65535
UINT16_MIN = 0
INT24_MAX = 8388607
INT24_MIN = -8388608
UINT24_MAX = 16777215
UINT24_MIN = 0
INT32_MAX = 2147483647
INT32_MIN = -2147483648
UINT32_MAX = 4294967295
UINT32_MIN = 0
INT64_MAX = 9223372036854775807
INT64_MIN = -9223372036854775808
UINT64_MAX = 18446744073709551615
UINT64_MIN = 0
FLOAT16_SIGNIFICAND_BITS = 11
FLOAT32_SIGNIFICAND_BITS = 23
FLOAT64_SIGNIFICAND_BITS = 53
FLOAT80_SIGNIFICAND_BITS = 64


class IntegerType(Enum):
    INT8 = (INT8_MIN, INT8_MAX)
    UINT8 = (UINT8_MIN, UINT8_MAX)
    INT16 = (INT16_MIN, INT16_MAX)
    UINT16 = (UINT16_MIN, UINT16_MAX)
    INT24 = (INT24_MIN, INT24_MAX)
    UINT24 = (UINT24_MIN, UINT24_MAX)
    INT32 = (INT32_MIN, INT32_MAX)
    UINT32 = (UINT32_MIN, UINT32_MAX)
    INT64 = (INT64_MIN, INT64_MAX)
    UINT64 = (UINT64_MIN, UINT64_MAX)

    def __init__(self, min_: int, max_: int):
        self.min_ = min_
        self.max_ = max_

    def is_cover_for(self, low_value: int, high_value: int) -> bool:
        return low_value >= self.min_ and high_value <= self.max_

    @property
    def range(self):
        return (self.min_, self.max_)

    @classmethod
    def smallest_cover_for(cls, low_value: int, high_value: int) -> Optional['IntegerType']:
        for int_type in cls:
            if int_type.is_cover_for(low_value, high_value):
                return int_type
        return None


# https://stackoverflow.com/questions/2189800/length-of-an-integer-in-python
def num_digits(n: int) -> int:
    if n > 0:
        digits = int(math.log10(n)) + 1
    elif n == 0:
        digits = 1
    else:
        digits = int(math.log10(-n)) + 2  # +1 if you don't count the '-'
    return digits

import math

INT8_MAX = 127
INT8_MIN = -128
UINT8_MAX = 255
UINT8_MIN = 0
INT16_MAX = 32767
INT16_MIN = -32768
UINT16_MAX = 65535
UINT16_MIN = 0
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


# https://stackoverflow.com/questions/2189800/length-of-an-integer-in-python
def num_digits(n: int) -> int:
    if n > 0:
        digits = int(math.log10(n)) + 1
    elif n == 0:
        digits = 1
    else:
        digits = int(math.log10(-n)) + 2  # +1 if you don't count the '-'
    return digits

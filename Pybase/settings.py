"""
Here are all settings number for Pybase

Date: 2019/10/27
"""

# page settings
PAGE_SIZE = 8192

# PAGE_INT_SIZE = 2048

PAGE_SIZE_BITS = 13

# MAX_FMT_INT_NUM = 128

# MAX_FILE_NUM = 128

# cache settings

CACHE_CAPACITY = 60000

HASH_MOD = 60000

# file settings

FILE_ID_BITS = 16

ID_DEFAULT_VALUE = -1

# table settings

MAX_COLUMNS = 31

MAX_TABLES = 31

# file header settings

HEADER_LENGTH_BYTES = 4

# record settings

RECORD_PAGE_FIXED_HEADER_SIZE = 5

RECORD_PAGE_NEXT_OFFSET = 1

HEADER_PAGE_ID = 0

PAGE_FLAG_OFFSET = 0

RECORD_PAGE_FLAG = 0

VARCHAR_PAGE_FLAG = 1

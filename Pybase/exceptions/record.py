"""
Here define exceptions for record system

Date: 2019/10/27
"""
from .base import Error


class RecordTooLong(Error):
    """
    This will be raised when a table's column is too long
    to save a record into a single page
    """


class PageOverflow(Error):
    """
    This will be raised when a page's data, usually means header page,
    is beyond size of a page
    """

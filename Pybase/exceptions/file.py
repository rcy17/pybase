"""
Here define exceptions for file system

Date: 2019/10/21
"""
from .base import Error


class OpenFileFailed(Error):
    """
    This will be raised when failed to open a file by filename
    """


class ReadFileFailed(Error):
    """
    This will be raised when failed to read a page from file
    """


class ExcutorFileError(Error):
    """
    This will be raised when FileExecutor meet logic error
    """

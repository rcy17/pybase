"""
Here define exceptions for file system

Date: 2019/10/21
"""


class Error(Exception):
    """Base class for exceptions in file system"""


class OpenFileFailed(Error):
    """
    This will be raised when failed to open a file by filename
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


class ReadFileFailed(Error):
    """
    This will be raised when failed to read a page from file
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

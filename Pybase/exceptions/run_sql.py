"""
Here define exceptions when run sql

Date: 2020/11/30
"""
from .base import Error


class DataBaseError(Error):
    """
    Data Base Error for db sql
    """


class ConstraintError(Error):
    """
    Constraint Error when trying to modify database
    """

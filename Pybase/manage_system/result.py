"""
Here defines QueryResult class, each sql query returns such object

Date: 2020/12/03
"""
from datetime import timedelta


class QueryResult:
    """
    This is SQL query's result
    headers are supposed to be tuple of string
    data are supposed to be tuple of tuple of string
    """
    def __init__(self, headers=None, data=None, message=None, change_db=None, cost=None):
        if not isinstance(headers, (list, tuple)):
            headers = (headers, )
        if data and not isinstance(data[0], (list, tuple)):
            data = tuple((each, ) for each in data)
        self._headers = headers
        self._data = data
        self._header_index = {h:i for i,h in enumerate(headers)}
        self._alias_map = {}
        self._message = message
        self._database = change_db
        self._cost = cost

    @property
    def headers(self) -> tuple:
        return self._headers

    @property
    def data(self) -> tuple:
        return self._data
    
    @property
    def alias_map(self) -> dict:
        return self._alias_map

    @property
    def message(self) -> str:
        return self._message

    @property
    def database(self) -> str:
        return self._database

    @property
    def cost(self) -> timedelta:
        return self._cost

    @cost.setter
    def cost(self, value: timedelta):
        self._cost = value
    
    def get_size(self) -> int:
        return len(self._data)
    
    def get_header_index(self, header) -> int:
        if header in self._alias_map:
            header = self._alias_map[header]
        if header in self._header_index:
            return self._header_index[header]

    def add_alias(self, alias, header):
        self._alias_map[alias] = header

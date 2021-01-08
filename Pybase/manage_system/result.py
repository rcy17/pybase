"""
Here defines QueryResult class, each sql query returns such object

Date: 2020/12/03
"""
from Pybase import settings
from datetime import timedelta


class QueryResult:
    """
    This is SQL query's result
    headers are supposed to be tuple of string
    data are supposed to be tuple of tuple of string
    """
    def __init__(self, headers=None, data=None, message=None, change_db=None, cost=None):
        if headers and not isinstance(headers, (list, tuple)):
            headers = (headers, )
        if data and not isinstance(data[0], (list, tuple)):
            data = tuple((each, ) for each in data)
        self._headers = headers
        self._data = data
        self._header_index = {h:i for i,h in enumerate(headers)} if headers else {}
        self._alias_map = {}
        self._message = message
        self._database = change_db
        self._cost = cost

    def simplify(self):
        """Simplify headers if all headers have same prefix"""
        if not self._headers:
            return
        header: str = self._headers[0]
        if header.find('.') < 0:
            return
        prefix = header[:header.find('.') + 1]  # Prefix contains "."
        for header in self._headers:
            if len(header) <= len(prefix) or not header.startswith(prefix):
                break
        else:
            self._headers = tuple(header[len(prefix):] for header in self._headers)

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
    
    def deal_null(self):
        for row in self.data:
            for i in range(len(row)):
                if row[i] == settings.NULL_VALUE:
                    row[i] = None
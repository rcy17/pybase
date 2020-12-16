"""
Here defines QueryResult class, each sql query returns such object

Date: 2020/12/03
"""


class QueryResult:
    """
    This is SQL query's result
    headers are supposed to be tuple of string
    data are supposed to be tuple of tuple of string
    """
    def __init__(self, headers, data):
        if not isinstance(headers, (list, tuple)):
            headers = (headers, )
        if data and not isinstance(data[0], (list, tuple)):
            data = ((each, ) for each in data)
        self._headers = headers
        self._data = data
        self._header_index = {h:i for i,h in enumerate(headers)}

    @property
    def headers(self) -> tuple:
        return self._headers

    @property
    def data(self) -> tuple:
        return self._data
    
    def get_header_index(self, header) -> int:
        if header in self._header_index:
            return self._header_index[header]
        else:
            return None

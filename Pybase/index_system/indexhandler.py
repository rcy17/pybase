"""
Here defines the index handler

Data: 2020/12/01
"""

from Pybase.file_system import FileManager

class IndexHandler:
    '''
    Class to treat a file as index
    '''
    def __init__(self, manager: FileManager) -> None:
        self._manager = manager
        


    
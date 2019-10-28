"""
Here defines Record

Data: 2019/10/24
"""
import numpy as np

from .rid import RID


class Record:
    """
    Class for records
    """

    def __init__(self, rid: RID, data: np.ndarray):
        self._rid = rid
        self._data = data

    @property
    def rid(self) -> RID:
        return self._rid

    @property
    def data(self) -> np.ndarray:
        return self._data

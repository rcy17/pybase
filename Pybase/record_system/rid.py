"""
Here defines RID

Data: 2019/10/24
"""


class RID:
    """
    RID is for record identifier, which is composed of page number and slot number
    """
    def __init__(self, page_number, slot_number):
        self._page = page_number
        self._slot = slot_number

    @property
    def page(self):
        return self._page

    @property
    def slot(self):
        return self._slot

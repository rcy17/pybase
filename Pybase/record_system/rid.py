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

    def __str__(self):
        return f'{{page_id: {self.page_id}, slot_id: {self.slot_id}}}'

    @property
    def page_id(self):
        """Number in [0, page_number)"""
        return self._page

    @property
    def slot_id(self):
        """Number in [0, record_per_page)"""
        return self._slot

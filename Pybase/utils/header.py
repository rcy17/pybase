"""
Here are some functions about header

Data:  2019/10/29
"""
import numpy as np

from Pybase import settings
from Pybase.exceptions.record import PageOverflowError
from .header_pb2 import HeaderInfo


def header_serialize(header: HeaderInfo) -> np.ndarray:
    data = header.SerializeToString()
    data_length = len(data)
    bias = settings.HEADER_LENGTH_BYTES
    if data_length + bias > settings.PAGE_SIZE:
        raise PageOverflowError(f'Page with {data_length + bias} bytes is beyond page size {settings.PAGE_SIZE}')
    page = np.zeros(settings.PAGE_SIZE, dtype=np.uint8)
    page[:bias] = np.frombuffer(bias.to_bytes(bias, 'big'), np.uint8)
    page[bias: bias + data_length] = np.frombuffer(data, np.uint8)
    return page


def header_deserialize(data: np.ndarray) -> HeaderInfo:
    # TODO: seems that deserialize has some bug compared to serializer
    bias = settings.HEADER_LENGTH_BYTES
    length = int.from_bytes(data[:bias].tobytes(), 'big')
    return HeaderInfo.FromString(data[bias: bias + length].tobytes())

"""
"""

from Pybase.index_system.fileindex import FileIndex
from Pybase.record_system.filescan import FileScan
from Pybase.record_system.filehandle import FileHandle
from Pybase.record_system.rid import RID

def search_by_index(fileindex: FileIndex, handle: FileHandle, lower:int, upper:int):
    rids = fileindex.range(lower, upper)
    records = [handle.get_record(rid)._data for rid in rids]
    return records

def search_by_filter(scan: FileScan, col_ids, col_ranges):
    assert len(col_ids) == len(col_ranges)
    for i in col_ranges:
        assert len(i) == 2
    limit_size = len(col_ids)
    records = []    
    for record in scan:
        matched = True
        for i in range(limit_size):
            col_id = col_ids[i]
            col_range = col_ranges[i]
            if col_range[0] <= record[col_id] < col_range[1]:
                continue
            else:
                matched = False
                break
        if matched:
            records.append(record._data)
    return records            
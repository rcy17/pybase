"""
"""

from Pybase.manage_system.result import QueryResult
import numpy as np


def nested_loops_join_data(outer: list, inner: list, outer_joined:list, inner_joined:list):
    assert len(outer_joined) == len(inner_joined)
    outer_left = [i for i in range(len(outer[0])) if i not in outer_joined]
    inner_left = [i for i in range(len(inner[0])) if i not in inner_joined]
    result = []
    def build_join_vals():
        join_vals = {}
        row_id = 0
        for row in outer:
            if join_vals.get(row[outer_joined]) is None:
                join_vals[row[outer_joined]] = [row_id]
            else:
                join_vals[row[outer_joined]].append(row_id)
            row_id += 1
    join_vals : dict = build_join_vals()
    for inner_id in range(len(inner)):
        if inner[inner_id][inner_joined] in join_vals:
            outer_list = join_vals[inner[inner_id][inner_joined]]
            for outer_id in outer_list:
                res_row = outer[outer_id][outer_left] + inner[inner_id][inner_left] + outer[outer_id][outer_joined]
                result.append(res_row)
    return result, outer_left, inner_left


def nested_loops_join(outer:QueryResult, inner:QueryResult, outer_joined:list, inner_joined:list) -> QueryResult:
    outer_data = outer.data
    inner_data = inner.data
    outer_head = outer.headers
    inner_head = inner.headers
    outer_joined_index = [outer.get_header_index(header) for header in outer_joined]
    inner_joined_index = [inner.get_header_index(header) for header in inner_joined]
    joined_data, outer_left, inner_left = nested_loops_join_data(outer_data, inner_data, outer_joined_index, inner_joined_index)
    result = QueryResult(outer_head[outer_left] + inner_head[inner_left] + outer_joined, joined_data)
    for outer_header, inner_header in zip(outer_joined, inner_joined):
        result.add_alias(inner_header, outer_header)
    for alias in outer.alais_map:
        result.add_alias(alias, outer.alais_map[alias])
    for alias in inner.alais_map:
        result.add_alias(alias, inner.alais_map[alias])
    return result
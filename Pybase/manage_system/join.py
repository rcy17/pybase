"""
"""

import numpy as np


def nested_loops_join(outer: np.ndarray, inner: np.ndarray, outer_joined:list, inner_joined:list):
    assert len(outer_joined) == len(inner_joined)
    outer_left = [i for i in range(outer.shape[1]) if i not in outer_joined]
    inner_left = [i for i in range(inner.shape[1]) if i not in inner_joined]
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
    for inner_id in range(inner.shape[0]):
        if join_vals.get(inner[inner_id][inner_joined]) is not None:
            outer_list = join_vals.get(inner[inner_id][inner_joined])
            for outer_id in outer_list:
                res_row = outer[outer_id][outer_left] + inner[inner_id][inner_left] + outer[outer_id][outer_joined]
                result.append(res_row)
    return result

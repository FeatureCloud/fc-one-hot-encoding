# The following code is a derivative work of the code from the scikit-survival package by S. Pölsterl,
# which is licensed under the GPL-3.0 License.
# This code therefore is also licensed under the terms of the GNU General Public License, version 3.

from typing import Dict, Set, List, Union

import numpy
import pandas
from pandas.api.types import is_categorical_dtype


def _get_mat(data, column_levels: List[Union[str, int]]):
    cat = pandas.Categorical(data, categories=column_levels)

    dummy_mat = numpy.eye(len(column_levels)).take(cat.codes, axis=0)

    # reset NaN GH4446
    dummy_mat[cat.codes == -1] = numpy.nan

    return dummy_mat


def _encode_categorical_series(series, levels: Dict[str, Set[Union[str, int]]]):
    levels_for_series: List[Union[str, int]] = sorted(levels[series.name])
    enc = _get_mat(series, levels_for_series)
    if enc is None:
        return

    if enc is None:
        return pandas.Series(index=series.index, name=series.name, dtype=series.dtype)

    if enc.shape[1] == 1:
        return series

    names = []
    for key in range(1, enc.shape[1]):
        names.append("{}={}".format(series.name, levels_for_series[key]))
    series = pandas.DataFrame(enc[:, 1:], columns=names, index=series.index)

    return series


def is_categorical_or_object(series):
    return is_categorical_dtype(series.dtype) or series.dtype.char == "O"


def encode_categorical(table, levels: Dict[str, Set[Union[str, int]]]):
    if isinstance(table, pandas.Series):
        if not is_categorical_dtype(table.dtype) and not table.dtype.char == "O":
            raise TypeError("series must be of categorical dtype, but was {}".format(table.dtype))
        return _encode_categorical_series(table, levels)

    columns_to_encode = set(levels.keys())

    items = []
    for name, series in table.iteritems():
        if name in columns_to_encode:
            series = _encode_categorical_series(series, levels)
            if series is None:
                continue
        items.append(series)

    # concat columns of tables
    new_table = pandas.concat(items, axis=1, copy=False)
    return new_table


def get_columns_to_encode(table: pandas.DataFrame):
    columns_to_encode = {nam for nam, s in table.iteritems() if is_categorical_or_object(s)}
    return columns_to_encode

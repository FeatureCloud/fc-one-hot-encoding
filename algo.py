from collections import defaultdict

import pandas as pd

from encode import *
import pandas


def set_as_categorical(table: pandas.DataFrame, column_label: str):
    table[column_label] = pd.Categorical(table[column_label])
    return table


def drop_rows_with_introduced_na_values(original_table: pandas.DataFrame, encoded_table: pandas.DataFrame):
    # get labels that changed
    original_labels = set(original_table.columns.values)
    new_labels = set(encoded_table.columns.values)
    diff_labels = new_labels.difference(original_labels)

    isna = encoded_table.isna()  # all NA values
    relevant_isna = isna[diff_labels].copy()  # NA values in newly introduced columns

    # ignore NA values that already were an NA in the original data
    for name, series in relevant_isna.iteritems():
        original_col_name = ''.join(name.split('=')[:-1])
        original_isna = original_table[original_col_name].isna()

        for i, orig in enumerate(original_isna):
            if orig:
                relevant_isna[name][i] = False

    relevant_rows_with_new_na_values = relevant_isna.any(axis=1)
    drop_indices = encoded_table.index[relevant_rows_with_new_na_values]
    return encoded_table.drop(drop_indices)


def get_categories(table: pandas.DataFrame):
    discrete_columns = get_columns_to_encode(table)

    summary = {}

    for col_name in discrete_columns:
        series = table[col_name]
        categories: pandas.Index = pandas.Categorical(series).categories
        summary[col_name] = set(categories.array)

    return summary


def combine(data: List[Dict[str, Set[Union[str, int]]]]):
    categorical_levels = defaultdict(set)

    print(data)
    for local_data in data:
        print(local_data)
        for column_name, column_values in local_data.items():
            categorical_levels[column_name].update(column_values)

    return categorical_levels

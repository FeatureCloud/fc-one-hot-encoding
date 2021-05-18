from collections import defaultdict
from .encode import *
import pandas


def get_categories(table: pandas.DataFrame):
    discrete_columns = get_columns_to_encode(table)

    summary = {}

    for col_name in discrete_columns:
        series = table[col_name]
        categories: pandas.Index = pandas.Categorical(series).categories
        summary[col_name] = set(categories.array)

    return summary


def combine(data: List[Dict[str, Set[str]]]):
    categorical_levels = defaultdict(set)

    print(data)
    for local_data in data:
        print(local_data)
        for column_name, column_values in local_data.items():
            categorical_levels[column_name].update(column_values)

    return categorical_levels

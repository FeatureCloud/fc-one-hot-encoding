from unittest import TestCase

import pandas
import pandas as pd

from app.algo import get_categories, set_as_categorical, drop_rows_with_introduced_na_values
from app.encode import encode_categorical, get_columns_to_encode


class TestEncodeCategorical(TestCase):
    def setUp(self) -> None:
        self.df = pandas.DataFrame(
            {
                'a': [0, 1, 2, 0],
                'b': ['high', 'low', 'mid', 'low'],
                'c': [2.85, 12.5, 0.25, -0.35],
            }
        )

    def test_column_names(self):
        encoded = encode_categorical(self.df, {'a': {0, 1, 2}, 'b': {'low', 'high', 'mid'}})
        self.assertListEqual(['a=1', 'a=2', 'b=low', 'b=mid', 'c'], list(encoded.columns.values))

    def test_missing_value_in_levels_results_in_na(self):
        encoded = encode_categorical(self.df, {'a': {0, 2}, 'b': {'low', 'high'}})  # category 1 and high are missing
        self.assertTrue(encoded['a=2'].isna().any())
        self.assertTrue(encoded['b=low'].isna().any())
        self.assertFalse(encoded['c'].isna().any())

    def test_drop_rows_with_introduced_na_values(self):
        df = pandas.DataFrame(
            {
                'a': [0, 1, 2, 0, pd.NA],
                'b': ['high', 'low', 'mid', 'low', 'low'],
                'c': [pd.NA, 12.5, 0.25, -0.35, 3.10],
            }
        )
        encoded = encode_categorical(df, {'a': {0, 2}, 'b': {'low', 'high'}})  # category 1 and high are missing
        filtered = drop_rows_with_introduced_na_values(df, encoded)

        self.assertListEqual([0, 3, 4], list(filtered.index.values))
        self.assertTrue(filtered.isna()['a=2'].iloc[2])
        self.assertTrue(filtered.isna()['c'].iloc[0])

    def test_columns_to_encode(self):
        columns_to_encode = get_columns_to_encode(self.df)
        self.assertSetEqual({'b'}, columns_to_encode)
        self.assertDictEqual({'b': {'low', 'high', 'mid'}}, get_categories(self.df))

    def test_set_as_categorical(self):
        set_as_categorical(self.df, 'a')  # register column 'a' as a categorical column
        columns_to_encode = get_columns_to_encode(self.df)
        self.assertSetEqual({'a', 'b'}, columns_to_encode)
        self.assertDictEqual({'a': {0, 1, 2}, 'b': {'low', 'high', 'mid'}}, get_categories(self.df))

from typing import Collection

import pandas as pd

from dirlin.dq.core import DataSource, ResultWrapper
from dirlin.dq.pipelines import Pipeline
from test.helpers import (
    matches_df_columns,
    matches_df_with_aliases_scalar,
    matches_df_with_aliases_series,
    scalar_fn,
    series_fn,
    example_df,
    example_df_factory, df_foo_factory
)


class TestFooBar(Pipeline):
    alias_mapping = {
        "record": ["Recordo", "Recarda"],
        "that": ["After That", "All That"],
        "flow": "fle"
    }

    # FUNCTIONS FOR CHECKS AND VALIDATIONS
    _matches_df_columns = matches_df_columns
    _matches_df_with_aliases_scalar = matches_df_with_aliases_scalar
    _matches_df_with_aliases_series = matches_df_with_aliases_series
    _scalar_fn = scalar_fn
    _series_fn = series_fn

    # FUNCTION FOR DATASOURCE FACTORY, THAT THE NEW PIPELINE SUPPORTS
    _fn_factory = example_df_factory
    _fn_factory_2 = df_foo_factory

    # data source properties
    _standard_ds = DataSource("basic_test", example_df)


def test_define_pipeline_object():

    # CALLING THE NEW PIPELINE SUBCLASS
    foo = TestFooBar()

    # CHECKING TO SEE IF THE FACTORY WORKED
    assert isinstance(foo.data_sources, Collection)

    # CHECKING TO SEE THAT THE FUNCTIONS WERE FOUND AND DEFINED
    assert isinstance(foo.checks, Collection)


def test_running_pipeline():
    foo = TestFooBar()

    # CURRENTLY WORKING ON THIS PART [2025.08.29]
    results = foo.run()
    assert isinstance(results, ResultWrapper)
    assert isinstance(results.as_dict(), dict)
    assert isinstance(results.as_dataframe(), pd.DataFrame)
    assert isinstance(results.as_dataframe(dtype="dict"), pd.DataFrame)
    assert isinstance(results.as_dataframe(dtype="message"), pd.DataFrame)

    print(results.as_dataframe())
    print(results.as_dataframe(dtype="dict"))
    print(results.as_dataframe(dtype="message"))
    print(results.as_dataframe(dtype="summary"))


def test_subclass_pipeline():
    foo = TestFooBar()
    Pipeline.run_subclass()


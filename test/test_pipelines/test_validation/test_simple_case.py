import pandas as pd
import pytest

from dirlin import BaseValidation


def example_dataframe_factory() -> pd.DataFrame:
    mapper = {
        "A": [1, 2, 3],
        "B": [9, 8, 7],
        "C": ["Sho", "Sanz", "Manz"]
    }
    return pd.DataFrame.from_dict(mapper)


# TESTS
# [ x ] SingleLayerObject with Alias Mapping already defined as a map (WORKS)
# [ x ] Pd.Series type of return
# (3) Returns where the param type and return type are different
# (4) List of Alias Mapping

# UPDATES
# [ ] a param or flag to determine the return we want on `run_validation` (dict, pd.Dataframe, transposed pd.DF)
# [ x ] verify the return types and param types matching - currently we look at params but return might be important


def bad_example_test_function(a: int, b: float | int, c: str):
    """bad function because this doesn't have a return type

    """
    return f"Hello {c}, you have {a + b}!"


def example_test_function(a: int, b: float | int, c: str) -> str:
    """has all the basic components that it needs

    """
    return f"Hello {c}, you have {a + b}!"


class BadSingleLayerObject(BaseValidation):
    example_test = bad_example_test_function


class SimpleSingleLayerObject(BaseValidation):
    example_test = example_test_function

    alias_mapping = {
        "a": ["A"],
        "b": ["B"],
        "c": ["C"]
    }


def example_series_function(a: pd.Series, b: pd.Series, c: pd.Series) -> pd.Series:
    return pd.Series(a * b * c)


class BadSeriesLayerObject(BaseValidation):
    example_test_series = example_series_function

    alias_mapping = {
        "a": ["A"],
        "b": ["B"],
        "c": ["C"]
    }

    example_test2 = example_test_function


def test_basic_one_function():
    # (1) We have the class that inherited from BaseValidation
    single_layer = BadSingleLayerObject()
    assert single_layer

    # (2) We have the example dataframe we want to check
    df = example_dataframe_factory()
    assert isinstance(df, pd.DataFrame)

    # (3) Because we have `column names` that don't match with
    # the name of the parameters, we should get a specific
    # KeyError raised (column names A, B, C don't directly match with a, b, c)
    with pytest.raises(KeyError) as KE:
        single_layer._run_validation(df=df)
        assert KE.match("Missing columns")

    # (4) Since we identified columns that don't match with parameters,
    # we follow the error message, and add in the alias mapping names here directly
    single_layer.alias_mapping["a"] = ["A", ]
    single_layer.alias_mapping["b"] = ["B", ]
    single_layer.alias_mapping["c"] = ["C", ]

    # (5) Because our BadSingleLayerObject's function does not have a
    # return type, we should also error out on that validation
    with pytest.raises(ValueError) as VE:
        single_layer._run_validation(df=df)
        assert VE.match("is missing a return type")

    # (6) we rerun the validation and don't get an error this time
    single_layer = SimpleSingleLayerObject()
    result = single_layer._run_validation(df=df)
    assert isinstance(result, dict)
    print()
    print(result)


def test_series_based_functions():
    """Looks like this works even on functions that uses a pd.Series type

    """
    validation = BadSeriesLayerObject()
    p = validation._run_validation(df=example_dataframe_factory())
    print(p)  # we get a {check_name: pd.Series}

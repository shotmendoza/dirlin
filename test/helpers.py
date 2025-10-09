import pandas as pd


def matches_df_columns(fee: str, flow: str, row: str) -> str:
    """Check 1 name is okay
    """
    return f"{fee} what {flow} where {row}"


def matches_df_with_aliases_scalar(record: str, flow: str, that: str) -> str:
    """parameters are based on the alias_mapping"""
    return f"{record} what {flow} where {that}"


def matches_df_with_aliases_series(record: pd.Series, that: pd.Series, foo: pd.Series) -> pd.Series:
    """parameters are based on the alias_mapping"""
    return pd.Series([f"{rec} what {fo} whoa {at}" for rec, fo, at in zip(record, foo, that)])


def scalar_fn(foof: int, fle: str) -> str:
    """uses scalar types as parameters"""
    return f"{foof} what {fle}"


def series_fn(fle: pd.Series, foo: pd.Series) -> pd.Series:
    """uses series types as parameters
    """
    return pd.Series([f"{fl} what {fo}" for fl, fo in zip(fle, foo)])


def check_scalar_bool(foo: int) -> bool:
    """checking for a scalar bool function"""
    return foo > 3


def check_series_bool(foo: pd.Series) -> pd.Series:
    """check for pd.Series bool function"""
    return pd.Series(foo > 3)


def check_scalar_bool_parent(foo: int) -> bool:
    """checking for a scalar bool function"""
    return foo > 3


def check_series_bool_parent(foo: pd.Series) -> pd.Series:
    """check for pd.Series bool function"""
    return pd.Series(foo > 3)


def check_scalar_bool_child(foo: int) -> bool:
    """checking for a scalar bool function (child)"""
    return foo > 3


def check_series_bool_child(foo: pd.Series) -> pd.Series:
    """check for pd.Series bool function (child)"""
    return pd.Series(foo > 3)


def check_scalar_bool_shared(shared: int) -> bool:
    """checking for a scalar bool function (child)"""
    return shared > 30


def check_series_bool_shared(shared: pd.Series) -> pd.Series:
    """check for pd.Series bool function (child)"""
    return pd.Series(shared > 30)


example_df = pd.DataFrame.from_dict(
    {
        "Idx": [1, 2, 3, 4, 5],
        "Recordo": ["Row 1o", "Row 2o", "Row 3o", "Row 4o", "Row 5o"],
        "Recarda": ["Row 1a", "Row 2a", "Row 3a", "Row 4a", "Row 5a"],
        "After That": ["Row 1", "Row 2", "Row 3", "Row 4", "Row 5"],
        "All That": ["Row 1", "Row 2", "Row 3", "Row 4", "Row 5"],
        "foof": ["Row 1", "Row 2", "Row 3", "Row 4", "Row 5"],
        "fle": ["Row 1", "Row 2", "Row 3", "Row 4", "Row 5"],
        "foo": [1, 2, 3, 4, 5],
    }
)

parent_df = pd.DataFrame.from_dict(
    {
        "Idx": [1, 2, 3, 4, 5],
        "shared": [10, 20, 30, 40, 50],
        "Recarda": ["Row 1a", "Row 2a", "Row 3a", "Row 4a", "Row 5a"],
        "After That": ["Row 1", "Row 2", "Row 3", "Row 4", "Row 5"],
        "All That": ["Row 1", "Row 2", "Row 3", "Row 4", "Row 5"],
        "foof": ["Row 1", "Row 2", "Row 3", "Row 4", "Row 5"],
        "fle": ["Row 1", "Row 2", "Row 3", "Row 4", "Row 5"],
        "foo": [1, 2, 3, 4, 5],
    }
)

child_df = pd.DataFrame.from_dict(
    {
        "Idx": [1, 2, 3, 4, 5],
        "shared": [10, 20, 30, 40, 50],
        "Recarda": ["Row 1a", "Row 2a", "Row 3a", "Row 4a", "Row 5a"],
        "After That": ["Row 1", "Row 2", "Row 3", "Row 4", "Row 5"],
        "All That": ["Row 1", "Row 2", "Row 3", "Row 4", "Row 5"],
        "foof": ["Row 1", "Row 2", "Row 3", "Row 4", "Row 5"],
        "fle": ["Row 1", "Row 2", "Row 3", "Row 4", "Row 5"],
        "foobar": [1, 2, 3, 4, 5],
    }
)


def example_df_factory() -> pd.DataFrame:
    return example_df


def df_foo_factory() -> pd.DataFrame:
    return example_df


def df_child_factory() -> pd.DataFrame:
    return child_df


def df_parent_factory() -> pd.DataFrame:
    return parent_df

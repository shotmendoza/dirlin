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


example_df = pd.DataFrame.from_dict(
    {
        "Idx": [1, 2, 3, 4, 5],
        "Recordo": ["Row 1o", "Row 2o", "Row 3o", "Row 4o", "Row 5o"],
        "Recarda": ["Row 1a", "Row 2a", "Row 3a", "Row 4a", "Row 5a"],
        "After That": ["Row 1", "Row 2", "Row 3", "Row 4", "Row 5"],
        "All That": ["Row 1", "Row 2", "Row 3", "Row 4", "Row 5"],
        "foof": ["Row 1", "Row 2", "Row 3", "Row 4", "Row 5"],
        "fle": ["Row 1", "Row 2", "Row 3", "Row 4", "Row 5"],
        "foo": ["Foo 1", "Foo 2", "Foo 3", "Foo 4", "Foo 5"],
    }
)


def example_df_factory() -> pd.DataFrame:
    return example_df


def df_foo_factory() -> pd.DataFrame:
    return example_df

import pandas as pd

from dirlin.validation import Pipeline, DataSource


def test_movement_of_df():
    df = pd.DataFrame.from_dict(
        {
            "Record": [1, 2, 3, 4, 5],
            "Current": ["Start", "Middle", "End", "Restart", "Middle"],
        }
    )

    df["Previous"] = df["Current"].shift(1)
    df["After"] = df["Current"].shift(-1)

    # Break it down:
    # 1. Current == Start
    # 2. If After == Start or None then Error

    # 3. Current == End
    # 4. If After != Restart OR Middle (Special) then Error

    print()
    print(df)


def test_validate_two_oh():
    def bar(fee: str, flow: str, row: str):
        """Check 1 name is okay
        """
        return f"{fee} what {flow} where {row}"

    def lebar(foof: int, fle: str) -> str:
        """Check 2 address is good
        """
        return f"{foof} what {fle}"

    def as_series_param(fee: pd.Series, foo: pd.Series):
        """Check 3 fee is correct
        """
        return fee, foo

    class FooBar(Pipeline):
        alias_mapping = {
            "fee": "After",
            "flow": ["More After", "After That"],
            "row": ["Record", "fle"],
            "fle": "Current"
        }

        _bar = bar
        _lebar = lebar
        _as_series_param = as_series_param

        def __init__(self):
            super().__init__()

    df = pd.DataFrame.from_dict(
        {
            "Record": [1, 2, 3, 4, 5],
            "Current": ["Row 1", "Row 2", "Row 3", "Row 4", "Row 5"],
            "After": ["Row 1", "Row 2", "Row 3", "Row 4", "Row 5"],
            "After That": ["Row 1", "Row 2", "Row 3", "Row 4", "Row 5"],
            "More After": ["Row 1", "Row 2", "Row 3", "Row 4", "Row 5"],
            "foof": ["Row 1", "Row 2", "Row 3", "Row 4", "Row 5"],
            "fle": ["Row 1", "Row 2", "Row 3", "Row 4", "Row 5"],
            "foo": ["Foo 1", "Foo 2", "Foo 3", "Foo 4", "Foo 5"],
        }
    )

    data = DataSource(
        "basic_test",
        df,
        alias_mapping={
            "fee": "After",
            "flow": ["More After", "After That"],
            "row": ["Record", "fle"],
            "fle": "Current"
        }
    )

    vf = FooBar()
    vf.load_data_sources(sources=data)
    vf.run()

    # print(vf.signatures)
    # t = vf._bind_signature_records(df)
    # print(t)

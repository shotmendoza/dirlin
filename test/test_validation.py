import pandas as pd


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

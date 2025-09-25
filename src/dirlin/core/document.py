import math
from datetime import date
from pathlib import Path
from typing import Hashable, Sequence, Any

import pandas as pd


class Document:
    def __init__(self, df: pd.DataFrame, path: Path):
        """wrapper around a Dataframe object so that we can format it with a higher context

        :param df: the dataframe we are working with
        :param path: of the dataframe object
        """
        self.dataframe: pd.DataFrame = df
        self.filepath: Path = path

    def check_columns(self, headers: list[str], match_all: bool = True, raise_error: bool = True) -> bool:
        """
        Checks the dataframe to confirm that expected columns are in the data.

        :param headers: the headers to check for
        :param match_all: defaults to confirm every column is in data. When False, confirms if any columns is in data
        :param raise_error: defaults to raising error for missing column. When False, only returns bool
        :return: True if all columns are present. False if any or all columns are missing (param dependent)
        """
        if match_all is True:
            if any([h in self.dataframe.columns for h in headers]) is False:
                if raise_error:
                    raise KeyError(f"Missing on or all expected columns {headers} in {self.filepath.stem} file.")
                return False
            return True

        if any([h in self.dataframe.columns for h in headers]) is True:
            return True
        else:
            if raise_error:
                raise KeyError(f"Missing all expected columns {headers} in {self.filepath.stem} file.")
            return False

    def move_file(self, destination: Path):
        """
        Moves the current file you are working with to another folder or destination.

        :param destination: the full path (including filename) of the Path object
        :return: the new path (also changes state of object in self.filepath)
        """
        try:
            self.filepath = self.filepath.rename(target=destination)
        except Exception as e:
            raise e
        return self.filepath

    def chunk(
            self,
            chunk_size: int = 10000,
            filename_prefix: str | None = None,
            write: bool = False
    ) -> list[pd.DataFrame]:
        """splits a large Dataframe into multiple chunk-sized dataframes

        :param write: bool, T/F, when True, will write the chunked dataframe to path. Will save in parent folder of path
        :param filename_prefix: the prefix we want to give as the filename of the files
        :param chunk_size:
        :return:
        """
        number_of_chunks = math.ceil(len(self.dataframe) / chunk_size)
        chunks = [self.dataframe[i*chunk_size: (i+1)*chunk_size] for i in range(number_of_chunks)]

        # Determining whether we're going to write to disc or not
        if write is True:
            for idx, df in enumerate(chunks):
                if not filename_prefix:
                    filename_prefix = f"{self.filepath.stem} - {date.today()}"
                df.to_csv(self.filepath.parent / f"{filename_prefix} - {idx + 1} of {len(chunks)}.csv", index=False)
        return chunks

    def as_ordered_transaction(self,
                               sort_by: Hashable | Sequence[Hashable],
                               group_by: Any,
                               value_fields: Hashable | Sequence[Hashable],
                               ascending: bool | list[bool] | tuple[bool] | None = None,
                               ) -> pd.DataFrame:
        """will sort and group an element based on a key column.
        The aggregate function here will be a cumulative sum, so that each record
        builds on the next one.

        We recommend using an index for the columns you want to use to sort, and making
        sure to fill in for any missing data points before putting into this function.

        Think transactions inside a group of transactions, that need to be processed in order, and you
        want to see how each transaction adds onto the previous one.

        Parameters:
            sort_by: the column(s) to sort by
            group_by: the column(s) to group by when aggregating
            value_fields: the column(s) to cumsum and aggregate on. These are the fields that will be used by agg func
            ascending: whether to sort in ascending or descending order. Defaults to ascending.

        """
        dataset = self.dataframe.copy()

        if ascending is None:
            ascending = [True for _ in sort_by]
        dataset = dataset.sort_values(by=sort_by, ascending=ascending, na_position='first')

        # todo utilize the power of DirlinFormatter to parse the value_fields and normalize it against premium field
        try:
            ...
        except Exception as e:
            raise e

        dataset[[field for field in value_fields]] = dataset.groupby(by=group_by)[value_fields].cumsum()
        return dataset

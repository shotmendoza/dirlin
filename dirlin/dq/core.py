from collections import Counter, defaultdict
from dataclasses import dataclass, field
import inspect
import re
from collections.abc import Collection
from datetime import date, datetime
from typing import Literal, Callable, Any

import pandas as pd
from pandas import DataFrame, Series, concat, MultiIndex


@dataclass(frozen=True)
class AliasMap:
    """object responsible for handling user input on an alias_mapping. Will run standard validations on the structure
    and have functionality to help with processing Alias Records, checking if the param exists in the alias map, or
    if a column name is a parameter in the alias map. Will also give information regarding missing parameters that it
    expects to find the exact match of.

    user defined mapping, that ties the column names in the data (report) to the parameter names in the check
    Used to define columns that don't exact-match a parameter in the object,
    but we want to use as an argument in the parameter.

    For example, if we have a column `Total Price` but our test function uses `price`
    as the parameter of the function, we would add `Total Price` as the value under
    `price` in the alias_mapping key-value pair. This would look like this:
    `{"price": ["Total Price]"}`.

    Is a key-value pair of {`parameter name`: [`associated columns`]}, and will tie into
    the function. The error code for `_verify_column_ties_to_parameter` will also notify
    you to add missing parameters into this variable as a dict.

    :param mapping: on writable in the init, but will have a clean, public facing property
    """
    mapping: dict[str, str] | dict[str, Collection]

    def get_relevant(self, keep: Collection) -> dict[str, Any]:
        """returns parameters that match with the values in the Collectable if it's in the alias_mapping.
        Used by Interface when wanting to only use relevant parameter keys when building records.
        """
        return {
            parameter: alias for parameter, alias in self.mapping.items()
            if parameter in keep
        }

    def build_alias_records(
            self,
            columns: Collection,
            parameters: Collection
    ) -> list[dict[str, Any]]:
        """builds the alias_records given the columns and required parameters.

        {
        parameter1: [(parameter1, arg1), (parameter2, arg2)],
        parameter2: [(parameter2, arg1), (parameter2, arg2)]
        }

        This function will only keep the keys that are relevant to the parameters.

        We need to format the user defined alias_mapping in a way that allows us to use it as a set of records.
        To do this, we also need the columns (self.columns) so this function should follow after.

        The parameter being used here is a user defined parameter from alias_mapping.
        These generate an Alias Record that is easier to handle when creating Bound Arguments.
        """
        # [0.1] We need to confirm the collection type columns is to prevent errors
        if isinstance(columns, str):
            raise TypeError(f"parameter `columns` cannot be a list")
        elif isinstance(columns, dict):
            columns = list(columns.keys())

        # [1.0] define the final variable - the goal is to combine alias_mapping [1.1] and columns [1.2]
        records = {}

        # [1.1] get the relevant parameters from the user input
        records = self.get_relevant(keep=parameters)

        # [1.2] get the relevant parameters from the columns
        for param in parameters:
            if param in records:
                if isinstance(records[param], str):  # checking for a single string value
                    records[param] = [records[param]]
                if param in columns:  # at this point, columns have not been matched to params directly
                    records[param].append(param)
            else:
                if param in columns:
                    records[param] = [param]

        # [2025.09.04] At this point, we would know if it was in a column or alias
        # if it's missing at this point, we may be able to just raise an error
        # based on the one that we don't have that we are expecting
        _missing_param_set = [param for param in parameters if param not in records]
        if _missing_param_set:
            raise KeyError(f"parameter `{_missing_param_set}` is not in the alias_records or DataSource")

        # [2025.09.03] in the future could add a log here for any that were skipped

        # [1.4] Grab the One-to-One and separate out the keys args[0] because 'fee': ['foo']
        one_to_one_records = {p: args[0] for p, args in records.items() if len(args) == 1}  # p: c
        one_to_many_records = {p: args for p, args in records.items() if len(args) > 1}
        _otm_keys = [k for k in records if k not in one_to_one_records]

        # [1.5] Let's attempt to create a clean, record set. 2nd step adds the One-to-One params
        records = [dict(zip(_otm_keys, group)) for group in zip(*one_to_many_records.values())]  # secret sauce
        records = [one_to_one_records | record for record in records]

        # [1.6] There's a weird bug where if a one-to-one exists but no one-to-many, then it returns no records
        # we need to add the check below to work around that
        if not records and one_to_one_records:
            records = [one_to_one_records]

        # We can add whatever logic we think fits here. The idea is that we want to limit the loops to keep speed
        # since we are going to repeatedly be using this obj to parse the two objects
        return records


@dataclass
class DataSource:
    """Handles the data that is used for the validation.

    Only needs `name` and `data` (dataframe)

    :param name: name of the data source
    :param data: the DataFrame
    """
    name: str
    """Name of the data source. Will be used in the final results section for reference."""

    data: DataFrame
    """The data we want checked by the validation and pipeline."""

    def __post_init__(self):
        """The available columns in the data source. Mapping with type to keep opportunity in future
        to incorporate some kind of type checking against the function. For example, making sure that if a date
        column is being used in the check, that the function can `fix` the unformatted date column on the report.
        """
        self.columns: dict[str, Any] = {col: self.data[col].dtype for col in self.data.columns}


@dataclass(init=False)
class ResultWrapper:
    fn_ran_in_session: Counter = field(default_factory=Counter)
    """the number of functions that have run through this ResultWrapper"""

    data_ran_in_session: Counter = field(default_factory=Counter)
    """the number of times a DataSources was run through this ResultWrapper"""

    def __init__(
            self,
            result: Series = None,
            label: str | None = None,
            function_name: str | None = None,
            fn_description: str | None = None,
            report_name: str | None = None,
            flag_true: bool = False,
    ):
        """Used as a wrapper for results.

        :param result: result of the FuncObj being run
        :param label: a unique label used as a key for the ResultWrapper deliverables
        :param function_name: the name of the func that was run. Recommended to fill this on empty results for debug.
        :param fn_description: the description of the func pulled from the docstring
        :param report_name: the name of the report or DataSource
        :param flag_true: True if function returns True on incorrect lines. Default is False.

        """
        # Any new records added, should be put below, under __add__, and _calculate_results

        ##########################################################
        # Properties - Label LEVEL FIELDS
        ##########################################################
        self.label_counter_total: Counter = Counter()
        """counts all unique labels in the session"""

        self.label_counter_checked: Counter = Counter()
        """counts every time a label is checked"""

        self.label_counter_skipped: Counter = Counter()
        """counts every time a label is skipped"""

        self.label_counter_message: Counter = Counter()
        """counts any label that was part of message function"""

        ##########################################################
        # Properties - Check | Label LEVEL FIELDS
        ##########################################################
        self.records_total: Counter = Counter()
        """Counter for total records on checks"""

        self.records_passed: Counter = Counter()
        """Counter for successes on checks"""

        self.records_failed: Counter = Counter()
        """Counter for failures on checks"""

        ##########################################################
        # Properties - FUNCTION LEVEL FIELDS
        ##########################################################
        # TODO [2025.09.06] The FN desc might not belong here
        self.fn_desc: dict = dict()
        """Mapping for the function description. Useful for error logs."""

        self.error_counter_fn: Counter = Counter()
        """Counter for total errors on a specific check"""

        self.fn_ran_in_session: Counter = Counter()
        """the number of functions that have run through this ResultWrapper"""

        self.fn_checked_in_session: Counter = Counter()
        """the number of times a function went through the checking process"""

        self.skipped_fn_names: dict = defaultdict(list)
        """Dict for any functions that got skipped with the functions as keys"""

        self.skipped_fn_counter: Counter = Counter()
        """counter for functions that were skipped with the functions as keys"""

        ##########################################################
        # Properties - OTHER LEVEL FIELDS
        ##########################################################
        self.records_affected: dict = defaultdict(list)
        """keeps the index of the affected records"""

        self.message_records: dict = defaultdict(list)
        """Dict for any functions that return anything other than a boolean series"""

        self.message_counter_fn_name: Counter = Counter()
        """counter for the number of functions that are messenger functions by fn"""

        self.message_counter_data: Counter = Counter()
        """counter for the number of functions that are messenger functions by DataSource"""

        ##########################################################
        # Properties - DATA LEVEL FIELDS
        ##########################################################
        self.error_counter_data: Counter = Counter()
        """Counter for total errors on a DataSource in total"""

        self.data_ran_in_session: Counter = Counter()
        """the number of times a DataSources was run through this ResultWrapper"""

        self.data_checked_in_session: Counter = Counter()
        """the number of times a DataSource went through the checking process"""

        self.skipped_fn_names_data_key: dict = defaultdict(list)
        """Dict for any functions that got skipped with the report as keys"""

        ##########################################################
        # KEYS - FOR JOINING THE DIFFERENT LEVELS
        ##########################################################
        self.label_to_fn_relation: dict = dict()
        """relational table for mapping label => function keys"""

        #########################
        # === INIT FUNCTIONS ===
        #########################
        # We have to put this here since we want to enforce that a single pd.Series comes in here when initialized
        self._is_check: bool | list[bool] = self.is_bool_series(result)  # currently only bool results are checks
        """flag that signifies whether the function that was used is returning bool values or other"""

        # Generates all the data needed for result functionality
        self._calculate_results(
            result=result,
            label=label,
            report_name=report_name,
            fn_name=function_name,
            fn_description=fn_description,
            flag_true=flag_true
        )

    def __add__(self, other: "ResultWrapper") -> "ResultWrapper":
        """used to combine multiple ResultWrappers together, so we don't need to come up with a separate dataclass
        to do so.
        """
        # === [0] we're going to assume everything based on the type under result ===
        if not isinstance(other, ResultWrapper):
            raise TypeError(f"Cannot combine ResultWrapper with {type(self)} and {other}")

        # === [1] Handling the counters ===
        # [records]
        self.records_total.update(other.records_total)
        self.records_passed.update(other.records_passed)
        self.records_failed.update(other.records_failed)
        # [errors]
        self.error_counter_fn.update(other.error_counter_fn)
        self.error_counter_data.update(other.error_counter_data)
        # [sessions]
        self.fn_ran_in_session.update(other.fn_ran_in_session)
        self.data_ran_in_session.update(other.data_ran_in_session)
        self.data_checked_in_session.update(other.data_checked_in_session)
        self.fn_checked_in_session.update(other.fn_checked_in_session)
        self.skipped_fn_counter.update(other.skipped_fn_counter)
        # [messages]
        self.message_counter_data.update(other.message_counter_data)
        self.message_counter_fn_name.update(other.message_counter_fn_name)
        # [labels]
        self.label_counter_total.update(other.label_counter_total)
        self.label_counter_checked.update(other.label_counter_checked)
        self.label_counter_skipped.update(other.label_counter_skipped)
        self.label_counter_message.update(other.label_counter_message)

        # === [2] Handling the mappings ===
        # [idx]
        for k, v in other.records_affected.items():
            self.records_affected[k].extend(v)  # dict {123: [fn1]} + {123: [fn2]}
        # [fn desc.]
        for k, v in other.fn_desc.items():
            if k not in self.fn_desc:
                self.fn_desc[k] = v
            elif k in self.fn_desc and v != self.fn_desc[k]:
                self.fn_desc[k] = f"{self.fn_desc[k]} | {v}"
        # [fn | data]
        for k, v in other.skipped_fn_names.items():
            self.skipped_fn_names[k].extend(v)
        for k, v in other.skipped_fn_names_data_key.items():
            self.skipped_fn_names_data_key[k].extend(v)
        # [message]
        for k, v in other.message_records.items():
            self.message_records[k].extend(v)  # dict
        # [relational]
        for k, v in other.label_to_fn_relation.items():
            if k not in self.label_to_fn_relation:
                self.label_to_fn_relation[k] = v
            # keep original value if key exists already
        # === done
        return self

    @classmethod
    def is_bool_series(cls, sample: Series) -> bool:
        """since the return type is insufficient when trying to figure out whether the pd.Series returns a boolean
        series or not, we're going to go ahead and create this function that checks whether this function returns
        the proper data.
        """
        if sample is None:
            # signifies a default value or an empty ResultWrapper
            return False
        return sample.isin((True, False)).any()

    def _calculate_results(
            self,
            result: Series,
            label: str | None = None,
            report_name: str | None = None,
            fn_name: str | None = None,
            fn_description: str | None = None,
            flag_true: bool = False,
    ) -> None:
        """calculates the derived values from the checks. Uses the results as a basis for creating
        these so that the end user can get more details on what happened.

        :param result: the result of the check
        :param report_name: the name of the DataSource used for the check
        :param fn_name: the name of the function that was used to calculate the result
        :param flag_true: flag that signifies whether the end result should keep fn returning False or True
        """
        # [1.0] Handling the various inputs we may get
        result = Series() if result is None else result
        fn_name = fn_name if fn_name is not None else "No Function"
        report_name = report_name if report_name is not None else "No Report"
        fn_description = fn_description if fn_description is not None else "Skipped"
        label = label if label is not None else "Skipped Function"

        # [1.1] Session Trackers, tracking any time `_calculate_results` gets run
        self.fn_ran_in_session.update({fn_name: 1})
        self.data_ran_in_session.update({report_name: 1})
        self.label_counter_total.update({label: 1})

        # [1.2] Fn Trackers, tracking any function level
        if fn_name not in self.fn_desc:
            self.fn_desc[fn_name] = fn_description

        # [1.3] add relationship
        self.label_to_fn_relation[label] = fn_name

        if not result.empty:  # Filled Results, we have data to work with
            if self._is_check:  # ensures pd.Results has boolean values as the results | else treat it like a msg
                # [2.1] session check trackers, update fields tracking any time a check is run
                self.fn_checked_in_session.update({fn_name: 1})
                self.data_checked_in_session.update({report_name: 1})
                self.label_counter_checked.update({label: 1})
                failed = result.count() - result.sum()  # less recalc

                # [2.2] label trackers, update records (!) can change to UUID or hash if necessary
                self.records_total.update({label: result.count()})  # records depend on the number of lines in df
                self.records_passed.update({label: result.sum()})
                self.records_failed.update({label: failed})

                # [2.3] idx trackers, (!) uses the index | could make this section more efficient without loop
                affected_policies = result.index[result] if flag_true else result.index[~result]
                for idx in list(affected_policies):
                    self.records_affected[idx].append(fn_name)  # indexes that we want flagged for future mask

                # [2.4] fn, data level => update with the number of errors
                self.error_counter_fn.update({fn_name: failed})
                self.error_counter_data.update({report_name: failed})

            else:  # non-bool fn = message fn
                # [3.1] session tracking for message labels
                self.label_counter_message.update({label: 1})

                # [3.2] idx trackers, (!) I think messages should be idx level (!) CHANGE TO ITD IF I REALIZE
                self.message_records[label].append(result)  # label here would be column name

                # [3.3] fn, data level => update counter for message functions
                self.message_counter_data.update({report_name: 1})  # message functions, DataSource as key
                # TODO [2025.09.06] may need to change the name or flag that this checks for the type and not occurrence
                if fn_name not in self.message_counter_fn_name:
                    self.message_counter_fn_name.update({fn_name: 1})  # message functions, FuncObj as key

        else:  # Empty Pd.Series, we consider these pure skips
            # [4.1] session tracking for skipped labels
            self.label_counter_skipped.update({label: 1})

            # [4.2] Trackers for skipped functions | non-boolean & other fn
            self.skipped_fn_names_data_key[report_name].append(fn_name)  # Skipped, DataSource as key
            self.skipped_fn_names[fn_name].append(report_name)  # Skipped, FuncObj as key
            self.skipped_fn_counter.update({fn_name: 1})

            # [4.3] NOTE
            # TODO [2025/09/06] realized that this would be able to return some value on what parameters were missing
            # For example, the InterfaceCheckObj could take this skipped property, and identify which
            # parameters were missing and return those values.

    def as_dict(self) -> dict:
        """used for quicker summary and error log creation
        """
        # initialize required properties
        temp_dict = vars(self)
        return temp_dict

    def as_dataframe(
            self,
            dtype: Literal["counter", "dict", "message", "summary", "log"] = "counter"
    ) -> DataFrame:
        """return the results as a DataFrame. Currently under progress but splits the dataframe into two types.
        Counter is an index DataFrame with Counter "Summary" values. The Dict is your regular dataframe.

        "summary" is for the classic v1 error summary format.
        "log" is for the classic v1 error log format.
        """
        supported = ["counter", "dict", "message", "summary", "log"]
        if dtype not in supported:
            raise ValueError(f"dtype must be one of {supported}")

        # (!) To start I think it's easier just to split these into Counters with the index DataFrame, and
        # Default dicts that have a regular dataframe, and have the user choose which DataFrame they want.
        # We can dive into this portion more later when we can understand multi-index columns better.

        # [Note] all the DataFrames currently is a multi-index DataFrame | except Summary and Log
        if dtype == "counter":
            return self._generate_counter_df()
        elif dtype == "dict":
            return self._generate_dict_df()
        elif dtype == "message":
            return self._generate_message_df()
        elif dtype == "summary":
            return self._generate_summary_df()
        elif dtype == "log":
            ...

    def _generate_counter_df(self) -> DataFrame:
        """helper function for generating a dataframe from counters. These kind of look like the classic summary
        sheet
        """
        _counter_df = None
        for second_level, first_level in self.as_dict().items():
            # This almost looks like the summary sheet
            if isinstance(first_level, Counter):
                if not first_level:  # missing the first level index => data | fn
                    first_level = {"Ran": 0}
                df = DataFrame.from_dict(first_level, orient='index', columns=["Count"])  # Counters have scalar values
                df.index = MultiIndex.from_product([[second_level], df.index], names=["Metric", "Item"])
                if _counter_df is None:
                    _counter_df = df
                    continue
                _counter_df = concat((_counter_df, df))
        return _counter_df

    def _generate_dict_df(self) -> DataFrame:
        """helper function for generating a dataframe from dicts. These have a little more detail, but are a little
        messy in its current state.
        """
        _dict_df = None
        for second_level, first_level in self.as_dict().items():  # 2: counter_name, 1: fn | d names
            # This has more specific values than the counters
            if isinstance(first_level, defaultdict):
                if not first_level:  # can't have this global because it changes the type from Counter to dict
                    first_level = {"Ran": [0]}
                # Attempt to create a column level for defaultdict types -- easier to tell the data
                df = DataFrame.from_dict(first_level)
                df.columns = MultiIndex.from_product([[second_level], df.columns], names=["Measure", "Item"])
                if _dict_df is None:
                    _dict_df = df
                    continue
                _dict_df = concat((_dict_df, df))
        return _dict_df

    def _generate_message_df(self) -> DataFrame:
        """helper function for generating a dataframe from message functions that aren't quite checks.
        This can, maybe in the future, be combined with other dataframes in order to make a result log,
        where each line that was checked can have a message column like (notes).
        """
        _msg_df = None
        for second_level, first_level in self.as_dict().items():  # 2: counter_name, 1: fn | d names
            # For handling message functions -> Special case
            if second_level == "message_records":
                if not first_level:  # there were no message functions
                    _msg_df = DataFrame()
                    continue
                transposed_dict = {k: v[0] for k, v in first_level.items()}
                df = DataFrame.from_dict(transposed_dict)
                if _msg_df is None:
                    _msg_df = df
                    continue
                _msg_df = concat((_msg_df, df))
        return _msg_df

    def _generate_summary_df(self):
        """creates a basic summary dataframe with the pass / fail for each check.

        Columns:
            - Label: a unique key for the check record
            - Check Function Name: name of the function used for the validation
            - Check Function Description: description of the function
            - Total Records Validated: the number of records that were validated in total
            - Total Records Passed: the number of records that successfully passed the validation
            - Total Records Failed: the number of records that failed the validation
        """
        # [1.1] work on the smallest unit, labels and create the pd.DataFrame
        label_bundle = (
            self.label_counter_skipped,
            self.label_counter_message,
            self.label_counter_checked,
            self.label_to_fn_relation
        )
        label_df = DataFrame(label_bundle).T
        label_df.columns = ["Skipped", "Message", "Checked", "Check"]
        bool_col = ["Skipped", "Message", "Checked"]
        label_df[bool_col] = label_df[bool_col].fillna(0).astype(bool)
        label_df.index.name = "Label"

        # [1.2] work on the next unit, records. these will be blank if all checks were skipped (ie message, skip)
        result_bundle = (self.records_total, self.records_passed, self.records_failed)
        result_df = DataFrame(result_bundle).T.fillna(0).astype(int)
        result_df.columns = ["Total", "Passed", "Failed"]
        result_df.index.name = "Label"

        # [1.3] work on the function level unit
        fn_bundle = (self.fn_desc, self.message_counter_fn_name, self.skipped_fn_counter)
        fn_df = DataFrame(fn_bundle).T
        fn_df.columns = ["Description", "Message Count", "Function Skipped"]
        number_col = ["Message Count", "Function Skipped"]
        fn_df[number_col] = fn_df[number_col].fillna(0).astype(int)
        fn_df["Description"] = fn_df["Description"].fillna("No description found").astype(str)

        # [1.4] Join the label level and function level df together
        fn_df.index.name = "Check"
        label_df = label_df.reset_index()
        final = label_df.merge(fn_df, how="left", left_on="Check", right_index=True)

        # [1.5] Join the Label and Results level together
        final = final.merge(result_df, how="left", left_on="Label", right_index=True)
        final[["Total", "Passed", "Failed"]] = final[["Total", "Passed", "Failed"]].fillna(0).astype(int)
        final = final.set_index("Label")
        print(final.columns)
        return final

# class _Deliverable:
#     @staticmethod
#     def run_summary(
#             results: dict[str, ResultWrapper],
#             group_name: str | None = None
#     ) -> pd.DataFrame:
#         """creates a basic summary dataframe with the pass / fail for each check.
#
#         Columns:
#             - Check Function Name: name of the function used for the validation
#             - Total Records Validated: the number of records that were validated in total
#             - Total Records Passed: the number of records that successfully passed the validation
#             - Total Records Failed: the number of records that failed the validation
#         """
#         # this will allow you to do result.run_summary or result.run_validation, result.error_log
#
#         # Summary without the validation_name
#         if group_name is None:
#             summary = {
#                 check_name: {
#                     "Check Function Name": r.function_name,
#                     "Check Description": r.function_description,
#                     "Total Records Validated": r.result.count(),
#                     "Total Records Passed": r.result.sum(),
#                     "Total Records Failed": len(r.result) - r.result.sum(),
#                 } for check_name, r in results.items()
#             }
#         else:
#             summary = {
#                 check_name: {
#                     "Group": group_name,
#                     "Check Function Name": r.function_name,
#                     "Check Description": r.function_description,
#                     "Total Records Validated": r.result.count(),
#                     "Total Records Passed": r.result.sum(),
#                     "Total Records Failed": len(r.result) - r.result.sum(),
#                 } for check_name, r in results.items()
#             }
#         result = pd.DataFrame(summary).T.reset_index().sort_values("Total Records Failed", ascending=False)
#         return result
#
#     @staticmethod
#     def run_error_log(
#             results: dict[str, ResultWrapper],
#             df: pd.DataFrame,
#             group_name: str | None = None
#     ) -> pd.DataFrame:
#         """gives you a Dataframe with the records that failed the validation
#         """
#         results_filter = []
#         for check_name, r in results.items():
#             temp_df = df[~r.result].copy()
#             temp_df["Check"] = r.function_name
#             if group_name is not None:
#                 temp_df["Group"] = group_name
#             results_filter.append(temp_df)
#
#         _df_results = pd.concat(results_filter, ignore_index=True)
#         return _df_results


@dataclass
class FuncObj:
    """handles each individual function. This is usually created by the Pipeline automatically when
    initializing the functions, and not by the user.
    """
    fn: Callable

    def __post_init__(self):
        """handles each individual function. This is usually created by the Pipeline automatically when
         initializing the functions, and not by the user.
         """
        self.name: str = str(self.fn.__name__)
        """name given to the function by the user."""

        self.doc: str = self._get_function_docstrings()
        """The docstring of the function, used to describe the check."""

        self.signatures: inspect.Signature = inspect.signature(self.fn)
        """the signatures (param: type) for the given function."""

        self.params = self.signatures.parameters
        """the parameters under the signature"""

        non_scalar_types = (DataFrame, Series)
        self.scalar_type_fn: bool = any(
            (p.annotation not in non_scalar_types for p in self.signatures.parameters.values())
        )
        """Determines how the function should be set up and run by any reports using the check."""

        self.return_type_fn: Any = self.signatures.return_annotation
        """The return type of the function. Used for the final deliverables to see how the results
        should be handled
        """

    def __call__(self, data: DataSource, records: list[dict[str, Any]]) -> ResultWrapper:
        """adding an instance functionality that will allow it to take data, along with the alias records,
        and create a ResultWrapper.

        :param data: DataSource,
        :param records: list[dict[str, Any]]
        """
        # TODO [2025.09.06] would be VERY helpful to know why the record was empty.
        #  This would help with debugging the code

        if records is None:
            skipped_label = f"{data.name}_{self.name}_{datetime.now().second}_{datetime.now().microsecond}_skipped"
            results = self._handle_result_wrapper(
                new_result=Series(),
                label=skipped_label,
                function_name=self.name,
                function_description=self.doc,
                report_name=data.name,
            )

        # Handling Scalars and Pandas type. Can be expanded in future with more param types.
        # Assumes unique keys. Use UUID if not.
        elif self.scalar_type_fn:
            results = self._handle_bound_args_scalar_fn(data, records)
        else:
            results = self._handle_bound_args_pandas_fn(data, records)
        return results

    def _get_function_docstrings(self, scope: Literal["first", "all"] = "first") -> str:
        """helper function used to extract the function docstrings. This will then be used for the final error log
        to show the description of what each check is doing based on the docstrings.

        :param scope: ['first', 'all'] determines whether to grab the first sentence of the docstring or to capture
        the entire docstring. Default is 'first'.
        """
        try:
            docstring = inspect.getdoc(self.fn)
            if not docstring:
                docstring = f"No description for {self.name}..."
                return docstring

            if scope == "all":
                return docstring
            elif scope == "first":
                # matches for any (.), (!), (?) and or a new line
                first = re.match(r"(.*?[.!?])(?:\s|$)|([^\n*]*)", docstring, re.DOTALL)
                docstring = (first.group(1) or first.group(2)).strip() if first else docstring
        except Exception as exc:
            raise exc
        return docstring

    def _handle_bound_args_pandas_fn(
            self,
            data: DataSource,
            records: list[dict[str, str]],
    ) -> ResultWrapper:
        """handles the bound argument creation process for Pandas functions.

        :param records: is a dictionary argument that holds the param: column pairs used on the function.
        """
        converted_records = [{p: data.data[c] for p, c in record.items()} for record in records]
        record_labels = [f"{data.name}_{str('_'.join(k for k in r.values()))}" for r in records]

        results = None
        for record, label in zip(converted_records, record_labels):
            bound_args = self.signatures.bind_partial(**record)
            bound_args.apply_defaults()
            # [2025.08.26] in the future, we could check for return type to make more robust
            curr = self.fn(*bound_args.args, **bound_args.kwargs)  # assumes pd.Series return

            # Appending the results of the check
            results = self._handle_result_wrapper(
                new_result=curr,
                label=label,
                function_name=self.name,
                function_description=self.doc,
                report_name=data.name
            )
        return results

    def _handle_bound_args_scalar_fn(
            self,
            data: DataSource,
            records: list[dict[str, str]],
    ) -> ResultWrapper:
        """handles the bound argument creation process for Scalar functions.

        :param records: is a dictionary argument that holds the param: column pairs used on the function.
        """
        rename_map = [{v: k for k, v in r.items()} for r in records]
        converted_records = [
            data.data.copy()[record.values()].rename(columns=rm).to_dict("records")
            for record, rm in zip(records, rename_map)
        ]
        record_labels = [f"{data.name}_{str('_'.join(k for k in r.values()))}" for r in records]

        results = None
        for record, label in zip(converted_records, record_labels):
            bound_args = [self.signatures.bind_partial(**r) for r in record]  # bind the args
            [r.apply_defaults() for r in bound_args]  # not sure if this is clean or works
            ran_args = [self.fn(*args.args, **args.kwargs) for args in bound_args]
            curr = Series(ran_args)

            results = self._handle_result_wrapper(
                new_result=curr,
                label=label,
                function_name=self.name,
                function_description=self.doc,
                report_name=data.name,
            )
        return results

    @classmethod
    def _handle_result_wrapper(
            cls,
            new_result: Series | None,
            label: str,
            function_name: str,
            function_description: str,
            report_name: str,
    ) -> ResultWrapper:
        """handles the ResultWrapper from the function.
        """
        temp = ResultWrapper(
            result=new_result,
            label=label,
            function_name=function_name,
            fn_description=function_description,
            report_name=report_name
        )
        return temp


@dataclass(init=False)
class InterfaceCheckObj:
    """Interfaces between the FuncObj, Data Sources, and ResultWrappers.
    """
    def __init__(
            self,
            checks: FuncObj | Collection[FuncObj] | None = None,
            alias_mapping: dict[str, str] | dict[str, Collection] | AliasMap | None = None
    ):
        """Interfaces between the FuncObj, Data Sources, and ResultWrappers.

        :param checks: The checks that we want to run through for validations
        :param alias_mapping: A mapping of parameter names to columns.
        """

        self._checks: FuncObj | Collection[FuncObj] = checks
        """The checks that we want to run through for validations."""

        self.alias_mapping: dict[str, str] | dict[str, Collection] | AliasMap | None = alias_mapping
        """user defined mapping, that ties the column names in the data (report) to the parameter names in the check
        Used to define columns that don't exact-match a parameter in the object,
        but we want to use as an argument in the parameter.
    
        For example, if we have a column `Total Price` but our test function uses `price`
        as the parameter of the function, we would add `Total Price` as the value under
        `price` in the alias_mapping key-value pair. This would look like this:
        `{"price": ["Total Price]"}`.
    
        Is a key-value pair of {`parameter name`: [`associated columns`]}, and will tie into
        the function. The error code for `_verify_column_ties_to_parameter` will also notify
        you to add missing parameters into this variable as a dict.
        """

        self.results: ResultWrapper | None = None
        """holds the results from the Interface being run by an outside manager.
        """

        #################
        # FORMATTING
        #################
        if isinstance(self.alias_mapping, dict):
            self.alias_mapping = AliasMap(mapping=self.alias_mapping)

    @property
    def checks(self) -> FuncObj | Collection[FuncObj]:
        """Public read-only property for the checks currently under this interface."""
        return self._checks

    def add_checks(self, checks: FuncObj | Collection[FuncObj]) -> None:
        # TODO [2025.09.05] we would add a check here to confirm the FuncObj is valid
        # To add, this is probably where we determine whether it's a check or a message

        # [O1] Checks is a single FuncObj
        if isinstance(checks, FuncObj):
            # [SO1] Interface.checks is collection
            if isinstance(self._checks, Collection) and isinstance(self._checks, list):
                self._checks.append(checks)
            # [SO2] Interface.checks is single value
            elif isinstance(self._checks, FuncObj):
                self._checks = [self._checks]
                self._checks.append(checks)
            self._checks = [checks]

    def build(
            self,
            data: DataSource,
    ) -> ResultWrapper:
        """public facing method for returning the results of the check based on a given DataSource
        """
        for fn in self.checks:
            # [1.1] Get the records for the fn in question
            try:
                # TODO [2025.09.06] we should actually be catching missing records in this alias portion
                records = self.alias_mapping.build_alias_records(data.columns, fn.params)
                temp_result = fn(data, records)  # [1.2] run the check function on the records

            except KeyError as KE:
                # We usually get a KeyError here when there are missing parameters, either
                # from the parameters not being defined in the alias_mapping, or the parameters
                # not being defined in the DataSource columns.

                # TODO [2025.09.06] we honestly should be catching this before it hits the interface
                skipped_label = f"{data.name}_{fn.name}_{KE.args[0]}_{datetime.now().microsecond}_skipped"
                temp_result = ResultWrapper(
                    function_name=fn.name,
                    label=skipped_label,
                    fn_description=fn.doc,
                    report_name=data.name,
                )
            except Exception as E:
                raise E
            if self.results is None:
                self.results = temp_result
                continue
            self.results += temp_result
        return self.results

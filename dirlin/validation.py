"""Part of the Dirlin Pipeline 2.0 (2025.04.30)"""

import dataclasses
import inspect
import logging
from collections import defaultdict
from collections.abc import Collection
from typing import Any, Callable

import pandas as pd
from tqdm import tqdm

from dirlin.core.api import DirlinFormatter, TqdmLoggingHandler
from dirlin.dq.interfaces import ResultWrapper


@dataclasses.dataclass
class _HandleColumnsParams:
    """MIGHT NEED IN FUTURE IF WE ARE TO TIE THIS TO THE PIPELINE AGAIN

    Manages the column, alias, parameter relationship until they get created into
    single records. Since this is on a report level, we will handle this individually and separate from the main
    class.
    """
    _obj: "Pipeline" or "DataSource"

    _reports: Collection[pd.DataFrame] | pd.DataFrame

    _signatures: dict | None = None
    """The mapping of the signatures under all functions in the subclasses"""

    ##########################################################
    # Properties Related to Aliases and Columns as parameters
    ##########################################################
    aliases: dict | None = None
    """The mapping of the aliases under all of the subclasses"""

    columns: dict | None = None
    """The columns tied to parameters"""

    params: dict | None = None
    """The combination of aliases and columns. The function responsible for getting this data ensures that
    no alias or column data is overwritten in cases one parameter is used in multiple functions.
    """

    #######################################################################
    # Properties related to adding bound arguments to signatures by record
    #######################################################################
    fn_records: dict | None = None
    """Stores the alias/column param records under the function so we can rapid fire run the functions."""

    def __post_init__(self):
        # CHECK TO MAKE SURE THE NECESSARY FUNCTION WAS RAN FIRST

        # === Ensures that Alias, Columns, Params, Signatures have all been `saved` into the subclasses
        self._initialize_alias_and_column_properties()

    def _get_alias_from_subclass(self):
        """goes down the inheritance chain and pulls the alias_mapping from each subclass.
        We centralize this data so that it can be used at any point in time.
        """
        ##########################################################
        # COMBINES ALL CLASS LEVELS OF ALIASES INTO ONE ALIAS
        ##########################################################
        # The idea is to take all the user defined alias_mapping from each subclass and make it into one dictionary
        aliases: dict | None = None
        for subclass in inspect.getmro(self._obj.__class__)[:-1]:
            if "alias_mapping" in subclass.__dict__:
                temp = subclass.__dict__["alias_mapping"]  # holds the user-defined alias_mapping dict
                if aliases is None:
                    aliases = temp
                    continue
                # now I want to check to ensure we're not overriding values from the higher level classes
                override_param = {}
                for param, col in temp.items():
                    try:
                        aliases[param]  # check for keyword in dict
                    except KeyError:  # is a new parameter we want to add
                        override_param[param] = col
                aliases = aliases | override_param

        self.aliases = aliases
        return aliases

    def _get_columns_from_subclass(self):
        """Maps out any parameters that matches with a column name
        """
        ##########################################################
        # Grabs all the columns that match with a parameter
        ##########################################################
        if not isinstance(self._reports, pd.DataFrame):  # wording / order matters. Collection treats DF as collection
            # weird way to do it, but we're going to combine all columns, and do it that way for now
            all_columns = list(set(column for report in self._reports for column in report.columns))
        else:
            all_columns = self._reports.columns

        col_mapping = {
            p: p
            for fn, sig in self._obj.signatures.items()
            for p in sig.parameters.keys() if p in all_columns
        }

        self.columns = col_mapping
        return col_mapping

    def _initialize_alias_and_column_properties(self) -> None:
        """combines parameters that match directly with the column name in the report,
        as well as the user defined aliases.
        """
        #########################################################################
        # (1) CREATE A SINGLE PARAMETER MAPPING BASED ON COLUMNS AND USER INPUT
        #########################################################################
        try:
            self._signatures = self.signatures.copy()
        except AttributeError:
            self._signatures = self._obj.signatures.copy()

        #################
        # LOGGER CONFIG
        #################
        _number_of_processes_in_class = 2 + len(self._signatures)
        pbar = tqdm(total=_number_of_processes_in_class, desc="Initializing Aliases and Columns...")

        pbar.set_description(desc="Confirming all required functions have run...")
        # === [1] Ensure all required functions have run ===
        if any((self.aliases is None, self._signatures is None)):
            self._get_alias_from_subclass()
        if self.columns is None:
            self._get_columns_from_subclass()
        pbar.update()

        pbar.set_description(desc="Confirming all required functions have run...")
        # === [2] Combine the two different sources into one source ===
        if self.params is None:
            self.params = defaultdict(list)
        for param, col in self.aliases.items():
            self.params[param].append(col) if isinstance(col, str) else self.params[param].extend(col)
        for param, col in self.columns.items():
            self.params[param].append(col) if isinstance(col, str) else self.params[param].extend(col)
        pbar.update()

        ##########################################################
        # (2) CREATE THE RECORDS
        ##########################################################
        # initialize the relationship properties
        self.fn_records = {}

        # There's cases when we use the same parameter for multiple functions, which is why we are doing it the hard way
        for fn, sig in self._signatures.items():
            pbar.set_description(f"Generating records for {fn}")
            # === [3.1] We only want one-many information right now===
            curr_fn_params = [
                p for p in sig.parameters
                if p in self.params and len(self.params[p]) != 1
            ]
            curr_one_to_many = [
                self.params[p] for p in curr_fn_params
                if p in self.params and len(self.params[p]) != 1
            ]
            # === [3.2] Now we're going to make them into arg records ===
            curr_records = [dict(zip(curr_fn_params, x)) for x in zip(*curr_one_to_many)]

            # === [4.1] Now let's take care of adding the one-to-one to the current records we have ===
            curr_one_to_one = {
                p: self.params[p] if len(self.params[p]) != 1 else self.params[p][0]  # want to remove it from list form
                for p in sig.parameters if p in self.params and len(self.params[p]) == 1
            }
            # === [4.2] There was an edge case where it would return blank if no many-to-many relationship
            if not curr_records and curr_one_to_one:
                self.fn_records[fn] = [curr_one_to_one]
                pbar.update()
                continue
            curr_records = [curr_one_to_one | record for record in curr_records]
            self.fn_records[fn] = curr_records
            pbar.update()
        pbar.set_description("Record Generation Complete!")
        pbar.close()

    def bind_signature_records(self, report: pd.DataFrame | None = None) -> dict[Any, list]:
        """Binds the arguments into records using the aliases and columns provided.
        """
        #############################
        # (3) INITIALIZE SUBCLASSES
        #############################
        # === Prepping for the Alias Properties ===
        if not report:
            if self._reports is None:
                raise ValueError("No reports provided. Need to initialize with one or, add as an argument to function.")
            report = self._reports.copy()

        # === Scalars Handling ===
        handle_fn_as_scalar = {
            fn: not any(
                (t.annotation in (pd.Series, pd.DataFrame) for pt, t in sig.parameters.items())
            ) for fn, sig in self._signatures.items()
        }

        #################################
        # (4) GENERATING THE SIGNATURES
        #################################
        function_bound_records = {}  # for running the arguments on the functions later
        for fn, sig in self._signatures.items():
            #############
            # THE SETUP
            #############
            curr_records = self.fn_records[fn]
            uses_scalar_fn = handle_fn_as_scalar[fn]

            # === [1.1] We only care about scalars to start ===
            if uses_scalar_fn:
                curr_reversed_map = [{v: k for k, v in arg.items()} for arg in curr_records]
                temp_transaction_hold = [
                    report[r.keys()].rename(columns=r).to_dict(orient="records")
                    for r in curr_reversed_map
                ]
                # === [1.2] Now we apply the arguments to the bound signature ===
                temp = [sig.bind_partial(**record) for aliases in temp_transaction_hold for record in aliases]
            else:
                temp_transaction_hold = [{p: report[col] for p, col in arg.items()} for arg in curr_records]
                temp = [sig.bind_partial(**aliases) for aliases in temp_transaction_hold]
            function_bound_records[fn] = temp
        return function_bound_records


class _BaseValidationCheck:
    def __init__(
            self,
            df: pd.DataFrame,
            function_return_type: dict,
            function_param_and_type: dict,
            params_in_class: dict,
            alias_mapping: dict,
    ):
        """used for running verification on the core class to ensure that the formatting
        and organization of the dataframe is correct, and made in a way that the BaseValidation
        object can handle.

        This class will handle the collection of needed data (functions, function param, dataframe) and
        hold the data for the Base Validation class to use.

        Methods with the name `verify` is used for ensuring that the entire pipeline can run without
        errors.

        Methods with `get` are used for gathering the fields and data that are required for the
        BaseValidation class, and verifying the pipeline.

        Methods with `map` are responsible for tying together the Dataframe column names to the parameters
        of the check functions

        The workflow would look like this:
            1. Get the user defined alias_mapping to map columns to the parameters
            2. Verify all expected parameters have a column tied to it
            3. Verify expected return type
            4. Verify values in the columns

        :param df: the dataframe we are verifying that follows the formatting for BaseValidation
        :param function_return_type: dictionary of {`check`: `return_type`}. Pulled from `_get_function_return_type`
        :param function_param_and_type: dictionary of `check: {parameter: Type}`. From `_get_function_param_and_type`
        :param params_in_class: dictionary of `parameter: list[column] | None`. From `_get_all_params_in_class`
        :param alias_mapping: {`parameter name`: [`associated columns`]}
        """
        self.df = df
        self.function_return_type = function_return_type
        self.function_param_and_type = function_param_and_type
        self.params_in_class = params_in_class
        self.alias_mapping = alias_mapping

    def _verify_function_param_return_match(self) -> bool:
        """verifies that the param types (pd.Series, scalar) matches the return type on a function.

        The context for this is when we are deciding to process as a Series function or a Scalar function,
        we need to add this cap in order to understand how to save the results.

        :return: True / False
        """
        missing_return_value = [
            function_name for function_name, return_type in self.function_return_type.items()
            if return_type is None
        ]
        if missing_return_value:
            raise ValueError(f"Functions {missing_return_value} is missing a return type.")

        for check, parameter in self.function_param_and_type.items():
            _has_series_type = False
            _has_scalar_type = False
            if all((isinstance(arg, pd.Series) for arg in parameter)):
                if not isinstance(self.function_return_type[check], pd.Series):
                    raise TypeError(f"The return type of a function must match the parameter types")
            else:
                _has_scalar_type = True
                # TODO: Not sure what to do right now with this part of the function
        return True

    def _verify_function_params_match(self) -> bool:
        """want to make sure if one param is of type series, then another param matches, and should not be a
        scalar value.

        May be deprecated in the future in favor of making sure that the return and parameter of
        the majority matches

        :return:
        """
        for check, check_args in self.function_param_and_type.items():
            _has_series_type = False
            _has_scalar_type = False
            for param, p_type in check_args.items():
                if isinstance(p_type, pd.Series):
                    _has_series_type = True
                elif isinstance(p_type, pd.DataFrame):
                    raise NotImplementedError(
                        f"We currently do not support Dataframes as a check argument."
                        f" Please update the check function to include either on pd.Series, list-like, or scalar values"
                    )
                else:  # we're going to assume single values for now and not lists. Those will error out for now.
                    _has_scalar_type = True
            if _has_series_type and _has_scalar_type:
                raise ValueError(f"The function cannot have both a scalar parameter and a series parameter."
                                 f" Please update the function to include one or the other, but not both")
        return True

    def _verify_alias_mapping(self) -> bool:
        """verifies that alias mapping was properly defined and usable.

        :return: True if the alias mapping was properly defined and usable, raises an error otherwise
        """
        columns = {column: False for column in self.df.columns.to_list()}

        invalid_field_names = []
        if self.alias_mapping:
            for param in self.alias_mapping:
                param_results = []
                for arg in self.alias_mapping[param]:
                    try:
                        columns[arg] = True
                        param_results.append(True)
                    except KeyError:
                        param_results.append(False)
                if not any((result for result in param_results)):  # because none of the fields we defined are in the df
                    invalid_field_names.append(param)

        if invalid_field_names:
            raise KeyError(
                f"{invalid_field_names} were not in the dataframe. "
                f"Please update the `alias_mapping` variable with the correct field names."
            )
        return True

    def _verify_column_ties_to_parameter(self) -> bool:
        """verifies that all the parameters in the super class has atleast one associated column
        tied to it.

        If that check fails, it will raise an error, and will notify the user to add the column
        to the `alias_mapping` variable so that is ties to the parameter it's missing,
        or to remove the function altogether if it's irrelevant.
        """
        column_mapping = {column: False for column in self.df.columns.to_list()}
        params = self.params_in_class

        # Tying Out the Columns
        missing_params = []
        for param, args in params.items():
            try:
                column_mapping[param]  # param name is not a column name
            except KeyError:
                if args is None:  # because `get_all_params_in_class` will make the value a list from alias or None
                    missing_params.append(param)
        if missing_params:
            raise KeyError(
                f"Missing columns: {missing_params}. Declare the columns associated with the missing parameters"
                f" inside the `alias_mapping` variable in the class."
            )
        return True

    def check_all(self) -> bool:
        """we'll hard code all the checks for now, but this can be made more dynamic in the future.
        Will run all the validations we have set up so far to ensure proper formatting

        Will raise an error if any of the functions fail

        :return: True / False
        """
        self._verify_alias_mapping()  # validates alias mapping is set up correctly
        self._verify_column_ties_to_parameter()  # validates that all parameters are accounted for
        self._verify_function_params_match()  # verifies that parameter types are uniform
        self._verify_function_param_return_match()  # need input for the type of func param
        return True


class _Deliverable:
    @staticmethod
    def run_summary(
            results: dict[str, ResultWrapper],
            group_name: str | None = None
    ) -> pd.DataFrame:
        """creates a basic summary dataframe with the pass / fail for each check.

        Columns:
            - Check Function Name: name of the function used for the validation
            - Total Records Validated: the number of records that were validated in total
            - Total Records Passed: the number of records that successfully passed the validation
            - Total Records Failed: the number of records that failed the validation
        """
        # todo this function and run_error_log should both be under the same class Result
        # this will allow you to do result.run_summary or result.run_validation, result.error_log

        # Summary without the validation_name
        if group_name is None:
            summary = {
                check_name: {
                    "Check Function Name": r.function_name,
                    "Check Description": r.function_description,
                    "Total Records Validated": r.result.count(),
                    "Total Records Passed": r.result.sum(),
                    "Total Records Failed": len(r.result) - r.result.sum(),
                } for check_name, r in results.items()
            }
        else:
            summary = {
                check_name: {
                    "Group": group_name,
                    "Check Function Name": r.function_name,
                    "Check Description": r.function_description,
                    "Total Records Validated": r.result.count(),
                    "Total Records Passed": r.result.sum(),
                    "Total Records Failed": len(r.result) - r.result.sum(),
                } for check_name, r in results.items()
            }
        result = pd.DataFrame(summary).T.reset_index().sort_values("Total Records Failed", ascending=False)
        return result

    @staticmethod
    def run_error_log(
            results: dict[str, ResultWrapper],
            df: pd.DataFrame,
            group_name: str | None = None
    ) -> pd.DataFrame:
        """gives you a Dataframe with the records that failed the validation
        """
        results_filter = []
        for check_name, r in results.items():
            temp_df = df[~r.result].copy()
            temp_df["Check"] = r.function_name
            if group_name is not None:
                temp_df["Group"] = group_name
            results_filter.append(temp_df)

        _df_results = pd.concat(results_filter, ignore_index=True)
        return _df_results


# 2025.02.26 - creating new architecture for the quick pipeline
# we're doing this to make setup easier since it got a little complicated
# and time-consuming when onboarding new clients
class BaseValidation:
    """object used for Validation objects in order to help in reusing the same set of checks
    over different reports.

    ...

    The class should be inherited by a core set of classes `MyValidationSet(BaseValidation)`, where
    the `MyValidationSet` has defined checks as we will go over below. You could then layer these checks by
    inheriting the previous class SecondLayerValidation(MyValidationSet) with its own set of checks.

    The object inheriting BaseValidation class then uses the check function, `function_name = defined_function`

    ...

    For any columns that don't match directly with the parameter name, you can use `alias_mapping` to tie the parameter
    to the field name. Ex: `{price: [Total Price,], loss: [Total Loss,]}`

    We could even treat the alias_mapping as a dictionary,
    defining it as `alias_mapping[price] = [Total Price,]` and repeating for each parameter we want to define.

    ...

    Essentially, the inheriting class will have a set of functions (returns a pd.Series) it wants to run,
    this class will compile the returned pd.Series from the defined functions, and create summaries and
    dataframes based on the boolean value of the results.

    This object will infer the arguments based on the column names / alias_mapping, in order to get
    the results from the functions.
    ...

    """

    _validator: _BaseValidationCheck = _BaseValidationCheck
    _formatter: DirlinFormatter = DirlinFormatter()
    """class level utility functions for parsing strings, numbers, and pd.Series"""

    _report_creation: _Deliverable

    # 2025.05.29: we're going to add logging capabilities to the validation pipeline
    _logger = logging.getLogger("dirlin.core.validation")
    """used for logging to the console"""
    _logger.setLevel(logging.INFO)
    tqdm_logger = TqdmLoggingHandler()  # logging to stay in line with tqdm
    _logger.addHandler(tqdm_logger)

    @classmethod
    def _run_validation(cls, df: pd.DataFrame) -> dict[str, ResultWrapper]:
        """main function for BaseValidation, and runs the validation functions under the class,
        and returns a dictionary.

        Parameters:
            - df: pd.DataFrame -> accepts a dataframe and will return the pass / fail for each check

        """
        # STEP 0: 2025/05/02 adding the alias pull here for visibility
        cls._logger.info(f"Running Validation...")
        cls.alias_mapping = cls._get_all_alias_mapping_in_class()  # should pull alias from subclass too

        # STEP 1: VALIDATE to validate that the class was set up correctly and is usable (GLOBAL)
        # todo: This portion needs to be updated so that checks are actually working
        _verify = cls._validator(
            df,
            cls._get_function_return_type(),
            cls._get_function_param_and_type(),
            cls._map_param_to_alias(),
            cls.alias_mapping,
        ).check_all()

        # STEP 2: INITIALIZE to create all the maps, to prepare for running the checks
        # todo: this portion needs to be cleaned so that the steps are more clear

        # What are the values that we need?
        # The function parameter names
        # The column names on the dataframe

        # What mapping do we need?
        # Parameter Name: Column Name
        # Function: Parameter

        # If we have a list of functions, we can tie arguments based on position and kwargs

        function_name_to_args_mapping = cls._map_function_to_args(df)  # gives me the func_name and (param and arg) tup
        """function used to pull the check functions, which includes the name, params, and args.
        This creates associations between the checks we have with the report columns we want to use as arguments.
        """

        function_mapping = cls._get_all_functions_in_class()  # gives me each function to iterate through
        """creates a mapping of the check_name, and the actual function code that goes with it.
        Helpful for creating an iterable for the next few steps
        """

        function_to_function_type_map = cls._map_function_to_function_type()  # gives me the function type (param?)
        """current limitation of the pipeline, but this function determines the parameter types, and ensures that
        we return the current type. This is needed because we can't return scalar values when the parameters are
        expecting a Series type.
        """

        function_name_to_docstring_mapping = cls._get_all_function_docstrings()  # gives me the docstrings for the funcs
        """creates a mapping of {check_name: docstring}, which we use as one of the columns in our final deliverable
        to describe what each check is accomplishing and checking for.
        """

        # STEP 3: RUN THE CHECKS
        results = cls._process_function_with_args(
            df,
            function_map=function_mapping,
            function_args=function_name_to_args_mapping,
            function_type_map=function_to_function_type_map,
            function_docs=function_name_to_docstring_mapping
        )
        return results

    @classmethod
    def _process_function_with_args(
            cls,
            df: pd.DataFrame,
            *,
            function_map: dict,
            function_type_map: dict[str, bool],  # true is series
            function_args: dict[str, list[dict]],
            function_docs: dict[str, str],

    ) -> dict[str, ResultWrapper]:
        """identifies the type of function we are dealing with, and will run the
        function and its arguments according to its needs. For example, a parameter of type pd.Series will
        run differently than a function running based on float.

        Parameter:
            - function_map: gives you a {check_name: check} mapping that helps with iterating through all checks
            - function_args: gives you a list of parameter to column pairs. Each member of list
            has the same set of parameter keys, but different column values.
            - function_type_map: gives you the return and parameter types for the functions so we know
            how to handle them
            - function_docs: gives you the docstrings for the functions

        """
        results = {}
        pbar = tqdm(total=len(function_map), desc="Processing validations")
        for function_name, function in function_map.items():
            pbar.set_description(f"Running: {function_name}")
            # function_args has the same key as function_map
            param_args_list = function_args[function_name]  # should be list of {param: col}
            match function_type_map[function_name]:
                case True:
                    result = cls._process_function_as_series_function(
                        function,
                        function_name,
                        df,
                        param_args_list,
                        function_docs[function_name],
                    )
                case _:
                    result = cls._process_function_as_scalar_function(
                        function,
                        function_name,
                        df,
                        param_args_list,
                        function_docs[function_name],
                    )
            temp = {r.parameters_used: r for r in result}
            results = results | temp  # todo 2025.05.05 thinking this needs to be the ResultsWrapper to make API clear
            pbar.update()
        cls._logger.info(f"Validations complete!")
        return results

    @classmethod
    def _process_function_as_series_function(
            cls,
            function: Callable,
            function_name: str,
            df: pd.DataFrame,
            args_list: list[dict[str, str]],
            docs: str,
    ) -> list[ResultWrapper]:
        """processes the class function assuming every parameter has a pd.Series as the param type, and also
        returns a pd.Series type

        :param function: function to run the args on
        :param df: the dataframe to run the args on
        :param args_list: a list of `param: column` pairs

        :return: _ResultWrapper
        """
        # Step 1: Iterate Through Args Pair
        # We need this because arg list only gives you the field name and doesn't tie it to an existing DF
        new_keys = {
            cls._formatter.convert_dict_to_ref_names(args_pair, "series"): {
                param: df[column] for param, column in args_pair.items()
            } for args_pair in args_list
        }  # this should look like `check_name`: `{param: pd.Series}` aka args_pair
        # want to note that args_pair does not return check_name, as the name of the function, but
        # will return as the combination of all the parameters (2025/05/05)

        # Adding in the new docstring functionality (2025.05.07)

        # todo 2025.05.07 make the process_function (x2) more OOP -- the ResultWrapper should be handled on level up
        # list comprehension to get all the results as a list
        deliverable = [
            ResultWrapper(
                result=function(**args),
                parameters_used=name,
                function_name=function_name,
                function_description=docs,
            )
            for name, args in new_keys.items()
        ]
        return deliverable

    @classmethod
    def _process_function_as_scalar_function(
            cls,
            function: Callable,
            function_name: str,
            df: pd.DataFrame,
            args_list: list[dict[str, str]],
            docs: str,
    ) -> list[ResultWrapper]:
        """processes the class function assuming every parameter has a scalar as the param type, and returns
        a single scalar type as well

        :param function: the callable function we will be validating the Dataframe with
        :param df: the dataframe to run the args on
        :param args_list: a list of `param: column` pairs

        Note that when you iterate through the args_list, you will get a different combination of columns.

        arg_list is from a higher level, and gives you a mapping of the columns involved with the function
        you are using. func1(param1: column1, param2: column2, param3: column3)

        :return: list of ResultWrappers
        """
        deliverable = []
        for args_pair in args_list:
            # Step 1: rename the column names to the parameter names so that we can unpack as args
            column_rename_map = {column: param for param, column in args_pair.items()}
            temp: pd.DataFrame = df[list(column_rename_map)].copy()  # filter to keep only function context
            temp = temp.rename(columns=column_rename_map)  # rename for arg unpacking

            # Step 2: convert to a dictionary and unpack the rows in the dataframe in the function
            # now we have a list of records with corresponding args
            # we can use this list to run through the scalar functions since this is a records dict
            list_of_temp_args = temp.to_dict(orient='records')
            results = [function(**args) for args in list_of_temp_args]

            # Step 3: Formatting and Return
            check_name = cls._formatter.convert_dict_to_ref_names(args_pair)
            deliverable.append(
                ResultWrapper(
                    result=pd.Series(results),
                    parameters_used=check_name,
                    function_name=function_name,
                    function_description=docs,
                )
            )
        return deliverable

    @classmethod
    def _map_function_to_args(cls, df: pd.DataFrame) -> dict[str, list[dict]]:
        """identifies whether a param has a one-to-one or a one-to-many relationship with a column.
        Once identified, will flatten fields with a one-to-many, and will create a list of
        function args. Keeps in context of the check that the parameters are under to ensure
        that the arg dicts we made tie out correctly to the function. This is important so that the
        function argument list does not have a parameter that is not associated with the function.

        :return: a dictionary with key-value pairs of {`check`: [{p1: c1, p2: c2, ...}], ...}
        """
        check_mapping = cls._get_function_param_and_type()
        param_mapping = cls._map_param_to_columns(df)
        deliverable = {}

        for check, param_set in check_mapping.items():
            # STEP 1 is to categorize whether the param has a one-to-one or one-to-many relationship
            # We do this by comparing the length of the field list we have from the `get param column mapping` func
            one_to_one = {}
            one_to_many = {}
            for param, fields in param_mapping.items():
                if param in param_set:  # if this param is part of the check
                    if isinstance(fields, str):
                        fields = [fields, ]
                    if len(fields) == 1:  # only one field associated with the param. Previously known as static
                        one_to_one[param] = fields[0]
                    elif len(fields) > 1:
                        one_to_many[param] = fields
                    # ZERO param args should be caught in `verify_column_ties_to_param`

            # STEP 2 is to flatten out the parameter-column mapping for one-to-many relationships
            # we do this so that we can iterate on the parameters that can take in different columns
            # if we have stock_a_price and stock_b_price with stock_price as the parameter
            # we want to be able to use both stock_a and stock_b with the other parameters in the function

            # NOTE: realized that if I want to reference the params being used in the final deliverable,
            # NOTE cont: I need to be able to create a dict instead of a list to so that I can reference
            # NOTE cont: the changed parameter names
            flat_one_to_many = cls._formatter.convert_dict_to_records(one_to_many)
            arg_sets: list[dict] = [one_to_one | one_to_many_args for one_to_many_args in flat_one_to_many]  # the args

            # 2025.03.21 previous way added too much complexity. Fixed to keep it clean.
            deliverable[check] = [args for args in arg_sets]
        return deliverable

    @classmethod
    def _map_param_to_columns(cls, df: pd.DataFrame) -> dict[str, Any]:
        """creates the actual mapping between the parameter and the columns associated with it,
        `_map_param_to_alias` ties out the alias to the parameters. This class then goes
        one step further by adding any columns that match the name of the parameter.

        :param df: the dataframe we want to use
        :return: dictionary with key-value pair of `{parameter: [column, alias]}`
        """
        params = cls._map_param_to_alias()
        column_mapping = {column: True for column in df.columns}
        param_column_mapping = dict()

        for param, alias_names in params.items():
            try:
                column_mapping[param]  # checks if the parameter has direct match in dataframe
                if param not in param_column_mapping:
                    param_column_mapping[param] = [param]  # we want to start it in list form
                elif isinstance(param_column_mapping[param], list):
                    param_column_mapping[param].append(param)
                elif isinstance(param_column_mapping[param], str):
                    param_column_mapping[param] = [param_column_mapping[param], param]
                else:
                    raise TypeError(f"{type(param_column_mapping[param])} is not a list or str")
            except KeyError as KE:  # parameter name was not in the dataframe columns
                # we should check if there's an alias
                if alias_names is None:
                    raise KE

                # At this point we are assuming that there is an alias name and no matching column
                # Now we add the params based on the number of alias we see
                if isinstance(alias_names, str):
                    param_column_mapping[param] = alias_names
                    continue
                # we will only accept the Parameters if the Alias Name is in the Dataframe we are planning on using
                param_column_mapping[param] = [name for name in alias_names if column_mapping.get(name) is not None]
        return param_column_mapping

    @classmethod
    def _map_function_to_function_type(cls) -> dict[str, bool]:
        """creates a mapping so that we can tell the type of function we are dealing with.
        For example, currently, we are trying to figure out if the function uses a pd.Series type params
        or if it uses scalar values (float, int, etc.) as arg types. We can't have both.

        {`check`: `True` if series-based, `False` if scalar}

        """
        deliverable = {}
        for check, args in cls._get_function_param_and_type().items():
            _has_series_type = False
            _has_scalar_type = False
            for param, p_type in args.items():
                if p_type == pd.Series:
                    _has_series_type = True
                elif isinstance(p_type, pd.DataFrame):
                    raise NotImplementedError(
                        f"We currently do not support Dataframes as a check argument."
                        f" Please update the check function to include either on pd.Series, list-like, or scalar values"
                    )
                else:  # we're going to assume single values for now and not lists. Those will error out for now.
                    _has_scalar_type = True
            if _has_series_type and _has_scalar_type:
                raise ValueError(f"The function cannot have both a scalar parameter and a series parameter."
                                 f" Please update the function to include one or the other, but not both")
            if _has_scalar_type:
                deliverable[check] = False
            else:
                deliverable[check] = True
        return deliverable

    @classmethod
    def _get_function_param_and_type(cls) -> dict:
        """private helper function used to get the class function's parameters and its associated type

        :return: a dictionary of `check: {parameter: Type, ...}` for all functions defined in the subclass.
        """
        _function_params = {
            check_name: {
                parameter: t
                for parameter, t in inspect.get_annotations(function).items() if parameter not in ('return',)
            } for check_name, function in cls._get_all_functions_in_class().items()
        }
        return _function_params

    @classmethod
    def _get_function_return_type(cls) -> dict:
        """private helper function that gets the class function's return

        :return: dictionary of {`check`: `return_type`}
        """
        check_return_annotation = {
            check_name: inspect.get_annotations(function)
            for check_name, function in cls._get_all_functions_in_class().items()
        }

        return_type_mapping = {
            check: param_pairs["return"] if "return" in param_pairs else None
            for check, param_pairs in check_return_annotation.items()
        }
        return return_type_mapping

    @classmethod
    def _get_all_alias_mapping_in_class(cls) -> dict:
        """private helper function used to get the alias mapping from all previous subclasses without
        overwriting previous values.

        Alias Mapping is a key-value pair of a function parameter (used for checks)
        with the column name on a report. We use this when column names on a report
        don't match exactly with the parameter name of a function. For example,
        if the function parameter is `price` and the column name is `Stock Price`.
        """
        all_alias_mapping: dict | None = None
        for subclass in cls.__mro__[:-1]:
            if "alias_mapping" in subclass.__dict__:
                temp = subclass.__dict__["alias_mapping"]  # holds the user-defined alias_mapping dict

                # initialization of all_alias_mapping
                # adding here, so we can run different logic once it's up and running
                if all_alias_mapping is None:
                    all_alias_mapping = temp
                    continue

                # now I want to check to ensure we're not overriding values from the higher level classes
                overide_param = {}
                for param, col in temp.items():
                    try:
                        all_alias_mapping[param]  # check for keyword in dict
                    except KeyError:  # is a new parameter we want to add
                        overide_param[param] = col
                all_alias_mapping = all_alias_mapping | overide_param
        return all_alias_mapping


################################
# THIRD ITERATION
################################



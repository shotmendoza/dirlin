import inspect
from typing import Collection

from pandas import DataFrame

from dirlin.dq.core import FuncObj, DataSource, InterfaceCheckObj, ResultWrapper


# [2025.08.29] I AM CURRENTLY HERE. JUST COMPLETED WIRING OUT THE CLASSES.
# PIPELINE: USER INTERACTS WITH THIS AS A SUBCLASS TO USE. DEFINES CHECKS, REPORTS, and Alias Mapping
# DataSource: This is the report the user wants to check through.
# FuncObj: This is a object representing the function, and handles everything related to functions
# InterfaceCheckObj: Object responsible for managing the DataSource, Function, Results, AliasMap
# AliasMap: Handles the creation of AliasRecords

class Pipeline:
    """Handles running checks on a class level

    Properties we expect from the subclasses inheriting from the Pipeline (important when we run the checks)
        1. `alias_mapping` to tie out the columns and parameters
        2. `reports` to tie out the dataframes we want to run through the checks
    """
    _register: list = []

    alias_mapping: dict[str, Collection | str] | None = dict()
    """user defined mapping, that ties the column names in the data (report) to the parameter names in the check
    Used to define columns that don't exact-match a parameter in the object,
    but we want to use as an argument in the parameter.

    For example, if we have a column `Total Price` but our test function uses `price`
    as the parameter of the function, we would add `Total Price` as the value under
    `price` in the alias_mapping key-value pair. This would look like this:
    `{"price": ["Total Price]"}`.

    Is a key-value pair of {`parameter name`: [`associated columns`]}, and will tie into
    the function. The error code for `_verify_column_ties_to_parameter` will also notify
    you to add missing parameters into this variable as a dict."""

    def __init__(self):
        """Base Class for handling the Function validation. Will prep the function side of the equation
        so that no arise will come up when processing.

        1. Gets all the functions and their names
        2. Will grab the docstrings for the functions
        3. Will grab the function signatures
        """

        # Property for holding all functions
        self._all_functions: Collection[FuncObj] | FuncObj | None = None
        """property that holds data regarding all the functions in the class (will be relevant when inheriting)
        """

        # === properties for running the checks ===
        self._data_sources: Collection[DataSource] | DataSource | None = None
        """The reports being checked by the Pipeline"""
        self._checks: Collection[FuncObj] | FuncObj | None = None
        """The checks being run by the Pipeline"""
        self.results = None
        """Holds the results from the `run()` function"""

        # === INITIALIZING THE OBJECTS WE NEED ===
        self._initialize_functions()  # => initializes the DataSource and FuncObj for the checks
        self._initialize_aliases()  # => initializes the alias_mapping for global param: column recognition
        self._initialize_properties()  # => initializes any DataSources listed as properties

        # === Initializing the Interfaces ===
        self._manager = InterfaceCheckObj(self.checks, alias_mapping=self.alias_mapping)  # with initial values
        """The interface that facilitates protocol between FuncObj, Aliases, DataSource, Results"""

    def __init_subclass__(cls, **kwargs):
        """init for the subclass, we'll keep track of the subclasses that inherits from this base class
        so that we can run all the validations at the same time.
        """
        super().__init_subclass__(**kwargs)

        # confirm subclasses have alias_map defined
        subclass_defined_properties = cls.__dict__.keys()
        Pipeline._register.append(cls)

    @property
    def data_sources(self) -> Collection[DataSource]:
        """Read-Only Data Source property that holds all the DataSources currently in the Pipeline to be checked."""
        return self._data_sources

    @property
    def checks(self) -> Collection[FuncObj]:
        """Read-only property that holds all the checks currently being run by the Pipeline."""
        return self._checks

    @classmethod
    def run_subclass(cls):
        """runs the checks that were assigned for all objects that have inherited from the Pipeline baseclass"""
        result_mapping = dict()
        for subclass in cls._register:
            try:
                result_mapping[subclass.__name__] = subclass().run()
            except Exception as exc:
                raise exc
        return result_mapping

    def _initialize_functions(self) -> None:
        """gets all functions defined by the user in the subclass. The list of functions then get split into those
        that are factories for the reports we are checking, and the checks that are being used when the pipeline
        is running.
        """
        # (1) Pulling all the functions under the class and subclass
        all_functions = list()
        for validation_class in inspect.getmro(self.__class__)[:-2]:
            temp_functions = [
                FuncObj(fn=function) for name, function in validation_class.__dict__.items()
                if (inspect.isfunction(function) or inspect.ismethod(function)) and function not in all_functions
            ]
            all_functions.extend(temp_functions)
        self._all_functions = all_functions

        # [2025.08.29] - (!) assumes that any function returning a pd.DataFrame is report for now
        data_sources = list()
        if self._checks is None:
            self._checks = list()
        for fn_obj in all_functions:
            # (2) Getting DataSource Factories
            if fn_obj.return_type_fn == DataFrame:
                try:
                    _args = fn_obj.signatures.bind_partial()
                    _args.apply_defaults()
                    temp_data = DataSource(name=fn_obj.name, data=fn_obj.fn(*_args.args, **_args.kwargs))
                except TypeError as TE:
                    print(f"Running function {fn_obj}")
                    raise TE
                except ValueError as VE:
                    # (!) WE COULD SAVE THESE ERRORS SOMEWHERE FOR VISIBILITY
                    print(f"Running function {fn_obj}: {VE}")
                    continue   # don't want to add it as a datasource if it errors out
                except FileNotFoundError as FNF:
                    print(f"Running function {fn_obj}: {FNF}")
                    continue  # not adding in this case either
                except Exception as E:
                    raise E
                data_sources.append(temp_data)
                continue
            # (3) Getting the Checks => since we're assuming non-pd.DataFrame is a check
            self._checks.append(fn_obj)
        self._data_sources = data_sources  # saves the DataSource factory fn we ran as a source
        return None

    def _initialize_aliases(self) -> None:
        """goes down the inheritance chain and pulls the alias_mapping from each subclass.
        We centralize this data so that it can be used at any point in time.
        """
        # COMBINES ALL CLASS LEVELS OF ALIASES INTO ONE ALIAS
        # The idea is to take all the user defined alias_mapping from each subclass and make it into one dictionary
        aliases: dict | None = None
        for subclass in inspect.getmro(self.__class__)[:-1]:
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
        self.alias_mapping = aliases
        return None

    # TODO [2025.09.08] add initializing properties so you can grab DataSource types
    # to go one step further, even grabbing dataframes
    def _initialize_properties(self) -> None:
        """goes down the inheritance chain and pulls the properties from each subclass."""
        _data_sources = list()
        for s_class in inspect.getmro(self.__class__)[:-1]:
            # (!) currently only looking for DataSources or DataFrames to turn into data sources
            temp_data_source = [
                DataSource(field, data) if isinstance(data, DataFrame)
                else data
                for field, data in s_class.__dict__.items()
                if isinstance(data, DataSource)
            ]
            _data_sources.extend(temp_data_source)
        for d in _data_sources:
            if self._data_sources is None:
                self._data_sources = d
                return None
            elif isinstance(self._data_sources, Collection) and isinstance(self._data_sources, list):
                if d not in self._data_sources:
                    self._data_sources.append(d)
        return None

    ####################
    # PUBLIC APIs
    ####################
    def load_dataframe(
            self,
            name: str,
            data: DataFrame
    ) -> None:
        """loads the DataSource to the Pipeline for use with the validation, using a pandas Dataframe and name
        """
        if self._data_sources is None:
            self._data_sources = []
            self._data_sources.append(DataSource(name, data))
        elif isinstance(self._data_sources, DataSource):
            self._data_sources = [self._data_sources, DataSource(name, data)]
        elif isinstance(self._data_sources, list):
            self._data_sources.append(DataSource(name, data))
        else:
            raise TypeError(f'data_sources must be a DataSource or a list of DataSources, got {self._data_sources}')

        # TODO [2025.09.05] THIS IS WHERE WE NEED TO CONFIRM THAT THE DATAFRAME MATCHES AS EXPECTED WITH PARAMS
        # This is helpful for error handling when we're running and if we can get a quick `preview`
        # and see how the DataFrame integrates with the checks, then we'd be able to differentiate
        # between a check and one that returns a message

    def load_datasources(
            self,
            data_sources: Collection[DataSource] | DataSource
    ) -> None:
        """loads a Datasource to the Pipeline with the user giving the function a list of DataSources or a single
        DataSource.
        """
        if self._data_sources is None:
            self._data_sources = []

        # TODO [2025.09.05] WE ALSO NEED TO CONFIRM SOMEWHERE THAT THE DATASOURCE MATCHES AS EXPECTED WITH PARAMS
        # This is helpful for error handling when we're running and if we can get a quick `preview`
        # and see how the DataFrame integrates with the checks, then we'd be able to differentiate
        # between a check and one that returns a message

        # Handling loading a single Data Source
        if isinstance(data_sources, DataSource):
            if isinstance(self._data_sources, DataSource):
                self._data_sources = [self._data_sources, data_sources]
            elif isinstance(self._data_sources, list):
                self._data_sources.append(data_sources)
            else:
                raise TypeError(f'data_sources must be a DataSource or a list of DataSources, got {self._data_sources}')
            return None

        # Handling loading a list of Data Sources
        if isinstance(self._data_sources, DataSource):
            self._data_sources = [self._data_sources]
        if isinstance(data_sources, Collection) and not isinstance(data_sources, str):
            for data_source in data_sources:
                self._data_sources.append(data_source)
        return None

    def run(self) -> ResultWrapper:
        """runs the checks that was assigned to this specific instance, and applies the checks to the report.
        Each report will generate a Result object, that can later be used as an Error Log or Error Summary so that
        the end user can apply further actions to errors.
        """
        # [1.1] Gather all the reports and data sources that are getting checked
        if isinstance(self._data_sources, DataSource):
            updated_source = [self._data_sources]
        elif self._data_sources is None:
            updated_source = []
        else:
            updated_source = self._data_sources

        # [1.2] Initialize the Manager object that will run the checks
        # _manager = InterfaceCheckObj(self.checks, alias_mapping=self.alias_mapping)
        managers = [self._manager.build(source) for source in updated_source]

        # We can do this because we added __add__ that allows us to combine Result objects
        result = None
        for manager in managers:
            if result is None:
                result = manager
                continue
            result += manager
        if result is None:
            return ResultWrapper()
        self.results = result
        return self.results

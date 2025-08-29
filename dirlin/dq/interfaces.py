import dataclasses
import inspect
from typing import Callable

import pandas as pd


@dataclasses.dataclass
class ResultWrapper:
    """used as a wrapper for the results coming from either a scalar function
    or a Series type function.

    Fields:
        - `result`: a pd.Series type variable that holds the results from running the function
        - `parameters_used`: a str type variable that holds the string name of the check that will be put in the
        deliverable the name is the name of all the parameters combined, and currently, not the name of the function
        - `function_name`: a str type variable signifying the name of the function used to validate
    """
    result: pd.Series
    """stores the results of the validation in the BaseValidation object"""

    parameters_used: str
    """stores the name of the check that will be put in the deliverable"""

    function_name: str
    """name of the function we are running"""

    function_description: str
    """description of what the function checks for based on docstrings"""


@dataclasses.dataclass
class DataSourceLifecycleInterface:
    """interface used between the Validation and the DataSource. Communicates the missing columns and parameters
    from the Report that the Validation expected, and will return which functions did not run and which columns
    were missing causing the skip. Keeps track of the lifecycle of the DataSource to maintain observability
    """
    ########################
    # From Validation
    ########################
    fn_signature_map: dict = dataclasses.field(default_factory=dict)
    """Mapping of the functions and signature that the validation expects from the reports."""

    ########################
    # Observation Flags
    ########################
    _is_validation_completed: bool = False
    """determines whether the process of running the checks have been complete"""

    _is_skipped_functions: bool = False
    """flag indicating whether all functions ran, or True if some of the functions were skipped"""

    skipped_functions: list = dataclasses.field(default_factory=list)

@dataclasses.dataclass
class PipelineDataCheckInterface:
    """holds everything we need to run these checks on a one-to-one DataSource to Validation level.
    """




@dataclasses.dataclass
class PipelineSubclassInterface:
    """the interface / protocol for communication between the Data Source, Pipeline, and the Result objects.
    Each of the objects will have a method that can be invoked that will pull in the necessary information
    that it needs in order to complete a request
    """
    ###############################
    # INTERFACE CHECKS AND CONFIGS
    ###############################
    _missing_columns: dict[str, list]
    """stores the data on any DataSources that are missing the expected columns based on the params"""

    ######################
    # FROM DATASOURCE
    ######################
    alias_mapping: dict | None = None
    """for storing alias related conversion and mapping"""
    reports: dict | None = None
    """used to store the name: report mapping, used to classify the reports for the final results"""
    records: dict | None = None
    """used to store records by DataSource"""

    ########################
    # FROM PIPELINE
    ########################
    name: dict | str | None = None
    """stores the pipeline name it was ran on"""
    fn_map: dict[str, Callable] | None = None
    """stores the fn name and the actual function so that the Bound Args can be run"""
    fn_signature_map: dict[str, inspect.Signature] | None = None
    """stores the function and the signature that was defined by the user in the Pipeline"""

    ##########################
    # From Results
    ##########################
    results: dict | None = None  # on Data Source level
    """stores the results from the pipeline"""

    #########################
    # FUNCTIONS
    #########################
    def get_pipeline_data(self) -> dict:
        """returns data that is usually populated by the Pipeline
        """
        return {
            "name": self.name,
            "fn_map": self.fn_signature_map,
            "fn_signature_map": self.fn_signature_map
        }

    def get_source_records(self) -> dict:
        """since we expect DataSource to create the records based
        on the Pipeline data, which is stored as a record, this
        function returns the records given by the DataSource
        """
        return self.records

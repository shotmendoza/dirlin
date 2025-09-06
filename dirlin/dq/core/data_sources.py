from typing import Any

import pandas as pd

from dirlin.dq.interfaces import DataSourceLifecycleInterface


class DataSource:
    registry = list()
    """Will be used to initialize all the Subclasses of Data Source so we can run the pipeline at once"""
    def __init__(
            self,
            source_name: str,
            data: pd.DataFrame,
            alias_mapping: dict[str, list | str] | None = None,
    ):
        """DataSource is the report object that is getting checked.

        :param source_name: name of the data source or what you want to name the report / dataframe you are checking
        :param data: the actual data or dataframe you are checking
        :param alias_mapping: a dictionary that maps column names to parameter names

        """
        # super().__init__(_obj=self, _reports=data)

        # === Init for Alias and Columns ===
        if alias_mapping is None:
            alias_mapping = dict()
        self._alias_mapping = alias_mapping
        self._columns = dict()

        # === Initializing the Data ===
        self.source_name = source_name
        self.data = self._run_verify_data(data)

        # === For Observability ===
        self.lifetime: DataSourceLifecycleInterface = DataSourceLifecycleInterface()
        """used to keep track of the object's lifetime and results. The pipeline will update this property"""

        # [2025.08.27] I don't think the DataSource needs to hold Validation or Pipeline information
        # # === Pipeline Related Data ===
        # self.pipeline: PipelineSubclassInterface | None = None
        # self.signatures: dict | None = None
        # self._all_functions: dict | None = None

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        cls.registry.append(cls)

    def _run_verify_data(self, data: pd.DataFrame) -> pd.DataFrame:
        self._verify_aliases()
        self._verify_columns()
        return data

    def _verify_aliases(self):
        ...

    def _verify_columns(self):
        ...

    def missed_functions(self, pipeline: DataSourceLifecycleInterface) -> DataSourceLifecycleInterface:
        """takes a ReportCheckInterface from the pipeline and returns one based on the results
        """

    # [2025.08.26] I don't think the pipeline is something that needs to be saved into the DataSource
    # def init_pipeline_fn(
    #         self,
    #         pipeline_info: PipelineSubclassInterface
    # ) -> Any:
    #     """function used to primarily initialize the Data Source so that it is usable with the pipeline.
    #     It will take the function mapping from the Pipeline in order to determine whether the `alias_map`,
    #     `columns` and any other properties are ready to go through the Pipeline.
    #
    #     The pipeline gives the information through the PipelineSubclass Interface to make it easier to communicate.
    #     """
    #     # Initializing its own data with the Pipeline data
    #     self.pipeline = pipeline_info
    #     self.signatures = self.pipeline.fn_signature_map.copy()
    #     self._all_functions = self.pipeline.fn_map.copy()
    #     return self

    # [2025.08.26] same idea, but the actual validation should be run in validation or pipeline. Not the DataSource
    # def run_pipeline_fn(self) -> dict[str, list]:
    #     """applies the class function to a single DataFrame and returns the results based
    #     on the records that were saved earlier.
    #     """
    #     if self.pipeline is None:
    #         raise ValueError("`init_pipeline_fn` must be called before `run_pipeline_fn`.")
    #
    #     result = {}
    #     for name, fn in self._all_functions.items():
    #         # Keep tabs on this. We might just be keeping the kwargs portion
    #         if not str(name).startswith("__"):
    #             fn_output = [fn(*record.args, **record.kwargs) for record in self.fn_records[name]]
    #             result[name] = fn_output
    #     self.pipeline.results = result
    #     return result

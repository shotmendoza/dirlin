import pandas as pd
import pytest

from dirlin import FolderPath
from dirlin import Pipeline, Report, Check, Validation


class TestPipeLine:
    f = FolderPath(path="/Volumes/Sho's SSD/trading")

    report_for_formatting = Report(
        name_convention="ohlcv",
        field_mapping={"High": "high", "Low": "low", 'Close': 'new_close'},
        column_type_cash=["high", "low"]
    )

    def _series_type_check_function(self, low: pd.Series, high: pd.Series) -> pd.Series:
        return pd.Series(low < high)

    new_check = Check(_series_type_check_function)
    validation = Validation(new_check)

    # df = qp.get_worksheet(report_for_formatting.name_convention, report=report_for_formatting)

    def test_pipeline_init(self):
        qp = Pipeline(self.f)

    def test_pipeline_run(self):
        qp = Pipeline(self.f)
        df = qp.get_worksheet("balance_sheet")
        assert isinstance(df, pd.DataFrame)

    def test_pipeline_run_with_reports(self):
        qp = Pipeline(self.f)
        r = Report(
            name_convention="ohlcv",
            field_mapping={},
            column_type_cash=["open"]
        )
        df = qp.get_worksheet(r.name_convention, report=r)
        assert isinstance(df, pd.DataFrame)

    def test_pipeline_run_with_validation(self):
        qp = Pipeline(self.f, report=self.report_for_formatting)
        qp._run_validation(self.validation)

    def test_pipeline_run_without_report_error(self):
        with pytest.raises(AttributeError) as ae:
            qp = Pipeline(self.f)
            qp._run_validation(validation=self.validation)
        assert (
                str(ae.value) == "Will need to run Pipeline.get_worksheet() "
                                 "or use a Report as an argument in Pipeline init."
        )


class TestCheckProcess:
    f = FolderPath(path="/Volumes/Sho's SSD/trading")
    qp = Pipeline(f)

    report_for_formatting = Report(
        name_convention="ohlcv",
        field_mapping={
            "High": "high",
            "Low": "low",
            'Close': 'foo_close',
            "Volume": "bar_close",
            "Open": "foobar_close"
        },
        column_type_cash=["high", "low"]
    )

    df = qp.get_worksheet(report_for_formatting.name_convention, report=report_for_formatting)

    def _series_type_check_function(
            self,
            low: pd.Series,
            high: pd.Series,
            new_close: pd.Series,
    ) -> pd.Series:
        return pd.Series(low < high)

    def non_series_check_function(self, low: float, high: float) -> bool:
        if high <= low:
            return False
        return True

    def test_create_check(self):
        new_check = Check(self._series_type_check_function)  # this is a check you would want to add
        result = new_check.validate(self.df)
        assert isinstance(result, pd.Series)

        non_series_check = Check(self.non_series_check_function)
        result = non_series_check.validate(self.df)
        assert isinstance(result, list)

    def test_multiple_types_check_function(self):
        series_check = Check(self._series_type_check_function)
        value_check = Check(self.non_series_check_function)
        validation = Validation([series_check, value_check])
        result = validation.run(self.df, infer_shared=True)
        print(result)
        print(result.info())

    def test_validation_run_shared_errors(self):
        """expect an error due to infer_shared being set to False and no column matching param directly"""
        with pytest.raises(ValueError) as ve:
            new_check = Check(self._series_type_check_function)
            validation = Validation(new_check)
            result = validation.run(self.df)

        assert (
                str(ve.value) == "Couldn't find matching column for parameters: ['new_close']. "
                                 "Consider making setting `infer_shared` to `True`."
        )

    def test_validation_run_shared_pass(self):
        """expect that the validation runs"""
        # We need to add a list later because there are instances where the keywords get out of sync
        new_check = Check(
            self._series_type_check_function,
        )
        validation = Validation(new_check)
        result = validation.run(self.df, infer_shared=True)

        print(validation.generate_error_log())

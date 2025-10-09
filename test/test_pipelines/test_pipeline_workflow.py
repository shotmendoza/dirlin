import pytest

from dirlin import Check, Validation, Report, Pipeline
from test.test_pipelines.test_pipeline_fixtures import single_stock_df, std_check_function, two_stock_df, \
    single_stock_df_b, series_check_function, same_name_check, similar_field_name_df

_shared_param_with_single_field = single_stock_df, std_check_function
"""testing class aims to test use cases where a test function has a shared parameter,
but the field in the DataFrame only has one field that is under the shared parameter.

For example, if a function has parameters(id, based_shared_field, static_field), then
we would expect that a dataframe that only had one column under that shared parameter (because
currently, the logic dictates that if no matching name is found, look at the core
of the word `shared_field`) would be a static field.

An example that got caught: `check_function_net_income(stock_gross_income, stock_expenses)`.
This was passed to a DataFrame that only had the following fields: `tsm_gross_income`, `tsm_expenses`.
Don't remember the error code but would want to catch these types of errors.
"""


_test_check_cases = [
    _shared_param_with_single_field,
    (two_stock_df, std_check_function),
]


@pytest.mark.parametrize("data, check", _test_check_cases)
def test_pipeline_validation_workflow(data, check):
    """parameterizing the actual check types with different types of checks

    :param check: the different types of check functions we might see in the wild
    :param data: just a standard DF we can use
    :return:
    """
    # ii) creating the pipeline
    check = Check(std_check_function)
    validation = Validation(check)
    df = data()

    # iii) Currently running it through pipeline since Pipeline is a little messed up
    result = validation.run(df, infer_shared=True)
    el = validation.generate_error_log()
    assert result is not None


def test_pipeline_workflow():
    """the aim of this test is to confirm that the Pipeline has a good workflow

    """
    # can probs be a fixture, but creating the Validation
    check1 = Check(std_check_function)
    validation = Validation([check1])

    # same, could be fixtures, but creating Report
    report1 = Report(df=single_stock_df())
    report2 = Report(df=single_stock_df_b())

    # Testing new pipeline api
    pipeline = Pipeline()

    pipeline.add_report_set(
        report=report1, validation=validation
    )

    pipeline.add_report_set(
        report=report2, validation=validation
    )

    x = pipeline.run_error_log()
    print(x)


def test_pipeline_workflow_series_function():
    check1 = Check(series_check_function)
    validation = Validation([check1])

    report1 = Report(df=single_stock_df())
    report2 = Report(df=single_stock_df_b())

    pipeline = Pipeline()

    pipeline.add_report_set(
        report=report1, validation=validation
    )

    pipeline.add_report_set(
        report=report2, validation=validation
    )

    y = pipeline.run_error_log()
    print(y)


def test_pipeline_workflow_similar_param_name():
    check1 = Check(same_name_check)
    validation = Validation([check1])

    report1 = Report(df=similar_field_name_df())

    pipeline = Pipeline()
    pipeline.add_report_set(
        report=report1, validation=validation
    )

    y = pipeline.run_error_log()
    print(y)

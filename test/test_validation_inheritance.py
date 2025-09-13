from datetime import date

from dirlin.dq.core import ResultWrapper
from dirlin.dq.pipelines import Pipeline
from test.helpers import (
    check_scalar_bool_parent,
    check_scalar_bool_child,
    check_series_bool_parent,
    check_series_bool_child,
    df_child_factory,
    df_parent_factory,
    check_scalar_bool_shared,
    check_series_bool_shared
)

# CHECKLIST
# (1) Parent and Child could have common aliases that they share (shared)
# (2) They also should have different parameters that are unique to themselves (foo, foobar)


class TestParent(Pipeline):
    # FUNCTIONS FOR CHECKS AND VALIDATIONS
    _check_scalar_bool_parent = check_scalar_bool_parent
    _check_series_bool_parent = check_series_bool_parent
    _check_scalar_bool_shared = check_scalar_bool_shared
    _check_series_bool_shared = check_series_bool_shared

    # FUNCTION FOR DATASOURCE FACTORY, THAT THE NEW PIPELINE SUPPORTS
    _parent_df = df_parent_factory


class TestChild(TestParent):
    # ALIAS MAPPING
    alias_mapping = {
        "foo": "foobar"
    }

    # FUNCTIONS FOR CHECKS AND VALIDATIONS
    _check_scalar_bool_child = check_scalar_bool_child
    _check_series_bool_child = check_series_bool_child

    # FUNCTION FOR DATASOURCE FACTORY, THAT THE NEW PIPELINE SUPPORTS
    _df_child_factory = df_child_factory


def test_init_parent_child():
    parent = TestParent()
    child = TestChild()

    parent_result = parent.run()
    child_result = child.run()

    assert isinstance(parent_result, ResultWrapper)
    assert isinstance(child_result, ResultWrapper)

    parent_result.as_dataframe(dtype="summary").to_csv(f"Test - Parent - {date.today()}.csv")
    child_result.as_dataframe(dtype="summary").to_csv(f"Test - Child - {date.today()}.csv")


def test_child_treatment_of_parent():
    """the parent object is not behaving as expected when the child object is the one being handled and run.
    The hope behind this test is to identify why the parent doesn't properly run its tests.
    """
    child = TestChild()
    result = child.run()

    result.as_dataframe(dtype="summary").to_csv(f"Test - Child - {date.today()}.csv")

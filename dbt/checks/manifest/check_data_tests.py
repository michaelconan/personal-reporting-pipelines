"""Custom manifest checks for dbt data-test descriptions."""

from dbt_bouncer.check_framework.decorator import check, fail
from dbt_bouncer.utils import clean_path_str, get_clean_model_name, is_description_populated


@check
def check_data_test_description_populated(test, *, min_description_length: int | None = None):
    """Data tests must document the business rule they enforce.

    The purpose of this check is to ensure every SQL test under ``tests/`` explains what
    condition it is validating and why the check exists. A populated description makes the test
    self-explanatory for reviewers and keeps governance expectations consistent with model docs.
    """
    resource_path = (
        getattr(test, "original_file_path", None)
        or getattr(test, "path", None)
        or getattr(test, "original_path", None)
        or ""
    )
    resource_path = clean_path_str(resource_path)
    if not resource_path.startswith("tests/"):
        return

    description = (
        getattr(test, "description", None)
        or getattr(getattr(test, "config", None), "description", None)
        or getattr(getattr(test, "meta", None), "description", None)
        or ""
    )
    if not is_description_populated(description, min_description_length or 20):
        fail(f"`{get_clean_model_name(test.unique_id)}` does not have a populated description.")

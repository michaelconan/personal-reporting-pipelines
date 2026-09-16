"""Custom manifest checks for dbt seeds."""

from dbt_bouncer.check_framework.decorator import check, fail
from dbt_bouncer.utils import get_clean_model_name


@check
def check_seed_has_unique_test(
    seed,
    ctx,
    *,
    accepted_uniqueness_tests: list[str] | None = [  # noqa: RUF012
        "dbt_expectations.expect_compound_columns_to_be_unique",
        "dbt_utils.unique_combination_of_columns",
        "unique",
    ],
):
    """Seeds must have a test for uniqueness of a column.

    !!! info "Rationale"

        Seeds are loaded directly into the warehouse from CSV files and are
        referenced by downstream models. Without a uniqueness test on the
        grain key, duplicate rows in a seed can silently fan-out joins and
        corrupt downstream counts. This check ensures every seed asserts
        uniqueness on at least one column.

    Parameters:
        accepted_uniqueness_tests (list[str] | None): List of tests accepted as
            uniqueness tests. Defaults to `unique`,
            `dbt_utils.unique_combination_of_columns`, and
            `dbt_expectations.expect_compound_columns_to_be_unique`.

    Receives:
        seed (SeedNode): The SeedNode object to check.
        ctx (CheckContext): Injected check context with artifact lookups.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | list[str] | None): Regex pattern(s) to match the seed path. Seed paths
            that match any pattern will not be checked.
        include (str | list[str] | None): Regex pattern(s) to match the seed path. Only seed
            paths that match any pattern will be checked.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_seed_has_unique_test
              include: ^seeds/
        ```
        ```yaml
        manifest_checks:
            - name: check_seed_has_unique_test
              accepted_uniqueness_tests:
                - dbt_utils.unique_combination_of_columns
                - unique
        ```

    """
    num_unique_tests = 0
    for test in ctx.tests_by_attached_node.get(seed.unique_id, []):
        test_metadata = getattr(test, "test_metadata", None)
        if test_metadata and (
            (
                f"{getattr(test_metadata, 'namespace', '')}.{getattr(test_metadata, 'name', '')}"
                in (accepted_uniqueness_tests or [])
            )
            or (
                getattr(test_metadata, "namespace", None) is None
                and getattr(test_metadata, "name", "") in (accepted_uniqueness_tests or [])
            )
        ):
            num_unique_tests += 1
    if num_unique_tests < 1:
        fail(
            f"`{get_clean_model_name(seed.unique_id)}` does not have a test for uniqueness of a column."
        )

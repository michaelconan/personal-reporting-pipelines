"""Custom manifest checks for dbt model unit-test placement and naming."""

from pathlib import Path, PurePosixPath

from dbt_bouncer.check_framework.decorator import check, fail
from dbt_bouncer.utils import clean_path_str, compile_pattern, get_clean_model_name


def _normalise_parent_directory(path: str | None) -> str:
    """Return the parent directory of a dbt artifact path in POSIX form."""
    normalised_path = (path or "").replace("\\", "/").lstrip("./")
    parent_directory = PurePosixPath(normalised_path).parent.as_posix()
    return "" if parent_directory == "." else parent_directory


def _get_unit_test_target_node_ids(unit_test) -> set[str]:
    """Return model unique_ids targeted by a dbt unit test node."""
    target_node_ids: set[str] = set()

    tested_node_unique_id = getattr(unit_test, "tested_node_unique_id", None)
    if isinstance(tested_node_unique_id, str) and tested_node_unique_id:
        target_node_ids.add(tested_node_unique_id)

    depends_on = getattr(unit_test, "depends_on", None)
    depends_on_nodes = getattr(depends_on, "nodes", None)
    if isinstance(depends_on_nodes, list):
        target_node_ids.update(node_id for node_id in depends_on_nodes if isinstance(node_id, str))

    return target_node_ids


def _get_attached_unit_tests(model, ctx) -> list:
    """Collect unit tests attached to a model from available bouncer context lookups."""
    attached_unit_tests_by_unique_id: dict[str, object] = {}

    for mapping_name in ("unit_tests_by_depends_on_node",):
        mapping = getattr(ctx, mapping_name, None)
        if not isinstance(mapping, dict):
            continue
        for unit_test in mapping.get(model.unique_id, []):
            unit_test_unique_id = getattr(unit_test, "unique_id", None)
            if isinstance(unit_test_unique_id, str) and unit_test_unique_id:
                attached_unit_tests_by_unique_id[unit_test_unique_id] = unit_test

    manifest = getattr(ctx, "manifest_obj", None)
    manifest_unit_tests = getattr(manifest, "unit_tests", None)
    if isinstance(manifest_unit_tests, dict):
        for unit_test in manifest_unit_tests.values():
            if model.unique_id in _get_unit_test_target_node_ids(unit_test):
                unit_test_unique_id = getattr(unit_test, "unique_id", None)
                if isinstance(unit_test_unique_id, str) and unit_test_unique_id:
                    attached_unit_tests_by_unique_id[unit_test_unique_id] = unit_test

    return list(attached_unit_tests_by_unique_id.values())


@check
def check_model_unit_test_location(model, ctx):
    """Unit tests for a model must be defined in the same folder as the model file.

    !!! info "Rationale"

        Co-locating model unit tests with their model keeps ownership clear and
        prevents drift where tests for one model are scattered across unrelated
        directories.

    Receives:
        model (ModelNode): The model node to check.
        ctx (CheckContext): Injected check context with artifact lookups.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | list[str] | None): Regex pattern(s) to match the model path. Model paths
            that match any pattern will not be checked.
        include (str | list[str] | None): Regex pattern(s) to match the model path. Only model
            paths that match any pattern will be checked.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
          - name: check_model_unit_test_location
            include: ^models/
        ```

    """
    model_directory = _normalise_parent_directory(getattr(model, "original_file_path", None))
    misplaced_unit_test_paths: list[str] = []

    for unit_test in _get_attached_unit_tests(model, ctx):
        unit_test_directory = _normalise_parent_directory(
            getattr(unit_test, "original_file_path", None)
        )
        if unit_test_directory != model_directory:
            misplaced_unit_test_paths.append(getattr(unit_test, "original_file_path", "<unknown>"))

    if misplaced_unit_test_paths:
        fail(
            f"`{get_clean_model_name(model.unique_id)}` has unit tests defined outside "
            f"`{model_directory}`: {', '.join(sorted(set(misplaced_unit_test_paths)))}."
        )


@check
def check_unit_test_file_name(unit_test, *, file_name_pattern: str):
    """Unit-test YAML files must use the project naming convention."""
    file_path = getattr(unit_test, "original_file_path", None)
    if not file_path:
        return

    file_name = Path(clean_path_str(file_path)).name
    pattern = compile_pattern(file_name_pattern.strip())

    if pattern.match(file_name) is None:
        fail(
            f"`{get_clean_model_name(unit_test.unique_id)}` is in a file named `{file_name}` "
            f"that does not match `{file_name_pattern}`."
        )

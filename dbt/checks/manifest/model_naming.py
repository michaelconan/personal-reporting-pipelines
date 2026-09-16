"""Custom manifest checks for dbt model naming conventions."""

from pathlib import Path

from dbt_bouncer.check_framework.decorator import check, fail
from dbt_bouncer.utils import clean_path_str, compile_pattern, get_clean_model_name


@check
def check_model_property_file_name(model, *, file_name_pattern: str):
    """Model property files must use the project naming convention."""
    file_path = getattr(model, "patch_path", None) or getattr(model, "original_file_path", None)
    if not file_path:
        return

    file_name = Path(clean_path_str(file_path)).name
    pattern = compile_pattern(file_name_pattern.strip())

    if pattern.match(file_name) is None:
        fail(
            f"`{get_clean_model_name(model.unique_id)}` has a properties file named `{file_name}` "
            f"that does not match `{file_name_pattern}`."
        )

from typing import Any
from jsonpath_ng import parse

# str: Sentinel value used to mark fields for deletion
SENTINEL = "__TO_DELETE__"


def _remove_sentinel(obj: Any, sentinel: str) -> Any:
    """Recursively remove fields with a sentinel value from a dictionary or list.

    The root object should be an object or list of objects, otherwise the function
    will return the object as is. This function is useful for cleaning up data
    structures where certain fields are marked for removal using a sentinel value.

    Args:
        obj (Any): Object to process, can be a dictionary, list, or primitive type.
        sentinel (str): Dedicated sentinel value to identify fields for removal.

    Returns:
        Any: Updated object with sentinel fields removed.
    """
    if isinstance(obj, dict):
        return {
            k: _remove_sentinel(v, sentinel) for k, v in obj.items() if v != sentinel
        }
    elif isinstance(obj, list):
        return [_remove_sentinel(v, sentinel) for v in obj if v != sentinel]
    else:
        return obj


def filter_fields(item: dict, paths: list[str]) -> dict:
    """Remove specified fields from a dictionary using JSONPath expressions.

    Args:
        item (dict): Record dictionary to apply filters to.
        paths (list[str]): One or more JSONPath expressions to filter out fields.

    Returns:
        dict: Updated dictionary with specified fields removed.
    """
    for path in paths:
        path_expr = parse(path)
        # path_expr.update(item, SENTINEL)
        path_expr.filter(lambda x: True, item)
    # return _remove_sentinel(item, SENTINEL)
    return item

from typing import Any, List, Optional
from requests import Request, Response
from dlt.common import jsonpath
from dlt.sources.helpers.rest_client.paginators import (
    OffsetPaginator,
    RangePaginator,
)


class HasMorePaginator(OffsetPaginator):
    """A paginator that uses offset-based pagination strategy.

    This paginator is a modified version of the OffsetPaginator that
    supports the `has_more` flag to determine if there are more pages
    to fetch. It is useful for APIs that provide a boolean flag indicating
    whether more data is available, rather than relying solely on the
    total count of items or a maximum offset.
    """

    def __init__(
        self,
        limit: int,
        offset: int = 0,
        offset_param: str = "offset",
        limit_param: str = "limit",
        total_path: Optional[jsonpath.TJsonPath] = None,
        maximum_offset: Optional[int] = None,
        stop_after_empty_page: Optional[bool] = True,
        has_more_path: Optional[jsonpath.TJsonPath] = "has_more",
    ) -> None:
        """
        Args:
            limit (int): The maximum number of items to retrieve
                in each request.
            offset (int): The offset for the first request.
                Defaults to 0.
            offset_param (str): The query parameter name for the offset.
                Defaults to 'offset'.
            limit_param (str): The query parameter name for the limit.
                Defaults to 'limit'.
            total_path (jsonpath.TJsonPath): The JSONPath expression for
                the total number of items.
            maximum_offset (int): The maximum offset value. If provided,
                pagination will stop once this offset is reached or exceeded,
                even if more data is available. This allows you to limit the
                maximum range for pagination. Defaults to None.
            stop_after_empty_page (bool): Whether pagination should stop when
              a page contains no result items. Defaults to `True`.
            has_more_path (jsonpath.TJsonPath): The JSONPath expression for
                a boolean indicating if there are more items to fetch.
        """
        if (
            has_more_path is None
            and total_path is None
            and maximum_offset is None
            and not stop_after_empty_page
        ):
            raise ValueError(
                "Either `has_more_path` or `total_path` or `maximum_offset`"
                " or `stop_after_empty_page` must be provided."
            )
        super().__init__(
            limit=limit,
            limit_param=limit_param,
            offset=offset,
            offset_param=offset_param,
            total_path=total_path,
            maximum_offset=maximum_offset,
            stop_after_empty_page=stop_after_empty_page,
        )
        self.has_more_path = has_more_path

    def update_state(
        self, response: Response, data: Optional[List[Any]] = None
    ) -> None:
        super().update_state(response, data)

        # Get has_more flag from the response and update paginator state
        values = jsonpath.find_values(self.has_more_path, response.json())
        has_more = values[0] if values else False
        self._has_next_page = has_more

    def __str__(self) -> str:
        return super().__str__() + f" has_more_path: {self.has_more_path}"

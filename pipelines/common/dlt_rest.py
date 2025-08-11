from typing import Any, List, Optional
import pendulum
from requests import Request, Response
from dlt.common.typing import TDataItem
from dlt.sources.helpers.rest_client.paginators import (
    BasePaginator,
)


class DateRangePaginator(BasePaginator):
    def __init__(
        self,
        start_date: pendulum.DateTime,
        end_date: pendulum.DateTime,
        step: pendulum.Duration,
        start_param: str = "start_date",
        end_param: str = "end_date",
        date_format: str = "YYYY-MM-DD",
    ):
        super().__init__()
        if not all([start_date, end_date, step, start_param, end_param]):
            raise ValueError(
                "start_date, end_date, step, start_param, and end_param must be provided."
            )

        self.start_date = start_date
        self.end_date = end_date
        self.step = step
        self.start_param = start_param
        self.end_param = end_param
        self.date_format = date_format
        self.current_start_date = start_date

    def init_request(self, request: Request) -> None:
        if request.params is None:
            request.params = {}

        self.update_request(request)

    def update_state(
        self, response: Response, data: Optional[List[TDataItem]] = None
    ) -> None:
        self.current_start_date += self.step
        if self.current_start_date >= self.end_date:
            self._has_next_page = False

    def update_request(self, request: Request) -> None:
        """Updates the request object with arguments for fetching the next page."""
        if request.params is None:
            request.params = {}

        current_end_date = self.current_start_date + self.step
        if current_end_date > self.end_date:
            current_end_date = self.end_date

        request.params[self.start_param] = self.current_start_date.format(
            self.date_format
        )
        request.params[self.end_param] = current_end_date.format(self.date_format)

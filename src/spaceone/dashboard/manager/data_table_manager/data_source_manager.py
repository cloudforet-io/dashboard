import logging
from typing import Literal, Tuple
from datetime import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd

from spaceone.dashboard.manager.data_table_manager import DataTableManager
from spaceone.dashboard.manager.cost_analysis_manager import CostAnalysisManager
from spaceone.dashboard.manager.inventory_manager import InventoryManager
from spaceone.dashboard.error.data_table import *

_LOGGER = logging.getLogger(__name__)
GRANULARITY = Literal["DAILY", "MONTHLY", "YEARLY"]


class DataSourceManager(DataTableManager):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cost_analysis_mgr = CostAnalysisManager()
        self.inventory_mgr = InventoryManager()

    def load_data_source(
        self,
        source_type: str,
        options: dict,
        granularity: GRANULARITY,
        start: str = None,
        end: str = None,
        sort: list = None,
        page: dict = None,
    ) -> dict:
        start, end = self._get_time_from_granularity(granularity, start, end)

        if timediff := options.get("timediff"):
            start, end = self._change_time(start, end, timediff)

        if source_type == "COST":
            self._analyze_cost(options, granularity, start, end)
        elif source_type == "ASSET":
            self._analyze_asset(options, granularity, start, end)
        else:
            raise ERROR_NOT_SUPPORTED_SOURCE_TYPE(source_type=source_type)

        if additional_labels := options.get("additional_labels"):
            self._add_labels(additional_labels)

        return self.response(sort, page)

    def _add_labels(self, labels: dict) -> None:
        for key, value in labels.items():
            self.df[key] = value

    def _analyze_asset(
        self,
        options: dict,
        granularity: GRANULARITY,
        start: str,
        end: str,
    ) -> None:
        asset_info = options.get("ASSET", {})
        metric_id = asset_info.get("metric_id")
        data_key = "value"
        data_name = options.get("data_name")
        date_format = options.get("date_format", "SINGLE")

        if metric_id is None:
            raise ERROR_REQUIRED_PARAMETER(parameter="options.ASSET.metric_id")

        query = self._make_query(
            data_key,
            data_name,
            granularity,
            start,
            end,
            options.get("group_by"),
            options.get("filter"),
            options.get("filter_or"),
        )

        params = {"metric_id": metric_id, "query": query}

        response = self.inventory_mgr.analyze_metric_data(params)
        results = response.get("results", [])

        if date_format == "SEPARATE":
            results = self._change_datetime_format(results)

        self.df = pd.DataFrame(results)

    def _analyze_cost(
        self,
        options: dict,
        granularity: GRANULARITY,
        start: str,
        end: str,
    ) -> None:
        cost_info = options.get("COST", {})
        data_source_id = cost_info.get("data_source_id")
        data_key = cost_info.get("data_key")
        data_name = options.get("data_name")
        date_format = options.get("date_format", "SINGLE")

        if data_source_id is None:
            raise ERROR_REQUIRED_PARAMETER(parameter="options.COST.data_source_id")

        if data_key is None:
            raise ERROR_REQUIRED_PARAMETER(parameter="options.COST.data_key")

        query = self._make_query(
            data_key,
            data_name,
            granularity,
            start,
            end,
            options.get("group_by"),
            options.get("filter"),
            options.get("filter_or"),
        )

        params = {"data_source_id": data_source_id, "query": query}

        response = self.cost_analysis_mgr.analyze_cost(params)
        results = response.get("results", [])

        if date_format == "SEPARATE":
            results = self._change_datetime_format(results)

        self.df = pd.DataFrame(results)

    @staticmethod
    def _change_datetime_format(results: list) -> list:
        changed_results = []
        for result in results:
            if date := result.get("date"):
                if len(date) == 4:
                    result["year"] = date
                elif len(date) == 7:
                    year, month = date.split("-")
                    result["year"] = year
                    result["month"] = month
                elif len(date) == 10:
                    year, month, day = date.split("-")
                    result["year"] = year
                    result["month"] = month
                    result["day"] = day

                del result["date"]
            changed_results.append(result)
        return changed_results

    def _change_time(self, start: str, end: str, timediff: dict) -> Tuple[str, str]:
        start_len = len(start)
        end_len = len(end)
        start_time = self._get_datetime_from_str(start)
        end_time = self._get_datetime_from_str(end)

        years = timediff.get("years", 0)
        months = timediff.get("months", 0)
        days = timediff.get("days", 0)

        if years:
            start_time = start_time + relativedelta(years=years)
            end_time = end_time + relativedelta(years=years)
        elif months:
            start_time = start_time + relativedelta(months=months)
            end_time = end_time + relativedelta(months=months)
        elif days:
            start_time = start_time + relativedelta(days=days)
            end_time = end_time + relativedelta(days=days)

        return (
            self._change_str_from_datetime(start_time, start_len),
            self._change_str_from_datetime(end_time, end_len),
        )

    @staticmethod
    def _change_str_from_datetime(dt: datetime, date_str_len: int) -> str:
        if date_str_len == 4:
            return dt.strftime("%Y")
        elif date_str_len == 7:
            return dt.strftime("%Y-%m")
        else:
            return dt.strftime("%Y-%m-%d")

    @staticmethod
    def _get_datetime_from_str(datetime_str: str, is_end: bool = False) -> datetime:
        if len(datetime_str) == 4:
            dt = datetime.strptime(datetime_str, "%Y")
            if is_end:
                dt = dt + relativedelta(years=1) - relativedelta(days=1)

        elif len(datetime_str) == 7:
            dt = datetime.strptime(datetime_str, "%Y-%m")
            if is_end:
                dt = dt + relativedelta(months=1) - relativedelta(days=1)
        else:
            dt = datetime.strptime(datetime_str, "%Y-%m-%d")

        return dt

    @staticmethod
    def _get_time_from_granularity(
        granularity: GRANULARITY,
        start: str = None,
        end: str = None,
    ) -> Tuple[str, str]:
        if start and end:
            return start, end
        else:
            now = datetime.utcnow()

            if granularity == "YEARLY":
                end_time = now.replace(month=1, day=1, hour=0, minute=0, second=0)
                start_time = end_time - relativedelta(years=2)
                return start_time.strftime("%Y"), end_time.strftime("%Y")

            elif granularity == "MONTHLY":
                end_time = now.replace(day=1, hour=0, minute=0, second=0)
                start_time = end_time - relativedelta(months=5)
                return start_time.strftime("%Y-%m"), end_time.strftime("%Y-%m")

            else:
                end_time = now.replace(hour=0, minute=0, second=0)
                start_time = end_time - relativedelta(days=29)
                return start_time.strftime("%Y-%m-%d"), end_time.strftime("%Y-%m-%d")

    @staticmethod
    def _make_query(
        data_key: str,
        data_name: str,
        granularity: GRANULARITY,
        start: str,
        end: str,
        group_by: list = None,
        filter: list = None,
        filter_or: list = None,
    ):
        return {
            "granularity": granularity,
            "start": start,
            "end": end,
            "group_by": group_by,
            "filter": filter,
            "filter_or": filter_or,
            "fields": {data_name: {"key": data_key, "operator": "sum"}},
        }

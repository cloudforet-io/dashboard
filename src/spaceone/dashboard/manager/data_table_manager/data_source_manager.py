import logging
import copy
from typing import Tuple
from datetime import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd

from spaceone.dashboard.manager.data_table_manager import DataTableManager, GRANULARITY
from spaceone.dashboard.manager.cost_analysis_manager import CostAnalysisManager
from spaceone.dashboard.manager.inventory_manager import InventoryManager
from spaceone.dashboard.error.data_table import *

_LOGGER = logging.getLogger(__name__)


class DataSourceManager(DataTableManager):
    def __init__(
        self,
        data_table_type: str,
        source_type: str,
        options: dict,
        widget_id: str,
        domain_id: str,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)

        if source_type not in ["COST", "ASSET"]:
            raise ERROR_NOT_SUPPORTED_SOURCE_TYPE(source_type=source_type)

        self.data_table_type = data_table_type
        self.cost_analysis_mgr = CostAnalysisManager()
        self.inventory_mgr = InventoryManager()
        self.source_type = source_type
        self.options = options
        self.widget_id = widget_id
        self.domain_id = domain_id

        self.data_name = options.get("data_name")
        if self.data_name is None:
            raise ERROR_REQUIRED_PARAMETER(key="options.data_name")

        self.data_unit = options.get("data_unit")
        self.date_format = options.get("date_format", "SINGLE")
        self.timediff = options.get("timediff")
        self.group_by = options.get("group_by")
        self.filter = options.get("filter")
        self.filter_or = options.get("filter_or")
        self.additional_labels = options.get("additional_labels")

    def get_data_and_labels_info(self) -> Tuple[dict, dict]:
        data_info = {self.data_name: {}}

        if self.data_unit:
            data_info[self.data_name]["unit"] = self.data_unit

        if self.timediff:
            data_info[self.data_name]["timediff"] = self.timediff

        labels_info = {}

        if self.group_by:
            for group_option in copy.deepcopy(self.group_by):
                if isinstance(group_option, dict):
                    group_name = group_option.get("name")
                    group_key = group_option.get("key")
                    if "." in group_key:
                        group_key = group_key.split(".")[-1]

                    name = group_name or group_key
                    if name is None:
                        raise ERROR_REQUIRED_PARAMETER(key="options.group_by.key")

                    if group_name:
                        del group_option["name"]

                    if group_key:
                        del group_option["key"]

                    labels_info[group_name] = group_option
                else:
                    labels_info[group_option] = {}

        if self.additional_labels:
            for key in self.additional_labels.keys():
                labels_info[key] = {}

        if self.date_format == "SINGLE":
            labels_info["Date"] = {}
        else:
            labels_info["Year"] = {}
            labels_info["Month"] = {}
            labels_info["Day"] = {}

        return data_info, labels_info

    def load(
        self,
        granularity: GRANULARITY = "MONTHLY",
        start: str = None,
        end: str = None,
        vars: dict = None,
    ) -> pd.DataFrame:
        try:
            start, end = self._get_time_from_granularity(granularity, start, end)

            if self.timediff:
                start, end = self._change_query_time(granularity, start, end)

            if self.source_type == "COST":
                self._analyze_cost(granularity, start, end, vars)
            elif self.source_type == "ASSET":
                self._analyze_asset(granularity, start, end, vars)

            if additional_labels := self.options.get("additional_labels"):
                self._add_labels(additional_labels)
        except Exception as e:
            self.error_message = e.message if hasattr(e, "message") else str(e)
            self.state = "UNAVAILABLE"
            _LOGGER.error(f"[load] add {self.source_type} source error: {e}")

        return self.df

    def _add_labels(self, labels: dict) -> None:
        for key, value in labels.items():
            self.df[key] = value

    def _analyze_asset(
        self,
        granularity: GRANULARITY,
        start: str,
        end: str,
        vars: dict = None,
    ) -> None:
        asset_info = self.options.get("ASSET", {})
        metric_id = asset_info.get("metric_id")
        data_key = "value"

        if metric_id is None:
            raise ERROR_REQUIRED_PARAMETER(parameter="options.ASSET.metric_id")

        query = self._make_query(
            data_key,
            granularity,
            start,
            end,
            vars=vars,
        )

        params = {"metric_id": metric_id, "query": query}

        response = self.inventory_mgr.analyze_metric_data(params)
        results = response.get("results", [])

        results = self._change_datetime_format(results)

        self.df = pd.DataFrame(results)

    def _analyze_cost(
        self,
        granularity: GRANULARITY,
        start: str,
        end: str,
        vars: dict = None,
    ) -> None:
        cost_info = self.options.get("COST", {})
        data_source_id = cost_info.get("data_source_id")
        data_key = cost_info.get("data_key")

        if data_source_id is None:
            raise ERROR_REQUIRED_PARAMETER(parameter="options.COST.data_source_id")

        if data_key is None:
            raise ERROR_REQUIRED_PARAMETER(parameter="options.COST.data_key")

        query = self._make_query(
            data_key,
            granularity,
            start,
            end,
            vars=vars,
        )

        params = {"data_source_id": data_source_id, "query": query}

        response = self.cost_analysis_mgr.analyze_cost(params)
        results = response.get("results", [])

        results = self._change_datetime_format(results)

        self.df = pd.DataFrame(results)

    def _change_datetime_format(self, results: list) -> list:
        changed_results = []
        for result in results:
            if date := result.get("date"):
                if self.timediff:
                    date = self._change_date_by_timediff(date)

                if self.date_format == "SINGLE":
                    result["Date"] = date
                else:
                    if len(date) == 4:
                        result["Year"] = date
                    elif len(date) == 7:
                        year, month = date.split("-")
                        result["Year"] = year
                        result["Month"] = month
                    elif len(date) == 10:
                        year, month, day = date.split("-")
                        result["Year"] = year
                        result["Month"] = month
                        result["Day"] = day

                del result["date"]
            changed_results.append(result)
        return changed_results

    def _change_date_by_timediff(self, date: str) -> str:
        dt = self._get_datetime_from_str(date)
        years = self.timediff.get("years", 0)
        months = self.timediff.get("months", 0)

        if years:
            dt = dt - relativedelta(years=years)
        elif months:
            dt = dt - relativedelta(months=months)

        return self._change_str_from_datetime(dt, len(date))

    def _change_query_time(
        self, granularity: str, start: str, end: str
    ) -> Tuple[str, str]:
        start_len = len(start)
        end_len = len(end)
        start_time = self._get_datetime_from_str(start)
        end_time = self._get_datetime_from_str(end, is_end=True)

        years = self.timediff.get("years", 0)
        months = self.timediff.get("months", 0)

        if years:
            start_time = start_time + relativedelta(years=years)
            end_time = end_time + relativedelta(years=years)
        elif months:
            start_time = start_time + relativedelta(months=months)
            end_time = end_time + relativedelta(months=months)

        if granularity == "YEARLY":
            if start_len == 4:
                if start_time + relativedelta(years=3) <= end_time:
                    end_time = start_time + relativedelta(years=2)
        elif granularity == "MONTHLY":
            if start_len == 4:
                start_time = end_time
            elif start_len == 7:
                if start_time + relativedelta(months=12) <= end_time:
                    end_time = start_time + relativedelta(months=11)
        else:
            if start_len <= 7:
                start_time = end_time
            elif start_len == 10:
                if start_time + relativedelta(months=1) <= end_time:
                    end_time = (
                        start_time + relativedelta(months=1) - relativedelta(days=1)
                    )

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

    def _make_query(
        self,
        data_key: str,
        granularity: GRANULARITY,
        start: str,
        end: str,
        vars: dict = None,
    ):
        if self.filter:
            for filter_info in self.filter:
                query_value = filter_info.get("v") or filter_info.get("value")
                if self.is_jinja_expression(query_value):
                    query_value, gv_type_map = self.change_global_variables(
                        query_value, vars
                    )
                    query_value = self.remove_jinja_braces(query_value)
                    if (
                        isinstance(query_value, str)
                        or isinstance(query_value, int)
                        or isinstance(query_value, float)
                    ):
                        filter_info["v"] = [query_value]
                    elif isinstance(query_value, list):
                        filter_info["v"] = query_value

        return {
            "granularity": granularity,
            "start": start,
            "end": end,
            "group_by": self.group_by,
            "filter": self.filter,
            "filter_or": self.filter_or,
            "fields": {self.data_name: {"key": data_key, "operator": "sum"}},
        }

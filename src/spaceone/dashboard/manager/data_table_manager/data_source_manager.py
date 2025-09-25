import copy
import logging
from datetime import datetime
from typing import Tuple

import pandas as pd
from dateutil.relativedelta import relativedelta

from spaceone.dashboard.error.data_table import *
from spaceone.dashboard.manager.config_manager import ConfigManager
from spaceone.dashboard.manager.cost_analysis_manager import CostAnalysisManager
from spaceone.dashboard.manager.data_table_manager import DataTableManager
from spaceone.dashboard.manager.identity_manager import IdentityManager
from spaceone.dashboard.manager.inventory_manager import InventoryManager

_LOGGER = logging.getLogger(__name__)


class DataSourceManager(DataTableManager):
    def __init__(
        self,
        data_table_type: str,
        source_type: str,
        options: dict,
        widget_id: str,
        domain_id: str,
        workspace_id: str = None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)

        if source_type not in ["COST", "ASSET", "UNIFIED_COST"]:
            raise ERROR_NOT_SUPPORTED_SOURCE_TYPE(source_type=source_type)

        self.data_table_type = data_table_type
        self.cost_analysis_mgr = CostAnalysisManager()
        self.inventory_mgr = InventoryManager()
        self.identity_mgr = IdentityManager()
        self.config_mgr = ConfigManager()
        self.source_type = source_type
        self.options = options
        self.widget_id = widget_id
        self.domain_id = domain_id
        self.workspace_id = workspace_id

        self.data_name = options.get("data_name")
        if self.data_name is None:
            raise ERROR_REQUIRED_PARAMETER(key="options.data_name")

        self.data_unit = options.get("data_unit")
        self.timediff = options.get("timediff")
        if self.timediff:
            self.timediff_data_name = self.timediff["data_name"]

        self.group_by = options.get("group_by")
        self.filter = options.get("filter")
        self.filter_or = options.get("filter_or")
        self.sort = options.get("sort")

    def get_data_and_labels_info(self) -> Tuple[dict, dict]:
        data_info = {self.data_name: {}}

        if self.data_unit:
            data_info[self.data_name]["unit"] = self.data_unit

        if self.timediff:
            data_info[self.timediff["data_name"]] = {}

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

                    if group_key == "service_account_id":
                        service_account_info = group_option.get("tags")
                        if service_account_info:
                            for key in service_account_info:
                                labels_info[f"tags.{key}"] = {}

                            labels_info[group_name] = {}

                else:
                    labels_info[group_option] = {}

        labels_info["Date"] = {}

        return data_info, labels_info

    def load(
        self,
        granularity: str = "MONTHLY",
        start: str = None,
        end: str = None,
        vars: dict = None,
    ) -> pd.DataFrame:
        try:
            start, end = self._get_time_from_granularity(granularity, start, end)

            if self.source_type == "COST":
                self._analyze_cost(granularity, start, end, vars)
            elif self.source_type == "ASSET":
                self._analyze_asset(granularity, start, end, vars)
            elif self.source_type == "UNIFIED_COST":
                self._analyze_unified_cost(granularity, start, end, vars)

            if self.timediff:
                self.df = self._apply_timediff(granularity, start, end, vars)

            if self.group_by:
                self._add_none_value_group_by_columns()

                group_by_keys = {option.get("key"): option for option in self.group_by}

                service_account_info = group_by_keys.get("service_account_id")
                if service_account_info and service_account_info.get("tags"):
                    service_account_keys = service_account_info["tags"]
                    self._apply_left_join_tags_from_service_account(
                        service_account_keys
                    )

            self.state = "AVAILABLE"
            self.error_message = None

        except Exception as e:
            self.state = "UNAVAILABLE"
            self.error_message = e.message if hasattr(e, "message") else str(e)
            _LOGGER.error(f"[load] add {self.source_type} source error: {e}")

        return self.df

    def _analyze_asset(
        self,
        granularity: str,
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
        granularity: str,
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

        if self.df.empty:
            expected_columns = []
            if "group_by" in query and query.get("group_by"):
                for group in query["group_by"]:
                    if "name" in group:
                        expected_columns.append(group["name"])

            if "fields" in query and query.get("fields"):
                expected_columns.extend(list(query["fields"].keys()))

            if granularity:
                expected_columns.append("Date")

            if expected_columns:
                self.df = pd.DataFrame(columns=expected_columns)

    def _analyze_unified_cost(
        self,
        granularity: str,
        start: str,
        end: str,
        vars: dict = None,
    ) -> None:
        cost_info = self.options.get("UNIFIED_COST", {})
        data_key = cost_info.get("data_key")

        if data_key is None:
            raise ERROR_REQUIRED_PARAMETER(parameter="options.COST.data_key")

        if data_key == "cost":
            currency = self._get_currency_from_domain_config()
            self.currency = currency
            data_key = f"{data_key}.{currency}"

        query = self._make_query(
            data_key,
            granularity,
            start,
            end,
            vars=vars,
        )

        params = {"query": query}

        response = self.cost_analysis_mgr.analyze_unified_cost(params)
        results = response.get("results", [])

        results = self._change_datetime_format(results)

        self.df = pd.DataFrame(results)

    def _apply_timediff(
        self, granularity: str, start: str, end: str, vars: dict
    ) -> pd.DataFrame:
        origin_df = self.df.copy()

        start, end = self._change_query_time(granularity, start, end)

        if self.source_type == "COST":
            self._analyze_cost(granularity, start, end, vars)
        elif self.source_type == "ASSET":
            self._analyze_asset(granularity, start, end, vars)
        elif self.source_type == "UNIFIED_COST":
            self._analyze_unified_cost(granularity, start, end, vars)

        if self.df.empty:
            _LOGGER.debug(
                "self.df is empty, creating a DataFrame with the same columns as origin_df."
            )
            self.df = pd.DataFrame(columns=origin_df.columns)

        self.df["Date"] = self.df["Date"].apply(
            lambda x: self._change_date_by_timediff(x)
        )
        self.df.rename(columns={self.data_name: self.timediff_data_name}, inplace=True)

        origin_label_keys = [
            column for column in origin_df.columns if column != self.data_name
        ]
        diff_label_keys = [
            column for column in self.df.columns if column != self.timediff_data_name
        ]
        join_keys = list(set(origin_label_keys) & set(diff_label_keys))

        merged_df = pd.merge(origin_df, self.df, on=join_keys, how="outer")

        fill_na = {key: 0 for key in [self.data_name, self.timediff_data_name]}
        fill_na.update({key: "" for key in join_keys})
        merged_df = merged_df.fillna(value=fill_na)

        return merged_df

    @staticmethod
    def _change_datetime_format(results: list) -> list:
        changed_results = []
        for result in results:
            if date := result.get("date"):
                result["Date"] = date
                del result["date"]
            changed_results.append(result)
        return changed_results

    def _change_date_by_timediff(self, date: str) -> str:
        dt = self._get_datetime_from_str(date)
        years = int(self.timediff.get("years", 0))
        months = int(self.timediff.get("months", 0))

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
        granularity: str,
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
        granularity: str,
        start: str,
        end: str,
        vars: dict = None,
    ):
        self.filter = self.filter or []

        if self.filter:

            new_filter = []
            for filter_info in self.filter:

                query_value = filter_info.get("v") or filter_info.get("value")
                if self.is_jinja_expression(query_value):
                    query_value, gv_type_map = self.change_global_variables(
                        query_value, vars
                    )

                    query_value = self.remove_jinja_braces(query_value, gv_type_map)
                    if isinstance(query_value, (str, int, float)):
                        filter_info["v"] = [query_value]
                    elif isinstance(query_value, list):
                        filter_info["v"] = query_value

                    if not gv_type_map:
                        continue

                new_filter.append(filter_info)

            self.filter = new_filter

        if vars:
            for key, value in vars.items():
                if key in [
                    "workspace_id",
                    "project_id",
                    "project_group_id",
                    "service_account_id",
                ]:
                    if isinstance(value, list):
                        self.filter.append(
                            {"key": key, "value": value, "operator": "in"}
                        )
                    else:
                        self.filter.append(
                            {"key": key, "value": value, "operator": "eq"}
                        )
                elif key == "region_code":
                    if isinstance(value, list):
                        if (
                            self.source_type == "COST"
                            or self.source_type == "UNIFIED_COST"
                        ):
                            self.filter.append(
                                {"key": "region_code", "value": value, "operator": "in"}
                            )
                        else:
                            self.filter.append(
                                {
                                    "key": "labels.Region",
                                    "value": value,
                                    "operator": "in",
                                }
                            )
                    else:
                        if (
                            self.source_type == "COST"
                            or self.source_type == "UNIFIED_COST"
                        ):
                            self.filter.append(
                                {"key": "region_code", "value": value, "operator": "eq"}
                            )
                        else:
                            self.filter.append(
                                {
                                    "key": "labels.Region",
                                    "value": value,
                                    "operator": "eq",
                                }
                            )

        return {
            "granularity": granularity,
            "start": start,
            "end": end,
            "group_by": self.group_by,
            "filter": self.filter,
            "filter_or": self.filter_or,
            "fields": {self.data_name: {"key": data_key, "operator": "sum"}},
            "sort": self.sort,
        }

    def _add_none_value_group_by_columns(self) -> None:
        group_by_columns = [group_option.get("name") for group_option in self.group_by]
        none_group_by_columns = list(set(group_by_columns) - set(self.df.columns))

        for column in none_group_by_columns:
            self.df[column] = None

    def _apply_left_join_tags_from_service_account(self, service_account_keys) -> None:
        service_accounts_info = self.identity_mgr.list_service_accounts(
            self.workspace_id
        )

        if service_accounts_info:
            flattened_data = []
            for service_account_info in service_accounts_info["results"]:
                tags_data = {
                    key: value
                    for key, value in service_account_info.items()
                    if key != "tags" and key == "service_account_id"
                }
                tags = service_account_info.get("tags", {})
                for key, value in tags.items():
                    if key in service_account_keys:
                        tags_data[f"tags.{key}"] = value
                flattened_data.append(tags_data)

            tags_df = pd.DataFrame(flattened_data)

            self.df = pd.merge(
                self.df,
                tags_df,
                left_on="Service Account",
                right_on="service_account_id",
                how="left",
            )
            self.df.drop(columns=["service_account_id"], inplace=True)

            fill_na = {f"tags.{key}": "" for key in service_account_keys}
            self.df = self.df.fillna(value=fill_na)

    def _get_currency_from_domain_config(self):
        request_params = {"name": "settings"}
        domain_config = self.config_mgr.get_domain_config(
            request_params, self.domain_id
        )
        unified_cost_config = domain_config["data"].get("unified_cost_config")
        return unified_cost_config.get("currency")

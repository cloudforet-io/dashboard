import functools
from spaceone.api.dashboard.v1 import widget_pb2
from spaceone.core.pygrpc.message_type import *
from spaceone.core import utils
from spaceone.dashboard.model.widget_model import Widget

__all__ = ['WidgetInfo', 'WidgetsInfo']


def WidgetInfo(widget_vo: Widget, minimal=False):
    info = {
        'widget_id': widget_vo.widget_id,
        'name': widget_vo.name,
        'labels': change_list_value_type(widget_vo.labels),
        'user_id': widget_vo.user_id,
        'domain_id': widget_vo.domain_id
    }

    if not minimal:
        info.update({
            'options': _WidgetOptionsInfo(widget_vo.options),
            'variables': change_struct_type(widget_vo.variables),
            'schema': change_struct_type(widget_vo.schema),
            'tags': change_struct_type(widget_vo.tags),
            'created_at': utils.datetime_to_iso8601(widget_vo.created_at),
            'updated_at': utils.datetime_to_iso8601(widget_vo.updated_at)
        })

        return widget_pb2.WidgetInfo(**info)


def WidgetsInfo(widget_vos, total_count, **kwargs):
    return widget_pb2.WidgetsInfo(results=list(
        map(functools.partial(WidgetInfo, **kwargs), widget_vos)), total_count=total_count)


def _PeriodInfo(period):
    if period:
        info = {
            'start': period.start,
            'end': period.end
        }
        return widget_pb2.WidgetPeriod(**info)
    else:
        return None


def _DateRangeInfo(date_range):
    if date_range:
        info = {
            'enabled': date_range.enabled,
            'period_type': date_range.period_type,
            'period': _PeriodInfo(date_range.period)
        }
        return widget_pb2.WidgetDateRange(**info)
    else:
        return None


def _CurrencyInfo(currency):
    if currency:
        info = {
            'enabled': currency.enabled
        }
        return widget_pb2.WidgetCurrency(**info)
    else:
        return None


def _WidgetOptionsInfo(options):
    if options:
        info = {
            'date_range': _DateRangeInfo(options.date_range),
            'currency': _CurrencyInfo(options.currency)
        }

        return widget_pb2.WidgetOptions(**info)
    else:
        return None

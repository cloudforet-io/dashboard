import functools
from spaceone.api.dashboard.v1 import custom_widget_pb2
from spaceone.core.pygrpc.message_type import *
from spaceone.core import utils
from spaceone.dashboard.model.custom_widget_model import CustomWidget

__all__ = ['CustomWidgetInfo', 'CustomWidgetsInfo']


def CustomWidgetInfo(custom_widget_vo: CustomWidget, minimal=False):
    info = {
        'custom_widget_id': custom_widget_vo.custom_widget_id,
        'widget_name': custom_widget_vo.widget_name,
        'title': custom_widget_vo.title,
        'version': custom_widget_vo.version,
        'user_id': custom_widget_vo.user_id,
        'domain_id': custom_widget_vo.domain_id
    }

    if not minimal:
        info.update({
            'options': change_struct_type(custom_widget_vo.options),
            'settings': change_struct_type(custom_widget_vo.settings),
            'inherit_options': change_struct_type(custom_widget_vo.inherit_options),
            'labels': change_list_value_type(custom_widget_vo.labels),
            'tags': change_struct_type(custom_widget_vo.tags),
            'created_at': utils.datetime_to_iso8601(custom_widget_vo.created_at),
            'updated_at': utils.datetime_to_iso8601(custom_widget_vo.updated_at)
        })

    return custom_widget_pb2.CustomWidgetInfo(**info)


def CustomWidgetsInfo(custom_widget_vos, total_count, **kwargs):
    return custom_widget_pb2.CustomWidgetsInfo(results=list(
        map(functools.partial(CustomWidgetInfo, **kwargs), custom_widget_vos)), total_count=total_count)

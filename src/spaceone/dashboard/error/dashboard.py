from spaceone.core.error import *


class ERROR_NOT_SUPPORTED_VERSION(ERROR_INVALID_ARGUMENT):
    _message = "Widget does not create in old dashboard version. (dashboard_version = {version})"

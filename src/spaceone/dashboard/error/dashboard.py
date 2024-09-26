from spaceone.core.error import *


class ERROR_NOT_SUPPORTED_VERSION(ERROR_INVALID_ARGUMENT):
    _message = "Widget does not create in old dashboard version. (dashboard_version = {version})"


class ERROR_NOT_ALLOWED_SHARE(ERROR_INVALID_ARGUMENT):
    _message = (
        "Dashboard the belongs to folder cannot be shared. (folder_id = {folder_id})"
    )


class ERROR_NOT_ALLOWED_UNSHARE(ERROR_INVALID_ARGUMENT):
    _message = (
        "Dashboard the belongs to folder cannot be unshared. (folder_id = {folder_id})"
    )

from spaceone.core.error import *


class ERROR_LATEST_VERSION(ERROR_INVALID_ARGUMENT):
    _message = 'Do not remove latest version. (version = {version})'

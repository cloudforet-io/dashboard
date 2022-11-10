from spaceone.core.error import *


class ERROR_INVALID_USER_ID(ERROR_INVALID_ARGUMENT):
    _message = 'user_id is invalid. (user_id = {user_id}, tnx_user_id={tnx_user_id})'


class ERROR_LATEST_VERSION(ERROR_INVALID_ARGUMENT):
    _message = 'Do not remove latest version. (latest version = {version})'

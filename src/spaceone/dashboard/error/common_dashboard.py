from spaceone.core.error import *


class ERROR_INVALID_USER_ID(ERROR_INVALID_ARGUMENT):
    _message = 'Escalation policy is invalid. (user_id = {user_id})'

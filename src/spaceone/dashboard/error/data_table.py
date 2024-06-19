from spaceone.core.error import *


class ERROR_NOT_SUPPORTED_SOURCE_TYPE(ERROR_INVALID_ARGUMENT):
    _message = "Data table does not support source type. (source_type = {source_type})"


class ERROR_QUERY_OPTION(ERROR_INVALID_ARGUMENT):
    _message = "Query option is invalid. (key = {key})"


class ERROR_NOT_SUPPORTED_QUERY_OPTION(ERROR_INVALID_ARGUMENT):
    _message = "Query option is not supported. (key = {key})"


class ERROR_NOT_SUPPORTED_OPERATOR(ERROR_INVALID_ARGUMENT):
    _message = "Data table does not support operator. (operator = {operator})"

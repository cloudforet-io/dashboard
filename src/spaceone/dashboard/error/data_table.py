from spaceone.core.error import *


class ERROR_NOT_SUPPORTED_SOURCE_TYPE(ERROR_INVALID_ARGUMENT):
    _message = "Data table does not support source type. (source_type = {source_type})"


class ERROR_QUERY_OPTION(ERROR_INVALID_ARGUMENT):
    _message = "Query option is invalid. (key = {key})"


class ERROR_NOT_SUPPORTED_QUERY_OPTION(ERROR_INVALID_ARGUMENT):
    _message = "Query option is not supported. (key = {key})"


class ERROR_NOT_SUPPORTED_OPERATOR(ERROR_INVALID_ARGUMENT):
    _message = "Data table does not support operator. (operator = {operator})"


class ERROR_DUPLICATED_DATA_FIELDS(ERROR_INVALID_ARGUMENT):
    _message = "Data fields for join are duplicated. (fields = {fields})"


class ERROR_NO_FIELDS_TO_JOIN(ERROR_INVALID_ARGUMENT):
    _message = "There is no fields to join."


class ERROR_NOT_GLOBAL_VARIABLE_KEY(ERROR_INVALID_ARGUMENT):
    _message = "The variable is not a global variable. (global_variable_key = {global_variable_key})"


class ERROR_NO_FIELDS_TO_GLOBAL_VARIABLES(ERROR_INVALID_ARGUMENT):
    _message = "There is no fields in GLOBAL_VARIABLES. (vars = {vars})"

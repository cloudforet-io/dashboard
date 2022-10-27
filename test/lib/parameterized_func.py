from parameterized import parameterized


def key_value_name_func(testcase_func, param_num, param):
    key = parameterized.to_safe_name(str(param.args[0]))
    value = parameterized.to_safe_name(str(param.args[1]))
    return f"{testcase_func.__name__}(key={key},value={value})"

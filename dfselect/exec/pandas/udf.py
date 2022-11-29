import pandas as pd


def udf_IFNULL(check, null_val):
    """
    the udf function of IFNULL(check, null_val)
    :param check: the value to check null or not
    :param null_val: the value to return if check is null
    :return: the value of check if check is null or null_val otherwise
    """
    return udf_COALESCE(check, null_val)


def udf_COALESCE(check, *null_vals):
    """
    the udf function of COALESCE(check, null_val1, null_val2, ...)
    :param check: the value to check null or not
    :param null_vals: the values of extra values to check
    :return: return the non-null value of check and null_vals in turn, None if all are null
    """
    import numpy as np
    if check and not np.isnan(check):
        return check
    return udf_COALESCE(*null_vals)


def udf_IF(cond, true_val, false_val):
    """
    the udf function of IF(cond, true_val, false_val)
    :param cond: the condition to be check
    :param true_val: the return value if check is True
    :param false_val: the return value if check is False
    :return: true_val if cond is True or false_val otherwise
    """
    if isinstance(cond, pd.Series):
        return pd.Series([true_val if t else false_val for t in cond])
    return true_val if cond else false_val


def udf_F(a, b):
    return a + b

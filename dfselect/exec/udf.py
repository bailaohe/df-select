import pandas as pd


def udf_IFNULL(check, null_val):
    return udf_COALESCE(check, null_val)


def udf_COALESCE(check, *null_vals):
    import numpy as np
    if check and not np.isnan(check):
        return check
    return udf_COALESCE(*null_vals)


def udf_IF(cond, true_val, false_val):
    if isinstance(cond, pd.Series):
        return pd.Series([true_val if t else false_val for t in cond])
    return true_val if cond else false_val


def udf_F(a, b):
    return a + b

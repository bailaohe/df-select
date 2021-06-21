from . import operator as op_repo


def _exec_func(op_code: str):
    op_func_name = "exec_" + op_code.upper()
    method = getattr(op_repo, op_func_name, None)
    if not method:
        raise RuntimeError("operator", op_code, 'not defined')
    return method


def exec_operator(df, op_code: str, ctx: dict, *args):
    op_func = _exec_func(op_code)
    if op_func:
        return op_func(df, ctx, *args)

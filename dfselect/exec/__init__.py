from ..errors import DFSelectExecError
from ..context import ctx_config_get_exec_engine


def _exec_func(op_code: str, ctx: dict):
    op_func_name = "exec_" + op_code.upper()
    exec_engine = ctx_config_get_exec_engine(ctx)
    method = getattr(exec_engine, op_func_name, None)
    if not method:
        raise DFSelectExecError(f'operator {op_code} not defined')
    return method


def exec_operator(df, op_code: str, ctx: dict, *args):
    op_func = _exec_func(op_code, ctx)
    if op_func:
        return op_func(df, ctx, *args)


def exec_operators(select_cmds: list or tuple, ctx: dict):
    df = None
    for operator in select_cmds:
        print(operator)
        df = exec_operator(df, operator[0], ctx, *operator[1])
    return df

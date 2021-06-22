from sqlparse.sql import Operation, Function, Identifier, Parenthesis, IdentifierList, Comparison

from ..errors import DFSelectExecError
from ..util import check_col_name, is_skip_token

_agg_func_dict = dict(
    avg='mean',
    mean='mean',
    count='count',
    sum='sum',
    std='std',
    median='median',
    quantile='quantile',
)


def eval_oper(oper: Operation, columns, row_key):
    oper_str = ''
    for token in oper.tokens:
        if isinstance(token, Function):
            oper_str += eval_func(token, columns, row_key)
        elif isinstance(token, Identifier):
            oper_str += f'{row_key}["{check_col_name(token.value, columns)}"]'
        elif isinstance(token, (Operation, Comparison)):
            oper_str += eval_oper(token, columns, row_key)
        else:
            oper_str += str(token)
    return oper_str


def eval_func_args(args: IdentifierList, columns, row_key):
    func_arg_str = ''
    for arg in args.tokens:
        if isinstance(arg, Function):
            func_arg_str += eval_func(arg, columns, row_key)
        elif isinstance(arg, Identifier):
            func_arg_str += f'{row_key}["{check_col_name(arg.value, columns)}"]'
        elif isinstance(arg, (Operation, Comparison)):
            func_arg_str += eval_oper(arg, columns, row_key)
        else:
            func_arg_str += str(arg)
    return func_arg_str


def eval_func(fn: Function, columns, row_key):
    func_str = ''
    func_key = str(fn.tokens[0])
    if func_key.lower() not in _agg_func_dict.keys():
        func_str = f'_load_udf("{func_key}")'
        for token in fn.tokens[1:]:
            if isinstance(token, Parenthesis):
                for token_in_paren in token.tokens:
                    if isinstance(token_in_paren, IdentifierList):
                        func_str += eval_func_args(token_in_paren, columns, row_key)
                    elif isinstance(token_in_paren, Function):
                        func_str += eval_func(token_in_paren, columns, row_key)
                    elif isinstance(token_in_paren, Identifier):
                        func_str += f'{row_key}["{check_col_name(token_in_paren.value, columns)}"]'
                    elif isinstance(token_in_paren, (Operation, Comparison)):
                        func_str += eval_oper(token_in_paren, columns, row_key)
                    else:
                        func_str += str(token_in_paren)

            else:
                func_str += str(token)
    else:
        agg_func_name = _agg_func_dict[func_key.lower()]
        func_str += '('
        for token in fn.tokens[1:]:
            if isinstance(token, Parenthesis):
                agg_func_args = [t for t in token.tokens if not is_skip_token(t)]
                for token_in_paren in agg_func_args:
                    if isinstance(token_in_paren, IdentifierList):
                        # func_str += eval_func_args(token_in_paren, columns, row_key)
                        raise DFSelectExecError(f'only single-param agg-function is supported')
                    if isinstance(token_in_paren, Function):
                        func_str += eval_func(token_in_paren, columns, row_key)
                    elif isinstance(token_in_paren, Identifier):
                        func_str += f'{row_key}["{check_col_name(token_in_paren.value, columns)}"]'
                    elif isinstance(token_in_paren, (Operation, Comparison)):
                        func_str += eval_oper(token_in_paren, columns, row_key)
                    else:
                        func_str += str(token_in_paren)
            else:
                func_str += str(token)
        func_str += ').' + agg_func_name + '()'
    return func_str


def eval_agg_expr(expr, columns, row_key):
    if isinstance(expr, Operation):
        return eval_oper(expr, columns, row_key)
    elif isinstance(expr, Function):
        return eval_func(expr, columns, row_key)

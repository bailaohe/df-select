from sqlparse.sql import Operation, Function, Identifier, Parenthesis, IdentifierList, Comparison
from ..util import check_col_name


def eval_oper(oper, columns, row_key):
    oper_str = ''
    for token in oper.tokens:
        if isinstance(token, Function):
            oper_str += eval_func(token, columns, row_key)
        elif isinstance(token, Identifier):
            oper_str += row_key + '["' + check_col_name(token.value, columns) + '"]'
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
            func_arg_str += row_key + '["' + check_col_name(arg.value, columns) + '"]'
        elif isinstance(arg, (Operation, Comparison)):
            func_arg_str += eval_oper(arg, columns, row_key)
        else:
            func_arg_str += str(arg)
    return func_arg_str


def eval_func(fn: Function, columns, row_key):
    func_str = '_load_udf("' + str(fn.tokens[0]) + '")'
    for token in fn.tokens[1:]:
        if isinstance(token, Parenthesis):
            for token_in_paren in token.tokens:
                if isinstance(token_in_paren, IdentifierList):
                    func_str += eval_func_args(token_in_paren, columns, row_key)
                elif isinstance(token_in_paren, Function):
                    func_str += eval_func(token_in_paren, columns, row_key)
                elif isinstance(token_in_paren, Identifier):
                    func_str += row_key + '["' + check_col_name(token_in_paren.value, columns) + '"]'
                elif isinstance(token_in_paren, (Operation, Comparison)):
                    func_str += eval_oper(token_in_paren, columns, row_key)
                else:
                    func_str += str(token_in_paren)

        else:
            func_str += str(token)
    return func_str


def eval_expr(expr, columns, row_key):
    if isinstance(expr, Operation):
        return eval_oper(expr, columns, row_key)
    elif isinstance(expr, Function):
        return eval_func(expr, columns, row_key)

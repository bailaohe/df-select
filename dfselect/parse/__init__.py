import sqlparse as sp
from sqlparse.sql import Statement, Identifier, Where, IdentifierList, Comparison, Operation, Function
from sqlparse.tokens import Comparison as compOp, Wildcard, DML, Literal, Keyword

from ..util import is_skip_token, move_on_next, collect_tokens_until, parse_identifier, reparse_token, \
    eval_literal_value
from ..errors import DFSelectParseError
from ..log import log


def parse_select(select: str):
    """
    parse the single select statement
    :param select: the single select statement
    :return: the parsed operation list
    """
    select = ' '.join(select.strip().split())
    log.debug('Parse select:')
    log.debug(f'> {select}')
    stmts = sp.parse(select)

    # only the single select is supported
    if len(stmts) > 1:
        raise DFSelectParseError("Only single select query can be processed")

    return _parse_select(stmts[0])


def _parse_select(stmt: Statement):
    """
    parse the select query to get the result dataframe
    :param stmt: the query statement
    :return: the result dataframe
    """
    assert _contains_sub_select(stmt), 'only the select statement is supported'
    pos = 0

    proj_columns, pos = _parse_select_clause(stmt, offset=pos)
    major_table, join_clauses, pos = _parse_from_clause(stmt, offset=pos)

    filter_expr = None
    order_by = None
    limit = None
    group_by = None
    group_by_seen = False

    if len(stmt.tokens) > pos and isinstance(stmt.tokens[pos], Where):
        where = stmt.tokens[pos]
        # process where
        filter_expr = _parse_where_clause(where)
        pos += 1

    pos = move_on_next(stmt, offset=pos)
    while len(stmt.tokens) > pos:
        keyword = stmt.tokens[pos]
        assert keyword.is_keyword, 'invalid'
        tokens_by_keyword, pos = collect_tokens_until(stmt, lambda t: t.is_keyword, pos + 1)
        if 'ORDER' in keyword.value.upper():
            order_by = _parse_order_clause(tokens_by_keyword)
        elif 'LIMIT' in keyword.value.upper():
            limit = _parse_limit_clause(tokens_by_keyword)
        elif 'GROUP' in keyword.value.upper():
            group_by = _parse_group_clause(tokens_by_keyword)
            group_by_seen = True
        pos = move_on_next(stmt, offset=pos)

    log.debug('parsed components:')
    log.debug(f'> FILTER: {filter_expr}')
    log.debug(f'> ORDER_BY: {order_by}')
    log.debug(f'> LIMIT: {limit}')
    log.debug(f'> GROUP_BY: {group_by}')

    assert major_table, 'major table parsed failed'

    operators = [
        ('LOAD', [major_table])
    ]
    for join_clause in join_clauses:
        operators.append(('JOIN', join_clause))
    if filter_expr:
        operators.append(('FILTER', [filter_expr]))
    if order_by:
        operators.append(('ORDER', order_by))
    if limit:
        operators.append(('LIMIT', limit))
    if group_by:
        operators.append(('GROUP', [group_by, proj_columns]))
    if proj_columns:
        operators.append(('PROJECT', proj_columns))

    log.debug('parsed operators:')
    for op in operators:
        log.debug(f'> {op[0]} {op[1]}')

    return operators


def _parse_group_item(item):
    if isinstance(item, Identifier):
        # extract the columns identifiers
        return parse_identifier(item, allow_alias=False)
    elif isinstance(item, Operation):
        # if the item is an expression
        return item.value, item.value.lower().strip('\'').strip('"')
    elif isinstance(item, Function):
        # if the item is a function call
        return item.value, item.value.lower().strip('\'').strip('"')
    # we do not support sub-query in select-clause
    elif _contains_sub_select(item):
        raise DFSelectParseError("sub-query is not supported in groupby-clause: {seg}".format(seg=str(item)))
    raise DFSelectParseError("invalid token in groupby-clause: {seg}".format(seg=str(item)))


def _parse_group_clause(group_tokens):
    token = group_tokens[0]
    passed_group_items = []
    if isinstance(token, IdentifierList):
        group_items = [t for t in token.tokens if not is_skip_token(t)]
        for group_item in group_items:
            passed_group_items.append(_parse_group_item(group_item))
    else:
        passed_group_items.append(_parse_group_item(token))
    return passed_group_items


def _parse_order_item(item):
    asc = True
    if item.is_group:
        tokens = [t for t in list(item.flatten()) if not is_skip_token(t)]
        if tokens[-1].ttype in Keyword.Order:
            direction = tokens.pop()
            if direction.value.lower() == 'desc':
                asc = False
            item = reparse_token(''.join([str(t) for t in tokens]))
    if isinstance(item, Identifier):
        # extract the columns identifiers
        return parse_identifier(item, allow_alias=False)[0], asc
    elif isinstance(item, Operation):
        # if the item is an expression
        return item.value, asc
    elif isinstance(item, Function):
        # if the item is a function call
        return item.value, asc
    # we do not support sub-query in select-clause
    elif _contains_sub_select(item):
        raise DFSelectParseError("sub-query is not supported in groupby-clause: {seg}".format(seg=str(item)))
    raise DFSelectParseError("invalid token in groupby-clause: {seg}".format(seg=str(item)))


def _parse_order_clause(order_tokens):
    token = order_tokens[0]
    passed_order_items = []
    if isinstance(token, IdentifierList):
        group_items = [t for t in token.tokens if not is_skip_token(t)]
        for group_item in group_items:
            passed_order_items.append(_parse_order_item(group_item))
    else:
        passed_order_items.append(_parse_order_item(token))
    return passed_order_items


def _parse_limit_clause(limit_tokens):
    token = limit_tokens[0]
    if isinstance(token, IdentifierList):
        limit_items = [t for t in token.flatten() if not is_skip_token(t)]
    else:
        limit_items = [token]
    if not limit_items:
        raise DFSelectParseError(f'invalid limit param: {limit_tokens}')
    if len(limit_items) > 2:
        raise DFSelectParseError(f'limit params too long: {limit_tokens}')
    invalid_limit_items = [t for t in limit_items if t.ttype not in Literal]
    if invalid_limit_items:
        raise DFSelectParseError(f'limit params contains invalid items: {limit_tokens}')
    limit_params = [eval_literal_value(t) for t in limit_items]
    if limit_params[-1] < 0:
        raise DFSelectParseError(f'limit params use invalid limit size: {limit_tokens}')
    if len(limit_params) > 1:
        if limit_params[0] < 0:
            raise DFSelectParseError(f'limit params use invalid limit offset: {limit_tokens}')
    else:
        limit_params = [0, *limit_params]
    return limit_params


def _contains_sub_select(item):
    """
    check whether an parsed item is a query DML statement
    :param item: the parsed item
    :param ctx: the context object
    :return: True or False
    """
    if not item.is_group:
        return item.ttype is DML and item.value.upper() == 'SELECT'
    for sub_token in item.tokens:
        if _contains_sub_select(sub_token):
            return True
    return False


def _parse_select_item(item):
    # skip the '*'
    if item.ttype is Wildcard:
        return None
    if item.ttype in Literal:
        # if the item is a constant of string or number
        const_val = int(item.value) if item.ttype in Literal.Number.Integer else float(
            item.value) if item.ttype in Literal.Number.Float else item.value
        return const_val, item.value.lower().strip('\'').strip('"')
    elif item.is_keyword and item.value.upper() == 'NULL':
        return None, 'NULL'
    elif isinstance(item, Identifier):
        # extract the columns identifiers
        return parse_identifier(item, allow_literal=True)
    elif isinstance(item, Operation):
        # if the item is an expression
        return item.value, item.value.lower().strip('\'').strip('"')
    elif isinstance(item, Function):
        # if the item is a function call
        return item.value, item.value.lower().strip('\'').strip('"')
    # we do not support sub-query in select-clause
    elif _contains_sub_select(item):
        raise DFSelectParseError("sub-query is not supported in select-clause: {seg}".format(seg=str(item)))
    else:
        raise DFSelectParseError("invalid token in select-clause: {seg}".format(seg=str(item)))


def _parse_select_clause(tokens, offset: int = 0):
    """
    eval the select clause to retrieve the selected columns
    :param tokens: the token list to parse
    :param offset: the start position to parse
    :return: the parsed selected columns
    """
    parsed_columns = []
    select_seen = False
    idx = 0
    for idx, item in enumerate(tokens[offset:]):
        # skip the whitespace or comment or punctuation
        if is_skip_token(item):
            continue
        # after select
        if select_seen:
            # process until we come to the next keyword
            # maybe 'FROM'
            if item.is_keyword:
                return parsed_columns, offset + idx
            elif isinstance(item, IdentifierList):
                columns = [t for t in item.tokens if not is_skip_token(t)]
                for (item_idx, column) in enumerate(columns):
                    select_item = _parse_select_item(column)
                    parsed_columns.append(select_item)
            else:
                select_item = _parse_select_item(item)
                if select_item:
                    parsed_columns.append(select_item)
        # pass the 'SELECT' keyword
        elif item.is_keyword and item.value.upper() == 'SELECT':
            select_seen = True
    # pass all tokens to the end
    return parsed_columns, offset + idx + 1


def _parse_join_conds(join_op, seg, joined: set, offset: int = 0):
    """

    :param seg:
    :param joined:
    :param offset:
    :return:
    """
    # collect join tables until we meet 'ON' keyword
    join_tables, on_offset = collect_tokens_until(seg,
                                                  lambda t: t.is_keyword and t.value.upper() == 'ON',
                                                  offset=offset + 1)
    # the join target table should be singleton
    if len(join_tables) != 1 or not isinstance(join_tables[0], Identifier):
        raise DFSelectParseError("invalid token in join tables: {seg}".format(seg=str(join_tables)))
    join_table = parse_identifier(join_tables[0])

    join_on_tokens, next_offset = collect_tokens_until(seg,
                                                       lambda t: not isinstance(t, Comparison)
                                                                 and not t.value.upper() in ('AND', 'OR'),
                                                       offset=on_offset + 1)
    join_bool_op = 'AND'
    join_conds = []
    check_tbls = {join_table[1]}.union(joined)
    for join_on_token in join_on_tokens:
        if is_skip_token(join_on_token):
            continue
        if isinstance(join_on_token, Comparison):
            join_expr = join_on_token
            left, right = _parse_join_expr(join_expr)

            join_prefixes = {left[0], right[0]}
            if len(join_prefixes) == 1:
                raise DFSelectParseError(
                    "join expression on columns from same table: {seg}".format(seg=str(join_expr)))
            if not check_tbls.issuperset(join_prefixes):
                raise DFSelectParseError(
                    "join expression on columns from separated set: {seg}".format(seg=str(join_expr)))
            join_conds.append((join_bool_op, left, right))

        elif join_on_token.value.upper() in ('AND', 'OR'):
            join_bool_op = join_on_token.value.upper()
        else:
            raise DFSelectParseError("invalid token in join-condition: {seg}".format(seg=str(join_on_tokens)))

    # joined.add(join_table)

    join_op_str = join_op.value.upper()
    join_mode = 'LEFT' if 'LEFT' in join_op_str else 'RIGHT' if 'RIGHT' in join_op_str else 'OUTER' if 'OUTER' in join_op_str else 'INNER'

    return (join_table, join_mode, join_conds), next_offset


def _parse_join_expr(join_expr: Comparison):
    def _parse_join_column(param: str):
        toks = param.split('.')
        if len(toks) < 2:
            raise DFSelectParseError(
                'table prefix is required: {seg}'.format(seg=str(join_expr)))
        if len(toks) > 2:
            raise DFSelectParseError(
                'invalid column identifier in join expression: {seg}'.format(seg=str(join_expr)))
        return toks

    if not isinstance(join_expr.left, Identifier) or not isinstance(join_expr.right, Identifier):
        raise DFSelectParseError(
            'the operand of join expression can only be column identifier: {seg}'.format(seg=str(join_expr)))

    left, _ = parse_identifier(join_expr.left, allow_alias=False)
    right, _ = parse_identifier(join_expr.right, allow_alias=False)

    join_op = join_expr.token_matching(lambda t: t.ttype is compOp, 0)
    if join_op.normalized != '=':
        raise DFSelectParseError('only equal operator supported in join expression: {seg}'.format(seg=str(join_expr)))

    return _parse_join_column(left), _parse_join_column(right)


def _parse_from_clause(tokens, offset: int = 0):
    """
    eval the from clause to retrieve the selected columns
    :param tokens: the token list to parse
    :param offset: the start position to parse
    :return: the parsed from columns
    """
    # the flag to mark from keyword
    from_seen = False
    major_table = None
    joined_tables = set()
    join_clauses = list()
    idx = 0
    for idx, item in enumerate(tokens[offset:]):
        if from_seen:
            if is_skip_token(item):
                continue
            if isinstance(item, Identifier):
                major_table = parse_identifier(item)
                joined_tables.add(major_table[1])
                continue
            if isinstance(item, IdentifierList):
                raise DFSelectParseError(
                    "comma splitted tables is not supported in from-clause near '{seg}', use join instead".format(
                        seg=str(item)))
            if 'JOIN' not in item.value.upper():
                continue

            next_offset = offset + idx
            join_token = item
            while join_token and join_token.is_keyword and 'JOIN' in join_token.value.upper():
                join_by_table, delta = _parse_join_conds(join_token, tokens[offset + idx:], joined_tables)
                joined_tables.add(join_by_table[0])
                join_clauses.append(join_by_table)
                next_offset += delta
                join_token = tokens[next_offset] if len(tokens.tokens) > next_offset else None
            return major_table, join_clauses, next_offset
        elif item.is_keyword and item.value.upper() == 'FROM':
            from_seen = True
    return major_table, join_clauses, offset + idx + 1


def _parse_filter_cond(expr: Comparison, ctx):
    """
    eval the filter expression in where-clause
    :param expr: the filter expression
    :param ctx: the context
    :return: the parsed filter
    """
    op_seen = False
    op_param = None
    op = None
    op_val = None
    for item in expr.tokens:
        if is_skip_token(item):
            continue
        if op_seen:
            if _contains_sub_select(item, ctx):
                op_val = _parse_select(item, ctx)
            else:
                op_val = item
        else:
            if isinstance(item, Identifier):
                op_param = item
            elif item.ttype is compOp:
                op = item
                op_seen = True
    return op, op_param, op_val, expr


def _parse_where_clause(where: Where):
    """
    eval the where clause to get the filter-list
    :param where: the where token
    :return: the parsed filter-list
    """
    where_seen = False
    filter_expr = ''
    for item in where.tokens:
        if is_skip_token(item):
            if not where_seen:
                continue
        if where_seen:
            filter_expr += str(item)
        else:
            if item.value.upper() == 'WHERE':
                where_seen = True
                continue
            raise DFSelectParseError("invalid token in where-class: '{seg}'".format(seg=str(item)))
    return filter_expr

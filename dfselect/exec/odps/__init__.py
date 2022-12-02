# import pandas as pd
# from pandas.core.groupby import DataFrameGroupBy
from odps.df.expr.expressions import CollectionExpr
from odps.df.expr.groupby import GroupBy
from sqlparse.sql import Identifier

from dfselect.context import ctx_load_table, ctx_config_get_table_loaders, ctx_config_add_table_loader, ctx_get_config
from dfselect.errors import DFSelectExecError, DFSelectContextError
from dfselect.log import log
from dfselect.util import check_col_name, is_col_literal, reparse_token, squeeze_blank
from .expr import eval_expr

_o = None


def exec_JOIN(df, ctx: dict, join_table, join_mode, join_exprs):
    """
    join the major table with the join_table
    :param df: the major table data object
    :param ctx: the context object
    :param join_table: the target table data to join
    :param join_mode: the join mode: left/right/inner
    :param join_exprs: the join expression
    :return: joined table data
    """
    join_table_alias = join_table[1]
    join_df = _load_table(ctx, join_table)
    left_on = []
    right_on = []
    for join_expr in join_exprs:
        _, param1, param2 = join_expr
        left, right = (param1, param2) if param2[0] == join_table_alias else (param2, param1)
        left_on.append(left[1])
        right_on.append(right[1])

    merged_df = df.merge(join_df, how=join_mode.lower(), left_on=left_on, right_on=right_on,
                         suffixes=['', '@' + join_table_alias]).rename(
        mapper=lambda x: '.'.join(reversed(x.split('@', 2))) if '@' in x else x, axis=1)
    return merged_df


def exec_PROJECT(df, ctx: dict, *columns):
    if isinstance(df, CollectionExpr):
        df = _extend_columns(df, *columns)
        col_names = list(map(lambda t: t[1], columns))
        proj_col_names = [check_col_name(c, [sc.name for sc in df.columns]) for c in col_names]
        return df[proj_col_names]
    elif isinstance(df, GroupBy):
        gf = df
        agg_columns = _check_and_get_agg_columns([gc.name for gc in gf._by], *columns)
        conds = []
        for agg_column in agg_columns:
            squeezed_column = squeeze_blank(agg_column[0])
            if squeezed_column == 'count(*)':
                conds.append('"' + agg_column[1] + '":pd.Series(r.index).count()')
            else:
                col_item = reparse_token(agg_column[0])
                conds.append('"' + agg_column[1] + '":' + eval_expr(col_item, gf._selected_obj.columns, 'r'))

        group_by_expr = 'pd.Series({' + ','.join(conds) + '})'
        log.debug('generated group-by expr:')
        log.debug(f'> {group_by_expr}')
        # log.debug(f'dataframe before group-by: \n{gf._selected_obj}')
        # log.debug(gf._selected_obj)
        return gf.apply(lambda r: eval(group_by_expr)).reset_index()
    return None


def exec_FILTER(df, ctx: dict, filter_expr):
    import re
    where_expr = re.sub('==*', '==', filter_expr)
    return df.query(where_expr)


def exec_ORDER(df, ctx: dict, *order_items):
    df = _extend_columns(df, *[(o[0], o[0]) for o in order_items])
    sort_by = []
    sort_asc = []
    for order_item in order_items:
        sort_by.append(order_item[0])
        sort_asc.append(order_item[1])
    return df.sort_values(by=sort_by, ascending=sort_asc)


def exec_LIMIT(df, ctx: dict, from_idx, limit):
    return df[:from_idx + limit]


def exec_GROUP(df, ctx: dict, group_items, proj_columns):
    # process projection at first to support group on expression (udf or operation)
    df = _extend_columns(df, *group_items)
    if proj_columns:
        gkeys = [t[0] for t in group_items]
        agg_columns = _check_and_get_agg_columns(gkeys, *proj_columns)

    group_keys = [check_col_name(g[0], [sc.name for sc in df.columns]) for g in group_items]
    gf = df.groupby(group_keys)
    return gf


def exec_LOAD(df, ctx: dict, table: tuple):
    return _load_table(ctx, table)


def initialize(ctx: dict):
    global _o
    from odps import ODPS
    odps_conn_config = ctx_get_config(ctx, 'odps')
    _o = ODPS(**odps_conn_config)

    def _tbl_loader_odps(table_key):
        return _o.get_table(table_key).to_df()

    ctx_config_add_table_loader(ctx, _tbl_loader_odps)


def output(result):
    return result.to_pandas()


def _load_table(ctx: dict, table: tuple):
    table_source, table_alias = table
    df = None
    try:
        df = ctx_load_table(ctx, table_source, table_alias)
    except DFSelectContextError as e:
        extra_table_loaders = ctx_config_get_table_loaders(ctx)
        if extra_table_loaders:
            for extra_table_loader in extra_table_loaders:
                df = extra_table_loader(table_source)
        if df is None:
            raise e

    return df


def _load_udf(func_code: str):
    from . import udf as udf_repo
    udf_name = "udf_" + func_code.upper()
    udf = getattr(udf_repo, udf_name, None)
    if not udf:
        raise DFSelectExecError(f'udf [{func_code}] not defined')
    return udf


def _extend_columns(df, *columns):
    if isinstance(df, CollectionExpr):
        assign_map = dict()
        for column in columns:
            col = column[0]
            if not col or is_col_literal(col):
                const_val = col
                column_series = df.apply(lambda r: const_val, axis=1)
                assign_map[column[1]] = column_series
            else:
                col_item = reparse_token(col)
                if isinstance(col_item, Identifier):
                    # column_series = df.apply(lambda r: r[check_col_name(col, df.columns)], axis=1, names=col).rename(col)
                    # column_series = df.apply(lambda r: str(r[col]), axis=1, names=[col_item.value],
                    #                            types=["string"], reduce=True).rename(col_item.value)
                    orig_column = check_col_name(col, [sc.name for sc in df.columns])
                    df[column[1]] = df[orig_column]
                else:
                    column_series = df.apply(lambda r: eval(eval_expr(col_item, df.columns, 'r')), axis=1)
                    assign_map[column[1]] = column_series
        if assign_map:
            for (col_key, assign_series) in assign_map.items():
                df[col_key] = assign_series
            # df = df.assign(**assign_map)
    return df


def _check_and_get_agg_columns(keys, *columns):
    check_keys = [squeeze_blank(k) for k in keys]
    unmap_keys = [squeeze_blank(k) for k in keys]
    agg_columns = []
    for column in columns:
        squeezed_column = squeeze_blank(column[0])
        if squeezed_column in [squeeze_blank(k) for k in check_keys]:
            unmap_keys.remove(squeezed_column)
        else:
            agg_columns.append(column)
    if len(unmap_keys) > 0:
        raise DFSelectExecError("group-by keys {} not used in select clause".format(unmap_keys))
    return agg_columns

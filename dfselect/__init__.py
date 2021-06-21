from .context import ctx_init
from .parse import parse_select
from .exec import exec_operators


def df_select(query: str, ctx: dict = None, tables: dict = None, config: dict = None):
    """
    process an select query on dataframe
    :param query: the single select query
    :param ctx: the provided context dict object
    :param tables: the tables loaded into context
    :param config: the config dict object
    :return:
    """
    operators = parse_select(query)
    ctx = ctx_init(ctx, tables=tables, config=config)
    result = exec_operators(operators, ctx)
    return result

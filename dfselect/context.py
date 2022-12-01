from .errors import DFSelectContextError
from .log import log

# the key to get the registered tables in context
_CTX_TABLES = 'tables'
# the key to get the configuration of parser and executor
_CTX_CONFIG = 'config'

# the config key to extra table loaders
_CONF_TABLE_LOADERS = 'table_loaders'
# the config key to user-defined executor engine
_CONF_EXEC_ENGINE = 'exec_engine'


def ctx_init(init_ctx: dict = None, tables: dict = None, config: dict = None):
    """
    initialize a context object of the df-select parser/executor
    :param init_ctx: the init dict object of the context
    :param tables: the table dict provided
    :param config: the config dict provided
    :return: the initialized context object
    """
    ctx = dict(init_ctx) if init_ctx else dict()

    # merge the table dict into the context
    _tables = ctx.get(_CTX_TABLES, dict())
    if tables:
        _tables.update(**tables)
    ctx[_CTX_TABLES] = _tables

    # merge the config dict into the context
    _config = ctx.get(_CTX_CONFIG, dict())
    if config:
        _config.update(**config)
    ctx[_CTX_CONFIG] = _config

    return ctx


def ctx_load_table(ctx: dict, table_source: str, table_alias: str = None, alias_replace: bool = True):
    """
    load an registered table from context
    :param ctx: the context object
    :param table_source: the table source/key to get the table object
    :param table_alias: the alias of the table
    :param alias_replace: whether to replace the entry by table_source to table_alias during table-load
    :return: the loaded table object
    """
    if table_source not in ctx[_CTX_TABLES]:
        raise DFSelectContextError('table {} not found'.format(table_source))
    df = ctx[_CTX_TABLES][table_source]

    # if the table key and alias is different and we want to replace the table key with alias
    if table_alias and table_alias != table_source:
        ctx[table_alias] = df
        if alias_replace:
            del ctx[_CTX_TABLES][table_source]
    return df


def ctx_add_table(ctx: dict, table_key: str, df, replace=False):
    """
    register a table into the context
    :param ctx: the context object
    :param table_key: the table key
    :param df: the table data object
    :param replace: whether to replace the existed table entry
    :return: None
    """
    if table_key in ctx[_CTX_TABLES]:
        if not replace:
            log.warning(f'table {table_key} already exists, ignore this operation')
        else:
            log.warning(f'table {table_key} already exists, will be replaced')
    ctx[_CTX_TABLES][table_key] = df


def ctx_set_config(ctx: dict, config_key: str, config_value):
    """
    set a config item into the context
    :param ctx: the context object
    :param config_key: the config key
    :param config_value: the config value
    :return: None
    """
    ctx[_CTX_CONFIG][config_key] = config_value


def ctx_append_config(ctx: dict, config_key: str, config_value, pos=None, clear=False):
    """
    append a config item into the list-like config entry of the context
    :param ctx: the context object
    :param config_key: the config key
    :param config_value: the config value
    :param pos: the position to place the config value
    :param clear: whether to clear the config entry list
    :return: None
    """
    if config_key not in ctx[_CTX_CONFIG] or clear:
        ctx[_CTX_CONFIG][config_key] = []
    if not pos or clear:
        ctx[_CTX_CONFIG][config_key].append(config_value)
    else:
        ctx[_CTX_CONFIG][config_key].insert(pos, config_value)


def ctx_get_config(ctx: dict, config_key: str, default_value=None):
    """
    get the config value from the context
    :param ctx: the context object
    :param config_key: the config key
    :param default_value: the default value to return if the config key is missed
    :return: the config value
    """
    return ctx[_CTX_CONFIG][config_key] if config_key in ctx[_CTX_CONFIG] else default_value


def ctx_config_add_table_loader(ctx: dict, table_loader, pos=None, clear=False):
    """
    add a new table loader into context
    :param ctx: the context object
    :param table_loader: the table loader
    :param pos: the position to place the table loader
    :param clear: whether to clear the config entry list
    :return: None
    """
    ctx_append_config(ctx, _CONF_TABLE_LOADERS, table_loader, pos=pos, clear=clear)


def ctx_config_get_table_loaders(ctx: dict):
    """
    get the table loader list from the context
    :param ctx: the context object
    :return: the table loader list
    """
    return ctx_get_config(ctx, _CONF_TABLE_LOADERS)


def ctx_config_set_exec_engine(ctx: dict, exec_engine):
    """
    set the executor engine to use into context
    :param ctx: the context object
    :param exec_engine: the executor engine module
    :return: None
    """
    ctx_set_config(ctx, _CONF_EXEC_ENGINE, exec_engine)
    if hasattr(exec_engine, 'initialize'):
        exec_engine.initialize(ctx)


def ctx_config_get_exec_engine(ctx: dict):
    """
    get the executor engine from the context
    :param ctx: the context object
    :return: the used executor engine
    """
    from .exec import pandas as pandas_engine
    return ctx_get_config(ctx, _CONF_EXEC_ENGINE, pandas_engine)

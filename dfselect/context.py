from .errors import DFSelectContextError
from .log import log

_CTX_TABLES = 'tables'
_CTX_CONFIG = 'config'

_CONF_TABLE_LOADERS = 'table_loaders'
_CONF_EXEC_ENGINE = 'exec_engine'


def ctx_init(init_ctx: dict = None, tables: dict = None, config: dict = None):
    ctx = dict(init_ctx) if init_ctx else dict()
    _tables = ctx.get(_CTX_TABLES, dict())
    if tables:
        _tables.update(**tables)
    ctx[_CTX_TABLES] = _tables
    _config = ctx.get(_CTX_CONFIG, dict())
    if config:
        _config.update(**config)
    ctx[_CTX_CONFIG] = _config
    return ctx


def ctx_load_table(ctx: dict, table_source: str, table_alias: str = None, alias_replace: bool = True):
    if table_source not in ctx[_CTX_TABLES]:
        raise DFSelectContextError('table {} not found'.format(table_source))
    df = ctx[_CTX_TABLES][table_source]
    if table_alias and table_alias != table_source:
        ctx[table_alias] = df
        if alias_replace:
            del ctx[_CTX_TABLES][table_source]
    return df


def ctx_add_table(ctx: dict, table_key: str, df, replace=False):
    if table_key in ctx[_CTX_TABLES]:
        if not replace:
            log.warning(f'table {table_key} already exists, ignore this operation')
        else:
            log.warning(f'table {table_key} already exists, will be replaced')
    ctx[_CTX_TABLES][table_key] = df


def ctx_set_config(ctx: dict, config_key: str, config_value):
    ctx[_CTX_CONFIG][config_key] = config_value


def ctx_append_config(ctx: dict, config_key: str, config_value, pos=None):
    if config_key not in ctx[_CTX_CONFIG]:
        ctx[_CTX_CONFIG][config_key] = []
    if not pos:
        ctx[_CTX_CONFIG][config_key].append(config_value)
    else:
        ctx[_CTX_CONFIG][config_key].insert(pos, config_value)


def ctx_get_config(ctx: dict, config_key: str, default_value=None):
    return ctx[_CTX_CONFIG][config_key] if config_key in ctx[_CTX_CONFIG] else default_value


def ctx_config_add_table_loader(ctx: dict, table_loader, pos=None):
    ctx_append_config(ctx, _CONF_TABLE_LOADERS, table_loader, pos=pos)


def ctx_config_get_table_loaders(ctx: dict):
    return ctx_get_config(ctx, _CONF_TABLE_LOADERS)


def ctx_config_set_exec_engine(ctx: dict, exec_engine):
    ctx_set_config(ctx, _CONF_EXEC_ENGINE, exec_engine)


def ctx_config_get_exec_engine(ctx: dict):
    from .exec import operator as pandas_operator
    return ctx_get_config(ctx, _CONF_EXEC_ENGINE, pandas_operator)

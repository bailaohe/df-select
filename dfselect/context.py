from .errors import DFSelectContextError

_CTX_TABLES = 'tables'
_CTX_CONFIG = 'config'


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
            del ctx['tables'][table_source]
    return df

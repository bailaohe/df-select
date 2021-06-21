from sqlparse.sql import Comment, Identifier
from sqlparse.tokens import Name, Punctuation, Literal

from .errors import DFSelectParseError, DFSelectExecError


def is_skip_token(item, reserve_punctuation=False):
    """
    check if the token can be skipped
    :param reserve_punctuation:
    :param item: the token
    :return: bool-result
    """
    if not reserve_punctuation:
        return item.is_whitespace or isinstance(item, Comment) or item.ttype is Punctuation
    return item.is_whitespace or isinstance(item, Comment)


def move_on_next(tokens, offset: int = 0):
    """
    move until we touch the next un-skipped token or EOF
    :param tokens: the token list
    :param offset: the start position to parse
    :return: the index of next un-skipped token
    """
    idx = 0
    for idx, item in enumerate(tokens[offset:]):
        if is_skip_token(item):
            continue
        return offset + idx
    return offset + idx + 1


def collect_tokens_until(tokens, matcher, offset: int = 0):
    """
    collect the sub-token list until we come to the next keyword or EOF
    :param tokens: the token list
    :param offset: the start position to parse
    :return: the extracted sub-token list and the next offset
    """
    extracted = []
    idx = 0
    for idx, item in enumerate(tokens[offset:]):
        if is_skip_token(item):
            continue
        if matcher(item):
            return extracted, offset + idx
        else:
            extracted.append(item)
    return extracted, offset + idx + 1


def parse_identifier(identifier: Identifier, allow_alias=True, allow_literal=False):
    toks = [t for t in identifier.tokens if not is_skip_token(t, reserve_punctuation=True) and not t.is_keyword]

    if not allow_literal and toks[0].ttype in Literal:
        raise DFSelectParseError('invalid identifier:', str(identifier), 'literal not supported')

    if len(toks) <= 2:
        identifier_value = eval_literal_value(toks[0]) if toks[0].ttype in Literal else toks[0].value

        if len(toks) == 1:
            return identifier_value, toks[0].value.lower()
        if not allow_alias:
            raise DFSelectParseError('invalid identifier:', str(identifier), 'alias not supported')
        # if toks[0] == toks[1]:
        #     raise ParadeSqlParseError('invalid identifier:', str(identifier), 'the same value of alias and key')
        return identifier_value, toks[1].value.lower()

    else:
        if toks[0].ttype is Name and toks[1].value == '.' and toks[2].ttype is Name:
            if len(toks) > 4:
                raise DFSelectParseError('invalid identifier:', str(identifier))
            identifier_value = toks[0].value + '.' + toks[2].value
            return identifier_value, identifier_value.lower() if len(toks) == 3 else toks[3].value.lower()

    raise DFSelectParseError('invalid identifier:', str(identifier))


def eval_literal_value(item):
    const_val = int(item.value) if item.ttype in Literal.Number.Integer else float(
        item.value) if item.ttype in Literal.Number.Float else item.value
    return const_val


def check_col_name(col_name: str, col_set):
    final_name = col_name
    if final_name not in col_set:
        final_name = final_name.split('.')[-1]
    if final_name not in col_set:
        raise DFSelectExecError('invalid column {}'.format(col_name))
    return final_name


def is_col_literal(col_val):
    return isinstance(col_val, (int, float)) or str(col_val).strip().startswith('\'') or str(
        col_val).strip().startswith(
        '"')


def reparse_token(token):
    import sqlparse as sp
    return sp.parse('select ' + token)[0].tokens[-1]


def squeeze_blank(seg):
    import re
    pattern = re.compile(r'\s+')
    return re.sub(pattern, '', seg)

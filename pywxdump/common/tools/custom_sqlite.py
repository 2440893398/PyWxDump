import sys
import traceback
from enum import auto
import typing as t
from sqlglot import exp, parse_one
from sqlglot.dialects.sqlite import SQLite
from sqlglot.expressions import Like, Binary, Predicate, Func
from sqlglot.helper import AutoName


class CustomTokenType(AutoName):
    MATCH = auto(),
    SIMPLE_QUERY = auto()


class Match(exp.Binary, exp.Predicate):
    pass


class SimpleQuery(exp.Func):
    arg_types = {"this": True, "expression": False}


def _build_match(args: t.List) -> Match:
    return Match(this=args[0], expression=args[1])


def _build_simple_query(args: t.List) -> SimpleQuery:
    return SimpleQuery(this=args[0], expression=args[1])


class CustomSQLite(SQLite):
    class Tokenizer(SQLite.Tokenizer):
        KEYWORDS = {**SQLite.Tokenizer.KEYWORDS,
                    "MATCH": CustomTokenType.MATCH,
                    "SIMPLE_QUERY": CustomTokenType.SIMPLE_QUERY}

    class Parser(SQLite.Parser):
        FUNCTIONS = {
            **SQLite.Parser.FUNCTIONS,
            "MATCH": _build_match,
            "SIMPLE_QUERY": _build_simple_query
        }

        def expression(self, exp_class=exp.Expression, comments=None, **kwargs):
            node = super().expression(exp_class, comments, **kwargs)
            if self._match(CustomTokenType.MATCH):
                expression = self.expression(exp_class, comments, **kwargs)
                return Match(this=node, expression=expression)
            if self._match(CustomTokenType.SIMPLE_QUERY):
                self._advance()  # Advance to the next token after SIMPLE_QUERY
                args = self._parse_string()  # Parse the arguments inside the parentheses
                self._match_r_paren()  # Match the closing parenthesis
                return SimpleQuery(this=node, expression=args)
            return node

    class Generator(SQLite.Generator):
        def match_sql(self, expression):
            return f"{self.sql(expression.this)} MATCH {self.sql(expression.expression)}"

        def simple_query_sql(self, expression):
            return f"SIMPLE_QUERY({self.sql(expression.expression.sql())})"

        def sql(self, expression, key=None, comment=True):
            if isinstance(expression, Match):
                return self.match_sql(expression)
            elif isinstance(expression, SimpleQuery):
                return self.simple_query_sql(expression)
            return super().sql(expression, key, comment)


        TRANSFORMS = {
            **SQLite.Generator.TRANSFORMS,
            Match: match_sql,
            SimpleQuery: simple_query_sql
        }


if __name__ == '__main__':
    def parse_with_custom_sqlite(sql):
        return parse_one(sql, read=CustomSQLite)


    sql = """
     select talker.UserName as talker,
                           talker.NickName            as nickName,
                           talker.Alias               as alias,
                           talker.Remark              as remark,
                           contactImage.bigHeadImgUrl as bigHeadImgUrl,
                           group_concat(fts.ROWID) as rowids,
                           count(fts.ROWID) as rowidCount
                    from FTSMSG__FTSChatMsg2 as fts
                    INNER JOIN FTSMSG__FTSChatMsg2_MetaData as metaData ON fts.rowid = metaData.docid
                    INNER JOIN FTSMSG__NameToId as nameToId ON metaData.entityId = nameToId.ROWID
                    INNER JOIN Contact as talker ON nameToId.userName = talker.userName
                    LEFT JOIN ContactHeadImgUrl AS contactImage ON contactImage.usrName = talker.userName
                    where fts.content match simple_query(?)
                    group by talker.UserName
    """

    try:
        print(Like)
        parsed = parse_with_custom_sqlite(sql)
        print("SQL parsed successfully:")
        print(parsed.sql(dialect=CustomSQLite))
    except Exception as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        print("".join(traceback.format_exception(exc_type, exc_value, exc_traceback)))
        print(f"Error parsing SQL: {e}")

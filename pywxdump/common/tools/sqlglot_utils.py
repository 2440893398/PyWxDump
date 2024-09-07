import sqlglot
from sqlglot import TokenType, Token, Parser
from sqlglot.expressions import Table, AggFunc, Group, Limit, Offset, Placeholder

from pywxdump.common.tools.custom_sqlite import CustomSQLite


class SqlglotUtils:
    _multi_table = {"FTSMSG", "MediaMSG", "MSG"}

    @staticmethod
    def exist_multi_base(tree):
        """
        判断是否存在分表查询

        :param tree: 语法树
        :return: 是否存在分表查询
        """
        multi_tables = []

        for node in tree.walk():
            if isinstance(node, Table):
                if node.name in SqlglotUtils._multi_table:
                    multi_tables.append(node.name)
        return len(multi_tables) > 0, multi_tables

    @staticmethod
    def is_aggregate(tree):
        """
        判断是否存在聚合函数

        :param tree: 语法树
        :return: 是否存在聚合函数
        """
        for node in tree.walk():
            if isinstance(node, AggFunc) or isinstance(node, Group):
                return True
        return False

    @staticmethod
    def is_page(tree):
        """
        判断是否存在分页

        :param tree: 语法树
        :return: 是否存在分页
        """
        for node in tree.walk():
            if isinstance(node, Limit) or isinstance(node, Offset):
                return True
        return False

    @classmethod
    def remove_page(cls, sqlTree):
        """
        移除分页

        :param sqlTree: 语法树
        :return: 无分页的语法树

        """

        def remove_limit(node):
            if isinstance(node, Limit) or isinstance(node, Offset):
                return node.args['expression']
            return node

        return sqlTree.transform(remove_limit)

    @staticmethod
    def get_select_fields_name(sqlTree, is_ignore_alias=False):
        """
        获取select字段

        :param sqlTree: 语法树
        :param is_ignore_alias: 是否忽略别名

        :return: select字段
        """
        select_fields = []

        select = SqlglotUtils.get_select_fields(sqlTree)
        if not select:
            return select_fields

        for expr in select:
            select_fields.append(SqlglotUtils.get_field_name(expr, is_ignore_alias))

        return select_fields

    @staticmethod
    def get_select_fields(sqlTree):
        """
        获取select字段

        :param sqlTree: 语法树

        :return: select字段
        """

        select = sqlTree.find(sqlglot.expressions.Select)
        if not select:
            return None

        return select.expressions

    @staticmethod
    def get_field_name(expr, is_ignore_alias=False) -> str:
        if expr.alias:
            if is_ignore_alias:
                return SqlglotUtils.sql(expr.this)
            else:
                return expr.alias
        else:
            return SqlglotUtils.sql(expr)

    @staticmethod
    def get_group_fields(sqlTree):
        """
        获取select字段

        :param sqlTree: 语法树
        :return: select字段
        """
        select_fields = []
        if sqlTree.find(sqlglot.expressions.Group):
            expressions = sqlTree.find(sqlglot.expressions.Group).expressions
            for expr in expressions:
                select_fields.append(SqlglotUtils.sql(expr))
        return select_fields

    @staticmethod
    def parse_one(sqlStr: str) -> sqlglot.expressions.Expression:
        """
        解析SQL

        :param sqlStr: SQL语句

        :return: 语法树
        """
        return sqlglot.parse_one(sqlStr, read=CustomSQLite)

    @staticmethod
    def sql(sqlTree) -> str:
        """
        打印sql

        :param sqlTree: 语法树

        :return: sql
        """
        return sqlTree.sql(dialect=CustomSQLite)


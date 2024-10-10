from abc import ABC, abstractmethod
from typing import Dict, Type

from sqlalchemy import Result, Row
from sqlglot.expressions import Expression

from pywxdump.common.tools.sqlglot_utils import SqlglotUtils
from pywxdump.db.aggregate.AggregateStrategyEnum import AggregateStrategyEnum
from pywxdump.db.aggregate.MutableRow import MutableRow


class ResultAggregateStrategy(ABC):
    """
    聚合策略
    """

    # 分隔符
    _separator = ','
    strategies: Dict[AggregateStrategyEnum, Type['ResultAggregateStrategy']] = {}

    @classmethod
    def register(cls, strategyEnum: AggregateStrategyEnum):
        def decorator(subclass):
            cls.strategies[strategyEnum] = subclass
            return subclass

        return decorator

    def __init__(self, sqlTree: Expression, selectField: Expression):
        if not self.check(selectField):
            raise ValueError(f'不支持的聚合函数{SqlglotUtils.sql(selectField)}')

        groupFields = SqlglotUtils.get_group_fields(sqlTree)
        self.sqlTree = sqlTree

        self.groupKey = self._separator.join(groupFields)
        # 获取group by字段的索引
        self.groupFieldIndex = set()
        self.aggregateField = selectField
        self.aggregateFieldIndex = None
        selectFields = SqlglotUtils.get_select_fields_name(self.sqlTree, True)
        for i in range(len(selectFields)):
            if selectFields[i] in groupFields:
                self.groupFieldIndex.add(i)
        selectFields = SqlglotUtils.get_select_fields_name(self.sqlTree, False)
        for i in range(len(selectFields)):
            if selectFields[i] in groupFields:
                self.groupFieldIndex.add(i)
            if selectFields[i] == SqlglotUtils.get_field_name(self.aggregateField, False):
                self.aggregateFieldIndex = i
        pass

    @abstractmethod
    def aggregate(self, existing_row: MutableRow, row: Row):
        """
        将实际结果与结果相加

        :param existing_row: 聚合结果
        :param row: 聚合结果

        :return: 聚合结果
        """
    def sql_preprocessing(self, sqlTree: Expression) -> Expression:
        """
        SQL预处理

        :param sqlTree: SQL语法树

        :return: SQL语法树
        """
        return sqlTree

    @staticmethod
    @abstractmethod
    def check(expressions: Expression) -> bool:
        """
        根据检查表达式检查是否使用这个策略

        :param expressions: select 参数

        :return: 结果
        """

    def get_row_key(self, row):
        """
        获取行的key

        :param row: 行

        :return: key
        """
        key = tuple(str(row[i]) for i in self.groupFieldIndex)
        keyStr = self._separator.join(key)
        return keyStr

from typing import Dict

from sqlalchemy import Result, Row
from sqlglot.expressions import Count, Expression

from pywxdump.db.aggregate.AggregateStrategyEnum import AggregateStrategyEnum
from pywxdump.db.aggregate.MutableRow import MutableRow
from pywxdump.db.aggregate.result_aggregate import ResultAggregateStrategy


@ResultAggregateStrategy.register(AggregateStrategyEnum.COUNT)
class CountAggregateStrategy(ResultAggregateStrategy):
    """
    计数聚合策略
    """

    def __init__(self, sqlTree, selectField):
        super().__init__(sqlTree, selectField)
        pass

    def aggregate(self, existing_row: MutableRow, row: Row):
        """
        将实际结果与结果相加

        :param aggregated_results: 流式聚合结果
        :param result: 聚合结果

        :return: 聚合结果
        """
        count_value = row[self.aggregateFieldIndex]

        value1 = existing_row[self.aggregateFieldIndex]
        value1 = count_value + value1
        existing_row[self.aggregateFieldIndex] = value1



    @staticmethod
    def check(expressions: Expression):
        """
        根据检查表达式检查是否使用这个策略

        :param expressions: select 参数

        :return: 结果
        """
        if expressions.find(Count):
            return True
        else:
            return False

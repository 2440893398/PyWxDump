from typing import Dict

from sqlalchemy import Row
from sqlglot import Expression
from sqlglot.expressions import Count, AggFunc

from pywxdump.dbpreprocess.aggregate.AggregateStrategyEnum import AggregateStrategyEnum
from pywxdump.dbpreprocess.aggregate.MutableRow import MutableRow
from pywxdump.dbpreprocess.aggregate.result_aggregate import ResultAggregateStrategy

@ResultAggregateStrategy.register(AggregateStrategyEnum.OVERRIDE)
class OverrideStrategy(ResultAggregateStrategy):
    """
     覆盖策略
     无法合并的直接覆盖
    """

    def __init__(self, sqlTree, selectField):
        super().__init__(sqlTree, selectField)
        pass

    def aggregate(self, existing_row: MutableRow, row: Row):
        """
        将实际结果与结果相加

        :param existing_row: 聚合结果
        :param row: 聚合结果

        :return: 聚合结果
        """
        existing_row[self.aggregateFieldIndex] = row[self.aggregateFieldIndex]

    @staticmethod
    def check(expressions: Expression) -> bool:
        """
        根据检查表达式检查是否使用这个策略

        :param expressions: select 参数

        :return: 结果
        """
        # 不包含聚合函数
        if expressions.find(AggFunc):
            return False
        else:
            return True

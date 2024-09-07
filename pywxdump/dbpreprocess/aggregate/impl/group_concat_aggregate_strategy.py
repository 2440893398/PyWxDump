from typing import Dict

from sqlalchemy import Row
from sqlglot import Expression
from sqlglot.expressions import GroupConcat

from pywxdump.dbpreprocess.aggregate.AggregateStrategyEnum import AggregateStrategyEnum
from pywxdump.dbpreprocess.aggregate.MutableRow import MutableRow
from pywxdump.dbpreprocess.aggregate.result_aggregate import ResultAggregateStrategy


@ResultAggregateStrategy.register(AggregateStrategyEnum.GROUP_CONCAT)
class GroupConcatAggregateStrategy(ResultAggregateStrategy):
    """
    GROUP_CONCAT 聚合策略
    """

    def __init__(self, sqlTree, selectField):
        super().__init__(sqlTree, selectField)
        self.__getSeparator(selectField)

    def aggregate(self, existing_row: MutableRow, row: Row):
        """
        将实际结果与结果相加

        :param existing_row: 聚合结果
        :param row: 当前行

        :return: 聚合结果
        """
        current_value = row[self.aggregateFieldIndex]

        existing_value = existing_row[self.aggregateFieldIndex]
        if existing_value:
            existing_row[self.aggregateFieldIndex] = f"{existing_value}{self.separator}{current_value}"
        else:
            existing_row[self.aggregateFieldIndex] = current_value

    def __getSeparator(self, sqlTree: Expression):
        """
        获取分隔符

        :param sqlTree: sql树
        """
        # 默认分隔符为逗号
        self.separator = ","

        # 对于 GROUP_CONCAT，我们需要提取可能的分隔符
        for node in sqlTree.find_all(GroupConcat):
            if node.args and len(node.args) > 1:
                separatorArg = node.args["separator"]
                if separatorArg:
                    self.separator = separatorArg.this

    @staticmethod
    def check(expressions):
        """
        根据检查表达式检查是否使用这个策略

        :param expressions: select 参数

        :return: 结果
        """
        if expressions.find(GroupConcat):
            return True
        else:
            return False

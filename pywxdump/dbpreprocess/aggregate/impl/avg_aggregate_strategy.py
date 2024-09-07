from typing import Dict

from sqlalchemy import Row
from sqlglot import Expression
from sqlglot.expressions import Avg, Select, Count, Sum, Literal

from pywxdump.common.tools.sqlglot_utils import SqlglotUtils
from pywxdump.dbpreprocess.aggregate.AggregateStrategyEnum import AggregateStrategyEnum
from pywxdump.dbpreprocess.aggregate.MutableRow import MutableRow
from pywxdump.dbpreprocess.aggregate.result_aggregate import ResultAggregateStrategy


@ResultAggregateStrategy.register(AggregateStrategyEnum.AVG)
class AvgAggregateStrategy(ResultAggregateStrategy):
    """
    平均值聚合策略
    """


    def __init__(self, sqlTree, selectField):
        super().__init__(sqlTree, selectField)
        self.count_field_index = None
        self.sum_field_index = None

    def aggregate(self, existing_row: MutableRow, row: Row):
        """
        将实际结果与结果相加

        :param aggregated_results: 流式聚合结果
        :param result: 聚合结果

        :return: 聚合结果
        """
        existing_row[self.sum_field_index] += row[self.sum_field_index]
        existing_row[self.count_field_index] += row[self.count_field_index]
        if existing_row[self.count_field_index] != 0:
            existing_row[self.aggregateFieldIndex] = (existing_row[self.sum_field_index] /
                                                      existing_row[self.count_field_index])
        else:
            raise ValueError("count field is 0")

    def sqlPreprocessing(self, sqlTree: Expression) -> Expression:
        """
        预处理sql

        :param sqlTree: sql树

        :return: 预处理后的sql树
        """
        # 分别查询的sql增加count字段 和 sum字段 用于聚合平均值
        sqlTree = sqlTree.copy()
        selectFields = SqlglotUtils.get_select_fields(sqlTree)
        for index, selectField in enumerate(selectFields):
            if selectField == self.aggregateField:
                # 替换 AVG 为 SUM，并添加别名
                sum_field = Sum(
                    this=selectField.this,
                    alias=f'__sum_{selectField.alias or selectField.this}'
                )
                selectFields.append(sum_field)
                self.sum_field_index = len(selectFields) - 1

                # 添加 COUNT 字段
                count_field = Count(
                    this=selectField.this,
                    alias=f'__count_{selectField.alias or selectField.this}'
                )
                selectFields.append(count_field)
                self.count_field_index = len(selectFields) - 1

        return sqlTree

    @staticmethod
    def check(expressions):
        """
        根据检查表达式检查是否使用这个策略

        :param expressions: select 参数

        :return: 结果
        """
        if expressions.find(Avg):
            return True
        else:
            return False

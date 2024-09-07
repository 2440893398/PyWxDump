import importlib
import os

from sqlglot import Expression

from pywxdump.common.tools.sqlglot_utils import SqlglotUtils
from pywxdump.dbpreprocess.aggregate.result_aggregate import ResultAggregateStrategy
from pywxdump.dbpreprocess.aggregate.impl import *


class AggregateStrategyFactory:

    @classmethod
    def getFieldAggregates(cls, sqlTree: Expression):
        """
        获取字段聚合策略
        """
        result = {}
        # 获取select字段
        if SqlglotUtils.is_aggregate(sqlTree):
            selectFields = SqlglotUtils.get_select_fields(sqlTree)
            for index, selectField in enumerate(selectFields):
                for strategy_class in ResultAggregateStrategy.strategies.values():
                    if strategy_class.check(selectField):
                        result[index] = strategy_class(sqlTree, selectField)
                        break
        return result

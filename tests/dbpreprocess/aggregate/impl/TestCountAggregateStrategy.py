import unittest
from unittest.mock import MagicMock
from sqlalchemy import Result, Row
from sqlglot import parse_one
from sqlglot.expressions import Count

from pywxdump.dbpreprocess.aggregate.MutableRow import MutableRow
from pywxdump.dbpreprocess.aggregate.impl.count_aggregate_strategy import CountAggregateStrategy


class TestCountAggregateStrategy(unittest.TestCase):

    def test_aggregate_results_correctly(self):
        sql = "select group1, count(value) from table group by group1"
        tree = parse_one(sql)
        selectField = tree.find(Count)
        strategy = CountAggregateStrategy(sqlTree=tree, selectField=selectField)
        strategy.groupFieldIndex = [0]
        strategy.aggregateFieldIndex = 1
        strategy.__separator = "_"

        aggregated_results = MutableRow(Row(MagicMock(), None, {'group1': 0, 'count': 1}, ('group1', 10)))

        strategy.aggregate(aggregated_results, Row(MagicMock(), None, {'group1': 0, 'count': 1}, ('group1', 15)))
        self.assertEqual(aggregated_results[1], 25)


if __name__ == "__main__":
    unittest.main()

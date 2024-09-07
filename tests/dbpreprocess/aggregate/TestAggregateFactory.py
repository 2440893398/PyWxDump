import unittest
from unittest.mock import MagicMock
from sqlglot import Expression, parse_one
from pywxdump.dbpreprocess.aggregate.aggregate_factory import AggregateStrategyFactory
from pywxdump.common.tools.sqlglot_utils import SqlglotUtils


class TestAggregateFactory(unittest.TestCase):

    def test_getFieldAggregates_returns_empty_dict_when_no_aggregates(self):
        sql = "select group1, value from table "
        tree = parse_one(sql)

        result = AggregateStrategyFactory.getFieldAggregates(tree)

        self.assertEqual(result, {})

    def test_getFieldAggregates_returns_aggregates_when_aggregates_present(self):
        sql = "select count(group1), count(value) from table "
        tree = parse_one(sql)

        result = AggregateStrategyFactory.getFieldAggregates(tree)

        self.assertEqual(len(result), 2)
        self.assertIn(0, result)
        self.assertIn(1, result)

    def test_getFieldAggregates_skips_non_matching_subclasses(self):
        sql = "select cll(group1), value from table "
        tree = parse_one(sql)

        result = AggregateStrategyFactory.getFieldAggregates(tree)

        self.assertEqual(result, {})


if __name__ == '__main__':
    unittest.main()

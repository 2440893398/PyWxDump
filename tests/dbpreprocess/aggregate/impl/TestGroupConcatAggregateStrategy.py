import unittest
from unittest.mock import MagicMock

from sqlalchemy import Row
from sqlglot import parse_one
from sqlglot.expressions import GroupConcat, Select, Column, Table

from pywxdump.dbpreprocess.aggregate.MutableRow import MutableRow
from pywxdump.dbpreprocess.aggregate.impl.group_concat_aggregate_strategy import GroupConcatAggregateStrategy


class TestGroupConcatAggregateStrategy(unittest.TestCase):

    def setUp(self):
        sql = "SELECT GROUP_CONCAT(name) FROM users GROUP BY age"
        tree = parse_one(sql)
        select_field = tree.find(GroupConcat)
        self.strategy = GroupConcatAggregateStrategy(tree, select_field)

    def test_check(self):
        expr_with_group_concat = parse_one("SELECT GROUP_CONCAT(name) FROM users")
        expr_without_group_concat = parse_one("SELECT name FROM users")

        self.assertTrue(GroupConcatAggregateStrategy.check(expr_with_group_concat))
        self.assertFalse(GroupConcatAggregateStrategy.check(expr_without_group_concat))

    def test_aggregate(self):
        aggregated_results = MutableRow(self.create_row([30, "Alice"]))

        # 模拟数据行
        row2 = self.create_row([30, "Bob"])

        self.strategy.aggregateFieldIndex = 1  # name 字段的索引
        self.strategy.get_row_key = lambda row: str(row[0])  # 使用 age 作为 key

        self.strategy.aggregate(aggregated_results, row2)

        self.assertEqual(aggregated_results[1], "Alice,Bob")

    def test_sql_preprocessing(self):
        sql_with_separator = "SELECT GROUP_CONCAT(name, '|') FROM users GROUP BY age"
        tree_with_separator = parse_one(sql_with_separator)
        select_field_with_separator = tree_with_separator.find(GroupConcat)
        strategy_with_separator = GroupConcatAggregateStrategy(tree_with_separator, select_field_with_separator)

        self.assertEqual(strategy_with_separator.separator, '|')

    def test_custom_separator_aggregate(self):
        self.strategy.separator = '|'
        aggregated_results =  MutableRow(self.create_row([30, "Alice"]))

        row2 = self.create_row([30, "Bob"])

        self.strategy.aggregateFieldIndex = 1
        self.strategy.get_row_key = lambda row: str(row[0])

        self.strategy.aggregate(aggregated_results, row2)

        self.assertEqual(aggregated_results[1], "Alice|Bob")

    def create_row(self, data):
        mock_parent = MagicMock()
        mock_key_to_index = {f'column_{i}': i for i in range(len(data))}
        return Row(mock_parent, None, mock_key_to_index, data)

if __name__ == '__main__':
    unittest.main()

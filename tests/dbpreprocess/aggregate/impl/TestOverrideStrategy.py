import unittest
from unittest.mock import MagicMock, patch
from sqlalchemy import Row
from sqlglot import parse_one

from pywxdump.common.tools.sqlglot_utils import SqlglotUtils
from pywxdump.dbpreprocess.aggregate.impl.override_strategy import OverrideStrategy
from pywxdump.dbpreprocess.aggregate.MutableRow import MutableRow


class TestOverrideStrategy(unittest.TestCase):

    def create_row(self, data):
        mock_parent = MagicMock()
        mock_key_to_index = {f'column_{i}': i for i in range(len(data))}
        return Row(mock_parent, None, mock_key_to_index, data)

    def test_aggregate_adds_new_row_if_key_not_exists(self):
        sql = "SELECT column_0 FROM table"
        tree = parse_one(sql)
        selectFields = SqlglotUtils.get_select_fields(tree)
        self.strategy = OverrideStrategy(tree, selectFields[0])
        real_result = MutableRow(self.create_row(['']))

        self.strategy.aggregate(real_result, self.create_row(['field1']))
        self.assertEqual(real_result[0], "field1")

    def test_aggregate_updates_existing_row_if_key_exists(self):
        sql = "SELECT column_0 FROM table"
        tree = parse_one(sql)
        selectFields = SqlglotUtils.get_select_fields(tree)
        self.strategy = OverrideStrategy(tree, selectFields[0])
        real_result = MutableRow(self.create_row(['field1']))
        self.strategy.aggregate(real_result, self.create_row(['field2']))
        self.assertEqual(real_result[0], "field2")

    def test_check_returns_false_if_aggfunc_found(self):
        sql = "SELECT count(name) as name FROM table"
        tree = parse_one(sql)
        selectFields = SqlglotUtils.get_select_fields(tree)
        result = OverrideStrategy.check(selectFields[0])
        self.assertFalse(result)

    def test_check_returns_true_if_no_aggfunc_found(self):
        sql = "SELECT name as name FROM table"
        tree = parse_one(sql)
        selectFields = SqlglotUtils.get_select_fields(tree)
        result = OverrideStrategy.check(selectFields[0])
        self.assertTrue(result)


if __name__ == '__main__':
    unittest.main()

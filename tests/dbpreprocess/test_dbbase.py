import unittest
from unittest.mock import patch, MagicMock

import sqlglot
from sqlalchemy import Row
from sqlglot.expressions import AggFunc

import unittest
from sqlglot import parse_one, expressions

from pywxdump.common.tools.sqlglot_utils import SqlglotUtils
from pywxdump.db.aggregate.MutableRow import MutableRow
from pywxdump.db.dbbase import DatabaseBase


class TestDatabaseBase(unittest.TestCase):
    @patch.object(DatabaseBase, '__init__', return_value=None)
    def setUp(self, mock_init):
        db_config = {
            "key": "test1",
            "type": "sqlite",
            "path": r"C:\***\wxdump_work\merge_all.db"
        }
        self.db_base = DatabaseBase(db_config)
        self.db_base._db_path = 'dummy_db_path'

    def test_exist_multi_table_with_single_table2(self):
        tree = parse_one("SELECT * FROM MSG__Name2ID")
        result, tables = self.db_base._DatabaseBase__exist_multi_base(tree)
        self.assertTrue(result)
        self.assertEqual(tables, ['MSG__Name2ID'])

    def test_exist_multi_table_with_multiple_tables(self):
        tree = parse_one("SELECT * FROM FTSMSG__NameToId JOIN MediaMSG__Media ON FTSMSG.id = MediaMSG.id")

        result, tables = self.db_base._DatabaseBase__exist_multi_base(tree)
        self.assertTrue(result)
        self.assertEqual(tables, ['FTSMSG__NameToId', 'MediaMSG__Media'])

    def test_exist_multi_table_with_no_matching_tables(self):
        tree = parse_one("SELECT * FROM SomeOtherTable")
        result, tables = self.db_base._DatabaseBase__exist_multi_base(tree)
        self.assertFalse(result)
        self.assertEqual(tables, [])

    def test_count_aggregate_strategy(self):
        self.real_db_path = 'C:/Users/24408/AppData/Local/Temp/wechat_decrypted_files/merge_all.db'
        self.real_db_base = DatabaseBase(db_path=self.real_db_path)
        sql = "select count(1) from MSG__Name2ID"
        result = self.real_db_base.execute_sql(sql)
        self.assertTrue(result[0][0] > 0)

    def test_merge_sorted_sequences(self):
        sequences = [MutableRow(self.create_row([1, 2])), MutableRow(self.create_row([1, 3]))]
        order_fields = [(0, "asc"), (1, "desc")]
        self.db_base.merge_sorted_sequences(sequences, order_fields)
        for sequence in sequences:
            print(sequence._data)
        # expected = [(1, 3), (1, 2)]
        # print("Result:", sequences)
        # print("Expected:", expected)
        # assert sequences == expected, "Test case failed!"

    def test_deal_pagination_with_limit_and_offset(self):
        sql_tree = parse_one("SELECT * FROM table LIMIT ? OFFSET ?")
        sql = "SELECT * FROM table LIMIT ? OFFSET ?"
        params = [10, 5]


        limit_config, new_sql_tree = self.db_base._DatabaseBase__deal_pagination(sql_tree, sql, params)
        assert len(params) == 0
        print(SqlglotUtils.sql(new_sql_tree))

    @patch('pywxdump.db.dbbase.SqlglotUtils')
    def test_deal_pagination_without_limit_and_offset(self, mock_sqlglot_utils):
        db = DatabaseBase({})
        sql_tree = parse_one("SELECT * FROM table")
        sql = "SELECT * FROM table"
        params = ()

        mock_sqlglot_utils.is_page.return_value = False

        limit_config, new_sql_tree = db._DatabaseBase__deal_pagination(sql_tree, sql, params)

        self.assertIsNone(limit_config)
        self.assertEqual(new_sql_tree, sql_tree)

    @patch('pywxdump.db.dbbase.SqlglotUtils')
    def test_deal_pagination_with_placeholder(self, mock_sqlglot_utils):
        db = DatabaseBase({})
        sql_tree = parse_one("SELECT * FROM table LIMIT ? OFFSET ?")
        sql = "SELECT * FROM table LIMIT ? OFFSET ?"
        params = (10, 5)

        mock_sqlglot_utils.is_page.return_value = True
        mock_sqlglot_utils.remove_page.return_value = sql_tree

        limit_config, new_sql_tree = db._DatabaseBase__deal_pagination(sql_tree, sql, params)

        self.assertEqual(limit_config['limit'], 10)
        self.assertEqual(limit_config['offset'], 5)
        self.assertEqual(new_sql_tree, sql_tree)

    @patch('pywxdump.db.dbbase.SqlglotUtils')
    def test_deal_pagination_with_aggregate(self, mock_sqlglot_utils):
        db = DatabaseBase({})
        sql_tree = parse_one("SELECT COUNT(*) FROM table LIMIT ? OFFSET ?")
        sql = "SELECT COUNT(*) FROM table LIMIT ? OFFSET ?"
        params = (10, 5)

        mock_sqlglot_utils.is_page.return_value = True
        mock_sqlglot_utils.is_aggregate.return_value = True
        mock_sqlglot_utils.remove_page.return_value = sql_tree

        limit_config, new_sql_tree = db._DatabaseBase__deal_pagination(sql_tree, sql, params)

        self.assertEqual(limit_config['limit'], 10)
        self.assertEqual(limit_config['offset'], 5)
        self.assertEqual(new_sql_tree, sql_tree)
    def create_row(self, data):
        mock_parent = MagicMock()
        mock_key_to_index = {f'column_{i}': i for i in range(len(data))}
        return Row(mock_parent, None, mock_key_to_index, data)
if __name__ == '__main__':
    unittest.main()

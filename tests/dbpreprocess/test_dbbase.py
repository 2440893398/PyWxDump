import unittest
from unittest.mock import patch, MagicMock

import sqlglot
from sqlglot.expressions import AggFunc

import unittest
from sqlglot import parse_one, expressions
from pywxdump.dbpreprocess.dbbase import DatabaseBase


class TestDatabaseBase(unittest.TestCase):
    @patch.object(DatabaseBase, '__init__', return_value=None)
    def setUp(self, mock_init):
        self.db_base = DatabaseBase(db_path='test_db_path')
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




if __name__ == '__main__':
    unittest.main()

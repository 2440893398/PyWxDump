import unittest

import sqlglot
from sqlglot import parse_one
from pywxdump.common.tools.sqlglot_utils import SqlglotUtils


class TestSqlglotUtils(unittest.TestCase):

    def test_get_group_fields_single_column(self):
        sql = "SELECT * FROM table GROUP BY column1"
        tree = parse_one(sql)
        self.assertEqual(SqlglotUtils.get_group_fields(tree), ["column1"])

    def test_get_group_fields_multiple_columns(self):
        sql = "SELECT * FROM table GROUP BY column1, column2"
        tree = parse_one(sql)
        self.assertEqual(SqlglotUtils.get_group_fields(tree), ["column1", "column2"])

    def test_get_group_fields_with_functions(self):
        sql = "SELECT * FROM table GROUP BY COUNT(column1)"
        tree = parse_one(sql)
        self.assertEqual(SqlglotUtils.get_group_fields(tree), ["COUNT(column1)"])

    def test_get_group_fields_empty(self):
        sql = "SELECT * FROM table"
        tree = parse_one(sql)
        self.assertEqual(SqlglotUtils.get_group_fields(tree), [])

    def test_single_order_by_asc(self):
        sql = "SELECT * FROM table_name ORDER BY field1 ASC"
        sql_tree = parse_one(sql)
        result = SqlglotUtils.multi_key_sort(sql_tree)
        expected = [('field1', 'asc')]
        self.assertEqual(result, expected)

    def test_single_order_by_desc(self):
        sql = "SELECT * FROM table_name ORDER BY field1 DESC"
        sql_tree = parse_one(sql)
        result = SqlglotUtils.multi_key_sort(sql_tree)
        expected = [('field1', 'desc')]
        self.assertEqual(result, expected)

    def test_multiple_order_by(self):
        sql = "SELECT * FROM table_name ORDER BY field1 ASC, field2 DESC"
        sql_tree = parse_one(sql)
        result = SqlglotUtils.multi_key_sort(sql_tree)
        expected = [('field1', 'asc'), ('field2', 'desc')]
        self.assertEqual(result, expected)

    def test_no_order_by(self):
        sql = "SELECT * FROM table_name"
        sql_tree = parse_one(sql)
        result = SqlglotUtils.multi_key_sort(sql_tree)
        expected = []
        self.assertEqual(result, expected)

    def test_mixed_order_by_with_functions(self):
        # Test with functions or expressions in the ORDER BY for comprehensiveness
        sql = "SELECT * FROM table_name ORDER BY field1 + 1 DESC, UPPER(field2) ASC"
        sql_tree = parse_one(sql)
        result = SqlglotUtils.multi_key_sort(sql_tree)
        # Depending on the SQL parsing library capabilities,
        # this might need to be adjusted for proper parsing of expressions
        expected = [("field1 + 1", 'desc'), ("UPPER(field2)", 'asc')]
        self.assertEqual(result, expected)

    def test_get_select_fields_single_column(self):
        sql = "SELECT column1 FROM table"
        tree = parse_one(sql)
        self.assertEqual(SqlglotUtils.get_select_fields_name(tree), ["column1"])

    def test_get_select_fields_multiple_columns(self):
        sql = "SELECT column1, column2 FROM table"
        tree = parse_one(sql)
        self.assertEqual(SqlglotUtils.get_select_fields_name(tree), ["column1", "column2"])

    def test_get_select_fields_with_alias(self):
        sql = "SELECT column1 AS col1 FROM table"
        tree = parse_one(sql)
        self.assertEqual(SqlglotUtils.get_select_fields_name(tree), ["col1"])

    def test_get_select_fields_with_functions(self):
        sql = "SELECT COUNT(column1) FROM table"
        tree = parse_one(sql)
        self.assertEqual(SqlglotUtils.get_select_fields_name(tree), ["COUNT(column1)"])

    def test_get_select_fields_empty(self):
        sql = "SELECT FROM table"
        tree = parse_one(sql)
        self.assertEqual(SqlglotUtils.get_select_fields_name(tree), [])

    def test_get_select_fields_ignore_alias(self):
        sql = "SELECT column1 AS col1 FROM table"
        tree = parse_one(sql)
        self.assertEqual(SqlglotUtils.get_select_fields_name(tree, is_ignore_alias=True), ["column1"])

    def test_is_page_returns_true_for_limit_clause(self):
        sql = "SELECT * FROM table LIMIT 10"
        tree = parse_one(sql)
        assert SqlglotUtils.is_page(tree) == True

    def test_is_page_returns_false_for_no_limit_clause(self):
        sql = "SELECT * FROM table"
        tree = parse_one(sql)
        assert SqlglotUtils.is_page(tree) == False

    def test_remove_page_removes_limit_clause(self):
        sql = "SELECT * FROM table LIMIT 10 OFFSET 5"
        tree = sqlglot.parse_one(sql)
        updated_tree = SqlglotUtils.remove_page(tree)
        assert "LIMIT" not in updated_tree.sql().upper()
        assert "OFFSET" not in updated_tree.sql().upper()

    def test_remove_page_does_nothing_if_no_limit_clause(self):
        sql = "SELECT * FROM table"
        tree = sqlglot.parse_one(sql)
        updated_tree = SqlglotUtils.remove_page(tree)
        assert updated_tree.sql() == tree.sql()

    def test_get_select_fields(self):
        sql = "SELECT column1, column2 FROM table"
        tree = parse_one(sql)
        self.assertEqual(SqlglotUtils.get_select_fields(tree), ["column1", "column2"])

    def test_deom(self):
        sql = """SELECT COUNT(1) FROM (select talker.UserName as talker,
                           talker.NickName            as nickName,
                           talker.Alias               as alias,
                           talker.Remark              as remark,
                           contactImage.bigHeadImgUrl as bigHeadImgUrl,
                           group_concat(fts.ROWID) as rowids,
                           count(fts.ROWID) as rowidCount
                    from FTSMSG0__FTSChatMsg2 as fts
                    INNER JOIN FTSMSG0__FTSChatMsg2_MetaData as metaData ON fts.rowid = metaData.docid
                    INNER JOIN FTSMSG0__NameToId as nameToId ON metaData.entityId = nameToId.ROWID
                    INNER JOIN Contact as talker ON nameToId.userName = talker.userName
                    LEFT JOIN ContactHeadImgUrl AS contactImage ON contactImage.usrName = talker.userName
                    where fts.content match simple_query(?)
                    group by talker.UserName
                    )"""
        tree = parse_one(sql, read="sqlite")
        print(tree.sql())

if __name__ == '__main__':
    unittest.main()

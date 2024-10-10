import unittest
from unittest.mock import patch, MagicMock

from pywxdump.common.fts.FTSType import FTSType
from pywxdump.dbpreprocess.dbbase import DatabaseBase
from pywxdump.db.fts.impl.parsingFTSContact import ParsingFTSContact
from pywxdump.db.fts.vo.aggregate_search_vo import AggregateSearchVo


class TestParsingFTSContact(unittest.TestCase):

    @patch.object(DatabaseBase, '__init__', return_value=None)
    def setUp(self, mock_init):
        self.parser = ParsingFTSContact('dummy_db_path')
        self.parser._db_path = 'dummy_db_path'
        pass

    @patch('pywxdump.dbpreprocess.fts.impl.parsingFTSContact.ParsingFTS.execute_sql_page')
    def test_search_returns_correct_results(self, mock_execute_sql_page):
        mock_result = MagicMock()
        mock_mappings = MagicMock()
        mock_mappings.all.return_value = [
            {'highlight_alias': 'highlight_0', 'highlight_nickname': 'highlight_1', 'highlight_remark': 'highlight_2',
             'UserName': 'UserName', 'bigHeadImgUrl': 'bigHeadImgUrl'}
        ]
        mock_result.mappings.return_value = mock_mappings
        mock_execute_sql_page.return_value = (1, mock_result)

        parser = self.parser
        result = parser.search('test_query', 1, 10)
        self.assertIsInstance(result, AggregateSearchVo)
        self.assertEqual(result.total_count, 1)
        self.assertEqual(result.page, 1)
        self.assertEqual(result.page_size, 10)
        self.assertEqual(result.itemTypes, FTSType.CONTACT.value)
        self.assertEqual(len(result.items), 1)

    @patch('pywxdump.dbpreprocess.fts.impl.parsingFTSContact.ParsingFTS.execute_sql_page')
    def test_search_handles_empty_results(self, mock_execute_sql_page):
        mock_result = MagicMock()
        mock_mappings = MagicMock()
        mock_mappings.all.return_value = []
        mock_result.mappings.return_value = mock_mappings
        mock_execute_sql_page.return_value = (0, mock_mappings)
        parser = self.parser
        result = parser.search('test_query', 1, 10)
        self.assertIsInstance(result, AggregateSearchVo)
        self.assertEqual(result.total_count, 0)
        self.assertEqual(result.page, 1)
        self.assertEqual(result.page_size, 10)
        self.assertEqual(result.itemTypes, FTSType.CONTACT.value)
        self.assertEqual(len(result.items), 0)

    @patch('pywxdump.dbpreprocess.fts.impl.parsingFTSContact.ParsingFTS.execute_sql_page')
    def test_search_handles_multiple_results(self, mock_execute_sql_page):
        mock_result = MagicMock()
        mock_mappings = MagicMock()
        mock_mappings.all.return_value = [
            {'highlight_alias': 'highlight_0', 'highlight_nickname': 'highlight_1', 'highlight_remark': 'highlight_2',
             'UserName': 'UserName1', 'bigHeadImgUrl': 'bigHeadImgUrl1'},
            {'highlight_alias': 'highlight_0', 'highlight_nickname': 'highlight_1', 'highlight_remark': 'highlight_2',
             'UserName': 'UserName2', 'bigHeadImgUrl': 'bigHeadImgUrl2'},
        ]
        mock_result.mappings.return_value = mock_mappings
        mock_execute_sql_page.return_value = (2, mock_result)
        parser = self.parser
        result = parser.search('test_query', 1, 10)
        self.assertIsInstance(result, AggregateSearchVo)
        self.assertEqual(result.total_count, 2)
        self.assertEqual(result.page, 1)
        self.assertEqual(result.page_size, 10)
        self.assertEqual(result.itemTypes, FTSType.CONTACT.value)
        self.assertEqual(len(result.items), 2)

    def test_dealSearchResultItem_returns_correct_list(self):
        parser = self.parser
        resultDic = [
            {
                'highlight_alias': 'alias1',
                'highlight_nickname': 'nickname1',
                'highlight_remark': 'remark1',
                'bigHeadImgUrl': 'url1'
            },
            {
                'highlight_alias': 'alias2',
                'highlight_nickname': 'nickname2',
                'highlight_remark': 'remark2',
                'bigHeadImgUrl': 'url2'
            }
        ]
        result = parser.dealSearchResultItem(resultDic)
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0].thumbnail, 'url1')
        self.assertEqual(result[0].title, 'nickname1')
        self.assertEqual(result[0].note, 'alias1 nickname1 remark1')
        self.assertEqual(result[1].thumbnail, 'url2')
        self.assertEqual(result[1].title, 'nickname2')
        self.assertEqual(result[1].note, 'alias2 nickname2 remark2')

    def test_dealSearchResultItem_handles_empty_list(self):
        parser = self.parser
        resultDic = []
        result = parser.dealSearchResultItem(resultDic)
        self.assertEqual(result, [])

    def test_dealSearchResultItem_handles_missing_keys(self):
        parser = self.parser
        resultDic = [
            {
                'highlight_alias': 'alias1',
                'highlight_nickname': 'nickname1',
                'bigHeadImgUrl': 'url1'
            }
        ]
        result = parser.dealSearchResultItem(resultDic)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].thumbnail, 'url1')
        self.assertEqual(result[0].title, 'nickname1')
        self.assertEqual(result[0].note, 'alias1 nickname1 ')

    def test_search_returns_correct_aggregate_search_vo(self):
        db_path = "C:/Users/24408/AppData/Local/Temp/wechat_decrypted_files/merge_all.db"
        parser = ParsingFTSContact(db_path)
        query = "部"
        result = parser.search(query, page=1, pagesize=10)
        print(result)

if __name__ == '__main__':
    unittest.main()

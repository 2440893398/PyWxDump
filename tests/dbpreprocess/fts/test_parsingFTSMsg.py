import unittest
from unittest.mock import patch

from pywxdump.dbpreprocess.dbbase import DatabaseBase
from pywxdump.db.fts.impl.parsingFTSMsg import ParsingFTSMsg
from pywxdump.db.fts.vo.aggregate_search_vo import SearchResultItem


class TestParsingFTSMsg(unittest.TestCase):
    @patch.object(DatabaseBase, '__init__', return_value=None)
    def setUp(self, mock_init):
        self.parser = ParsingFTSMsg(db_path='test_db_path')
        self.parser._db_path = 'dummy_db_path'

    def test_dealSearchResultItem_empty_resultDic(self):
        result = self.parser.dealSearchResultItem([], 'test_query')
        self.assertEqual(result, [])

    def test_dealSearchResultItem_single_rowid(self):
        resultDic = [{
            'nickName': 'John Doe',
            'alias': 'jdoe',
            'rowidCount': 1,
            'rowids': '1',
            'bigHeadImgUrl': 'http://example.com/image.jpg'
        }]
        self.parser.execute = lambda sql, params: [
            {'rowid': '1', 'highlight_content': 'highlighted content'}]
        result = self.parser.dealSearchResultItem(resultDic,'test_query')
        expected = [SearchResultItem(thumbnail='http://example.com/image.jpg', title='John Doe(jdoe)',
                                     note='highlighted content')]
        self.assertEqual(expected, result)

    def test_dealSearchResultItem_multiple_rowids(self):
        resultDic = [{
            'nickName': 'John Doe',
            'alias': 'jdoe',
            'rowidCount': 2,
            'rowids': '1,2',
            'bigHeadImgUrl': 'http://example.com/image.jpg'
        }]
        result = self.parser.dealSearchResultItem(resultDic,'test_query')
        expected = [
            SearchResultItem(thumbnail='http://example.com/image.jpg', title='John Doe(jdoe)', note='2条相关聊天记录')]
        self.assertEqual(result, expected)

    def test_dealSearchResultItem_no_alias(self):
        resultDic = [{
            'nickName': 'John Doe',
            'alias': '',
            'rowidCount': 1,
            'rowids': '1',
            'bigHeadImgUrl': 'http://example.com/image.jpg',
            'highlight_content': 'highlighted content'
        }]
        self.parser.execute = lambda sql, params: [
            {'rowid': '1', 'highlight_content': 'highlighted content'}]
        self.parser.to_dic = lambda x: x
        result = self.parser.dealSearchResultItem(resultDic,'test_query')
        expected = [
            SearchResultItem(thumbnail='http://example.com/image.jpg', title='John Doe', note='highlighted content')]
        self.assertEqual(result, expected)

    # def test_search_returns_correct_aggregate_search_vo(self):
    #     db_path = "C:/Users/24408/AppData/Local/Temp/wechat_decrypted_files/merge_all.db"
    #     parser = ParsingFTSMsg(db_path)
    #     query = "测试"
    #     result = parser.search(query, page=1, pagesize=10)
    #     print(result)
if __name__ == '__main__':
    unittest.main()

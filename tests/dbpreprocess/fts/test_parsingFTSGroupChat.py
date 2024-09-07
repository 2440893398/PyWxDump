import unittest
from unittest.mock import patch, MagicMock

from pywxdump.common.fts.FTSType import FTSType
from pywxdump.dbpreprocess.dbbase import DatabaseBase
from pywxdump.api.search.vo.aggregate_search_vo import AggregateSearchVo
from pywxdump.dbpreprocess.fts.impl.parsingFTSGroupChat import ParsingFTSGroupChat


class TestParsingFTSContact(unittest.TestCase):

    @patch.object(DatabaseBase, '__init__', return_value=None)
    def setUp(self, mock_init):
        self.parser = ParsingFTSGroupChat('dummy_db_path')
        self.parser._db_path = 'dummy_db_path'
        pass



    @patch('pywxdump.dbpreprocess.fts.impl.parsingFTSGroupChat.ParsingFTS.execute')
    def test_dealSearchResultItem_returns_correct_results(self, mock_execute_sql):
        mock_execute_sql.return_value = [
            {'talker': 'UserName1', 'highlight_groupRemark': 'remark1', 'highlight_nickname': 'nickname1',
             'highlight_alias': 'alias1'},
            {'talker': 'UserName2', 'highlight_groupRemark': 'remark2', 'highlight_nickname': 'nickname2',
             'highlight_alias': 'alias2'}
        ]
        resultDic = [
            {'groupTalker': 'group1', 'groupTalkerNickName': 'Group 1', 'groupTalkerRemarkName': 'Remark 1',
             'groupTalkerAlias': 'Alias 1', 'talker': 'UserName1,UserName2'}
        ]
        parser = ParsingFTSGroupChat('dummy_path')
        parser.to_dic = lambda x: x
        result = parser.dealSearchResultItem(resultDic, 'test_query')
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].title, 'Group 1(Alias 1)')
        self.assertEqual(result[0].note, 'alias1 nickname1 remark1,alias2 nickname2 remark2,')

    @patch('pywxdump.dbpreprocess.fts.impl.parsingFTSGroupChat.ParsingFTS.execute')
    def test_dealSearchResultItem_returns_correct_results(self, mock_execute_sql):
        mock_execute_sql.return_value = [
            {'talker': 'UserName1',
             'highlight_groupRemark': 'remark1',
             'highlight_nickname': 'nickname1',
             'highlight_alias': 'alias1'},
            {'talker': 'UserName2',
             'highlight_groupRemark': 'remark2',
             'highlight_nickname': 'nickname2',
             'highlight_alias': 'alias2'}
        ]
        resultDic = [
            {'groupTalker': 'group1',
             'groupTalkerNickName': 'Group 1',
             'groupTalkerRemarkName': 'Remark 1',
             'groupTalkerAlias': 'Alias 1',
             'talker': 'UserName1,UserName2'}
        ]
        parser = self.parser
        self.parser.to_dic = lambda x: x
        result = parser.dealSearchResultItem(resultDic, 'test_query')
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].title, 'Group 1(Alias 1)')
        self.assertEqual(result[0].note, 'alias1 nickname1 remark1,alias2 nickname2 remark2,')

    @patch('pywxdump.dbpreprocess.fts.impl.parsingFTSGroupChat.ParsingFTS.execute')
    def test_dealSearchResultItem_handles_empty_resultDic(self, mock_execute_sql):
        mock_execute_sql.return_value = []
        resultDic = []
        parser = self.parser
        self.parser.to_dic = lambda x: x
        result = parser.dealSearchResultItem(resultDic, 'test_query')
        self.assertEqual(result, [])

    @patch('pywxdump.dbpreprocess.fts.impl.parsingFTSGroupChat.ParsingFTS.execute')
    def test_dealSearchResultItem_handles_missing_talker(self, mock_execute_sql):
        mock_execute_sql.return_value = []
        resultDic = [
            {'groupTalker': 'group1',
             'groupTalkerNickName': 'Group 1',
             'groupTalkerRemarkName': 'Remark 1',
             'groupTalkerAlias': 'Alias 1',
             'talker': ''}
        ]
        parser = self.parser
        self.parser.to_dic = lambda x: x
        result = parser.dealSearchResultItem(resultDic, 'test_query')
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].title, 'Group 1(Alias 1)')
        self.assertEqual(result[0].note, '')

    @patch('pywxdump.dbpreprocess.fts.impl.parsingFTSGroupChat.ParsingFTS.execute')
    def test_dealSearchResultItem_handles_missing_groupTalkerAlias(self, mock_execute_sql):
        mock_execute_sql.return_value = [
            {'talker': 'UserName1',
             'highlight_groupRemark': 'remark1',
             'highlight_nickname': 'nickname1',
             'highlight_alias': 'alias1'}
        ]
        resultDic = [
            {'groupTalker': 'group1',
             'groupTalkerNickName': 'Group 1',
             'groupTalkerRemarkName': 'Remark 1',
             'talker': 'UserName1'}
        ]
        parser = self.parser
        self.parser.to_dic = lambda x: x
        result = parser.dealSearchResultItem(resultDic, 'test_query')
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].title, 'Group 1')
        self.assertEqual(result[0].note, 'alias1 nickname1 remark1,')

    # def test_search_returns_correct_aggregate_search_vo(self):
    #     db_path = "C:/Users/24408/AppData/Local/Temp/wechat_decrypted_files/merge_all.db"
    #     parser = ParsingFTSGroupChat(db_path)
    #     query = "部"
    #     result = parser.search(query, page=1, pagesize=10)
    #     print(result)
if __name__ == '__main__':
    unittest.main()

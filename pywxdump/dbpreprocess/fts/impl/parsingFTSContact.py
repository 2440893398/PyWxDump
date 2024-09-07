from pywxdump.api.search.vo.aggregate_search_vo import AggregateSearchVo, SearchResultItem
from pywxdump.common.fts.FTSType import FTSType

from pywxdump.dbpreprocess.fts.parsingFTS import ParsingFTS
from pywxdump.dbpreprocess.fts.parsingFTSFactroy import ParsingFTSFactory


@ParsingFTSFactory.register(FTSType.CONTACT)
class ParsingFTSContact(ParsingFTS):
    HIGHLIGHT_ALIAS = "highlight_alias"
    HIGHLIGHT_NICKNAME = "highlight_nickname"
    HIGHLIGHT_REMARK = "highlight_remark"
    USER_NAME = "UserName"
    BIG_HEAD_IMG_URL = "bigHeadImgUrl"

    __FTS_CONTACT_QUERY_SQL = '''SELECT
                                        simple_highlight(FTSContact15, 0, '[', ']') AS highlight_alias,
                                        simple_highlight(FTSContact15, 1, '[', ']') AS highlight_nickname,
                                        simple_highlight(FTSContact15, 2, '[', ']') AS highlight_remark,
                                        tmp.UserName as UserName,
                                        contactImage.bigHeadImgUrl as bigHeadImgUrl
                                    FROM FTSContact15 AS fts
                                    INNER JOIN FTSContact15_MetaData AS ftsMetaData ON fts.rowid = ftsMetaData.docid
                                    INNER JOIN FTSContact__NameToId AS tmp ON ftsMetaData.entityId = tmp.ROWID
                                    LEFT JOIN ContactHeadImgUrl AS contactImage ON contactImage.usrName = tmp.userName
                                    WHERE FTSContact15 MATCH simple_query(?);'''

    def search(self, query: str, page=1, pagesize=10) -> AggregateSearchVo:
        """
        搜索联系人

        :param query: 搜索关键字
        :param page: 页码
        :param pagesize: 每页数量

        :return: 搜索结果
        """
        total, result = self.execute_sql_page(self.__FTS_CONTACT_QUERY_SQL, (query,), page, pagesize)
        resultDic = self.to_dic(result)
        return AggregateSearchVo(total_count=total, page=page, page_size=pagesize,
                                 itemTypes=FTSType.CONTACT, items=self.dealSearchResultItem(resultDic))

    def dealSearchResultItem(self, resultDic) -> list:
        """
        处理搜索结果

        :param resultDic: 搜索结果

        :return: 处理后的搜索结果
        """
        result = []
        # 如果没有搜索结果，直接返回
        if not resultDic:
            return result
        # 处理搜索结果
        for item in resultDic:
            note = f"{item.get(self.HIGHLIGHT_ALIAS, '')} {item.get(self.HIGHLIGHT_NICKNAME, '')} {item.get(self.HIGHLIGHT_REMARK, '')}"
            result.append(SearchResultItem(thumbnail=item.get(self.BIG_HEAD_IMG_URL, ''),
                                           title=item.get(self.HIGHLIGHT_NICKNAME, ''),
                                           note=note))
        return result

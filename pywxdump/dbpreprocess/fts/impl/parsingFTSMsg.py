from pywxdump.api.search.vo.aggregate_search_vo import AggregateSearchVo, SearchResultItem
from pywxdump.common.fts.FTSType import FTSType

from pywxdump.dbpreprocess.fts.parsingFTS import ParsingFTS
from pywxdump.dbpreprocess.fts.parsingFTSFactroy import ParsingFTSFactory


@ParsingFTSFactory.register(FTSType.CHAT_RECORD)
class ParsingFTSMsg(ParsingFTS):
    TALKER = "talker"
    NICK_NAME = "nickName"
    ALIAS = "alias"
    REMARK = "remark"
    ROW_IDS = "rowids"
    ROWID_COUNT = "rowidCount"
    ROW_ID = "rowid"
    HIGH_LIGHT_CONTENT = "highlight_content"
    BIG_HEAD_IMG_URL = "bigHeadImgUrl"

    __FTS_MSG_QUERY_SQL = '''
                    select talker.UserName as talker,
                           talker.NickName            as nickName,
                           talker.Alias               as alias,
                           talker.Remark              as remark,
                           contactImage.bigHeadImgUrl as bigHeadImgUrl,
                           group_concat(fts.ROWID) as rowids,
                           count(fts.ROWID) as rowidCount
                    from FTSMSG__FTSChatMsg2 as fts
                    INNER JOIN FTSMSG__FTSChatMsg2_MetaData as metaData ON fts.rowid = metaData.docid
                    INNER JOIN FTSMSG__NameToId as nameToId ON metaData.entityId = nameToId.ROWID
                    INNER JOIN Contact as talker ON nameToId.userName = talker.userName
                    LEFT JOIN ContactHeadImgUrl AS contactImage ON contactImage.usrName = talker.userName
                    where fts.content match simple_query(?)
                    group by talker.UserName
    '''

    __FTS_MSG_DETIAL_SQL = '''
                    select fts.rowid as rowid,
                            simple_highlight(FTSMSG__FTSChatMsg2, 0, '[', ']') as highlight_content
                    from FTSMSG__FTSChatMsg2 as fts
                        INNER JOIN FTSMSG__FTSChatMsg2_MetaData as metaData ON fts.rowid = metaData.docid
                    where fts.content match simple_query(?)
                    AND fts.ROWID in (?)
    '''

    def search(self, query: str, page=1, pagesize=10) -> AggregateSearchVo:
        """
        搜索联系人

        :param query: 搜索关键字
        :param page: 页码
        :param pagesize: 每页数量

        :return: 搜索结果
        """
        total, result = self.execute_sql_page(self.__FTS_MSG_QUERY_SQL, (query,), page, pagesize)
        resultDic = self.to_dic(result)
        return AggregateSearchVo(total_count=total, page=page, page_size=pagesize,
                                 itemTypes=FTSType.GROUP_CHAT, items=self.dealSearchResultItem(resultDic, query))

    def dealSearchResultItem(self, resultDic, query) -> list:
        """
        处理搜索结果

        :param resultDic: 搜索结果
        :param query: 搜索关键字

        :return: 处理后的搜索结果
        """
        result = []
        # 如果没有搜索结果，直接返回
        if not resultDic:
            return result

        # 获取所有rowidCount是1的rowid
        rowIds = []
        for item in resultDic:
            if item.get(self.ROWID_COUNT) == 1:
                rowIds.append(item.get(self.ROW_IDS).split(',')[0])
        # 查询rowid对应的内容
        rowid2content = {}
        if rowIds:
            tmp = self.execute(self.__FTS_MSG_DETIAL_SQL, (query, rowIds,))
            tmp = self.to_dic(tmp)
            for item in tmp:
                rowid2content[item.get(self.ROW_ID)] = item.get(self.HIGH_LIGHT_CONTENT)
        # 处理搜索结果
        for item in resultDic:
            # 处理 title
            nickname = item.get(self.NICK_NAME, '')
            alias = item.get(self.ALIAS, '')
            title = f"{nickname}({alias})" if alias else nickname
            # 处理 note
            rowCount = item.get(self.ROWID_COUNT)
            if rowCount == 1:
                content = rowid2content.get(item.get(self.ROW_IDS))
                note = f"{content}"
            else:
                note = f"{rowCount}条相关聊天记录"
            result.append(SearchResultItem(thumbnail=item.get(self.BIG_HEAD_IMG_URL, ''),
                                           title=title,
                                           note=note))
        return result
        pass

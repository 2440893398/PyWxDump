from pywxdump.db.dto.dbMsgDetail import MsgDetail
from pywxdump.db.fts.dto.fts_msg_list_dto import FtsMsgListDto
from pywxdump.db.fts.dto.msg_by_query_dto import MsgByQuery
from pywxdump.db.fts.vo.aggregate_search_vo import AggregateSearchVo, SearchResultItem
from pywxdump.db.fts.parsingFTS import ParsingFTS, FTSType, HIGHLIGHT_LABEL_LEFT, HIGHLIGHT_LABEL_RIGHT
from pywxdump.db.fts.parsingFTSFactroy import ParsingFTSFactory
from pywxdump.db.fts.vo.fts_msg_list_vo import FtsMsgListVo
from pywxdump.db.fts.vo.msg_by_query_vo import MsgByQueryVo, MsgByQueryItemVo
from pywxdump.db.utils import type_converter, msg_utils


def lenIgnorLabel(content):
    """
    计算字符串长度，忽略标签

    :param content: 字符串

    :return: 字符串长度
    """
    return len(content.replace(HIGHLIGHT_LABEL_LEFT, '').replace(HIGHLIGHT_LABEL_RIGHT, ''))


@ParsingFTSFactory.register(FTSType.CHAT_RECORD)
class ParsingFTSMsg(ParsingFTS):
    TALKER = "talker"
    SORT_SEQUENCE = "sortSequence"
    CONTENT = "content"
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

    __FTS_MSG_QUERY_SQL_2 = '''
                    select metaData.sortSequence as sortSequence,
                            group_concat(fts.ROWID) as rowids,
                           count(fts.ROWID) as rowidCount
                    from FTSMSG__FTSChatMsg2_MetaData as metaData
                             INNER JOIN FTSMSG__NameToId as nameToId
                                        ON metaData.entityId = nameToId.ROWID and nameToId.userName = ?
                             INNER JOIN FTSMSG__FTSChatMsg2 as fts
                                        ON fts.rowid = metaData.docid
                    where 1==1
    '''

    __FTS_MSG_DETIAL_SQL = f'''
                    select fts.rowid as rowid,
                            metaData.sortSequence as sortSequence,
                            fts.content as content,
                            simple_highlight(FTSMSG__FTSChatMsg2, 0, '{HIGHLIGHT_LABEL_LEFT}', '{HIGHLIGHT_LABEL_RIGHT}') as highlight_content
                    from FTSMSG__FTSChatMsg2 as fts
                        INNER JOIN FTSMSG__FTSChatMsg2_MetaData as metaData ON fts.rowid = metaData.docid
                    where fts.content match simple_query(?)
                    AND fts.ROWID in (?)
    '''

    __MSG_DETAIL_SQL = '''
        SELECT localId,TalkerId,MsgSvrID,Type,SubType,CreateTime,IsSender,Sequence,StatusEx,FlagEx,Status,
            MsgSequence,StrContent,MsgServerSeq,StrTalker,DisplayContent,Reserved0,Reserved1,Reserved3,
            Reserved4,Reserved5,Reserved6,CompressContent,BytesExtra,BytesTrans,Reserved2,
            ROW_NUMBER() OVER (ORDER BY CreateTime ASC) AS id 
            FROM MSG__MSG WHERE Sequence = ? 
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
                                 itemTypes=FTSType.CHAT_RECORD, items=self.dealSearchResultItem(resultDic, query))

    def multiDimensionalSearch(self, query: MsgByQuery) -> MsgByQueryVo:
        """
        多维度聊天记录搜索
        """
        sql, params = self.__generate_fts_msg_query_sql(query)
        params.insert(0, query.chatRomeId)
        total, result = self.execute_sql_page(sql, tuple(params), query.page, query.page_size)
        resultDic = self.to_dic(result)
        return MsgByQueryVo(total_count=total, page=query.page, page_size=query.page_size,
                            items=self.dealMsgByQueryItemVo(resultDic, query.query))

    def getFtsMsgList(self,dto : FtsMsgListDto ) -> FtsMsgListVo:
        """
        获取聊天记录列表
        :param dto: 搜索参数
        :return: 搜索结果
        """
    pass

    def __generate_fts_msg_query_sql(self, query):
        base_sql = self.__FTS_MSG_QUERY_SQL_2
        where_clauses = []
        params = []
        # 查询聊天记录类型
        typeTuple = []
        typeList = query.query_type.value
        for typeName in typeList:
            typeTuple.append(type_converter(typeName))
        for condition in typeTuple:
            where_clauses.append(f"( metaData.type = ? AND metaData.subType = ? )")
            params.extend(condition)
        if where_clauses:
            where_sql = " OR ".join(where_clauses)
            base_sql += f" AND ({where_sql})"

        if query.query:
            base_sql += " AND fts.content match simple_query(?)"
            params.append(query.query)

        base_sql += " GROUP BY metaData.sortSequence"

        return base_sql, params
        pass

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
            tmp = self.execute_sql(self.__FTS_MSG_DETIAL_SQL, (query, rowIds,))
            tmp = self.to_dic(tmp)
            for item in tmp:
                rowid2content[str(item.get(self.ROW_ID))] = item.get(self.HIGH_LIGHT_CONTENT)
        # 处理搜索结果
        for item in resultDic:
            # 处理 title
            nickname = item.get(self.NICK_NAME, '')
            alias = item.get(self.ALIAS, '')
            title = f"{nickname}({alias})" if alias else nickname
            # 处理 note
            rowCount = item.get(self.ROWID_COUNT)
            if rowCount == 1:
                content = rowid2content.get(item.get(self.ROW_IDS), '')
                note = f"..." if lenIgnorLabel(content) > 20 else content
            else:
                note = f"{rowCount}条相关聊天记录"
            result.append(SearchResultItem(thumbnail=item.get(self.BIG_HEAD_IMG_URL, ''),
                                           title=title,
                                           note=note,
                                           bizId=item.get(self.TALKER, '')
                                           ))
        return result
        pass

    def dealMsgByQueryItemVo(self, resultDic, query):
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
        msgIds = []
        for item in resultDic:
            msgIds.append(item.get(self.SORT_SEQUENCE))
            if item.get(self.ROWID_COUNT) > 0:
                rowIds.extend(item.get(self.ROW_IDS).split(','))
        # 查询rowid+content对应的内容
        rowid2content = {}
        if rowIds and query:
            tmp = self.execute_sql(self.__FTS_MSG_DETIAL_SQL, (query, rowIds,))
            tmp = self.to_dic(tmp)
            for item in tmp:
                rowid2content[str(item.get(self.SORT_SEQUENCE)) + item.get(self.CONTENT)] = item.get(
                    self.HIGH_LIGHT_CONTENT)

        # 查询消息详情
        msgDetails = []
        if msgIds:
            tmp = self.execute_sql(self.__MSG_DETAIL_SQL, (msgIds,))
            allRow = tmp.fetchall()
            for row in allRow:
                msgDetails.append(MsgDetail(**msg_utils.get_msg_detail(row)))
        # 组织返回
        for msgDetail in msgDetails:
            content_key = str(msgDetail.Sequence) + (msgDetail.msg if msgDetail.msg else '')
            result.append(MsgByQueryItemVo(id=msgDetail.id,
                                           talker=msgDetail.talker,
                                           content=rowid2content.get(content_key, msgDetail.msg),
                                           thumbnail=msgDetail.src,
                                           send_time=msgDetail.CreateTime,
                                           file_size=''))
        return result

from pywxdump.db.fts.vo.aggregate_search_vo import AggregateSearchVo
from pywxdump.db.fts.vo.search_result_item import SearchResultItem
from pywxdump.db.fts.parsingFTS import ParsingFTS, FTSType, HIGHLIGHT_LABEL_LEFT, HIGHLIGHT_LABEL_RIGHT
from pywxdump.db.fts.parsingFTSFactroy import ParsingFTSFactory


@ParsingFTSFactory.register(FTSType.GROUP_CHAT)
class ParsingFTSGroupChat(ParsingFTS):
    GROUP_TALKER = "groupTalker"
    TALKER = "talker"
    GROUP_TALKER_NICKNAME = "groupTalkerNickName"
    GROUP_TALKER_REMARK = "groupTalkerRemarkName"
    GROUP_TALKER_ALIAS = "groupTalkerAlias"
    HIGH_LIGHT_GROUP_REMARK = "highlight_groupRemark"
    HIGH_LIGHT_NICKNAME = "highlight_nickname"
    HIGH_LIGHT_ALIAS = "highlight_alias"
    __FTS_GROUP_CHAT_QUERY_SQL = F'''select chatRoom.ChatRoomName as groupTalker
                                                 , ifnull(ftsChatRoom.highlight_alias, contact.Alias) as groupTalkerAlias
                                                 , COALESCE(ftsChatRoom.highlight_nickname, CASE WHEN contact.NickName='' THEN session.strNickName ELSE contact.NickName END) as groupTalkerNickName
                                                 , ifnull(ftsChatRoom.highlight_remark, contact.Remark) as groupTalkerRemarkName
                                                 , ifnull(ftsChatRoom.type,ftsChatRoomUser.type) as flag
                                                 , ftsChatRoomUser.talker as talker
                                            from ChatRoom as chatRoom
                                                inner join main.MicroMsg__Session as session on chatRoom.ChatRoomName = session.strUsrName
                                                inner join Contact as contact on chatRoom.ChatRoomName = contact.UserName
                                                     left join
                                                 (SELECT
                                                        'ChatRoom' as type,
                                                         simple_highlight(FTSContact15, 0,  '{HIGHLIGHT_LABEL_LEFT}', '{HIGHLIGHT_LABEL_RIGHT}') AS highlight_alias,
                                                         simple_highlight(FTSContact15, 1,  '{HIGHLIGHT_LABEL_LEFT}', '{HIGHLIGHT_LABEL_RIGHT}') AS highlight_nickname,
                                                         simple_highlight(FTSContact15, 2,  '{HIGHLIGHT_LABEL_LEFT}', '{HIGHLIGHT_LABEL_RIGHT}') AS highlight_remark,
                                                         tmp.UserName                                as UserName
                                                  FROM FTSContact15 AS fts
                                                           INNER JOIN FTSContact15_MetaData AS ftsMetaData ON fts.rowid = ftsMetaData.docid
                                                           INNER JOIN FTSContact__NameToId AS tmp ON ftsMetaData.entityId = tmp.ROWID
                                                  WHERE FTSContact15 MATCH simple_query(?)) as ftsChatRoom on ChatRoom.ChatRoomName = ftsChatRoom.UserName
                                                     left join
                                                 (select
                                                        'ChatRoom' as type,
                                                        groupTalker.UserName          as groupTalker,
                                                         group_concat(talker.userName) as talker
                                                  from FTSChatroom15_MetaData as c1
                                                           inner join FTSChatroom15 as fts on FTSChatroom15 match simple_query(?) and c1.docid = fts.rowid
                                                           inner join FTSContact__NameToId as talker on c1.talkerId = talker.ROWID
                                                           inner join FTSContact__NameToId as groupTalker on c1.groupTalkerId = groupTalker.ROWID
                                                  group by groupTalker.userName) as ftsChatRoomUser on ChatRoom.ChatRoomName = ftsChatRoomUser.groupTalker
                                            where flag IS NOT NULL

                                    '''

    __FTS_GROUP_CHAT_DETAIL_QUERY_SQL = F'''select
                                            c3.userName as talker,
                                            simple_highlight(FTSChatroom15, 0,  '{HIGHLIGHT_LABEL_LEFT}', '{HIGHLIGHT_LABEL_RIGHT}') as highlight_groupRemark,
                                            simple_highlight(FTSChatroom15, 1,  '{HIGHLIGHT_LABEL_LEFT}', '{HIGHLIGHT_LABEL_RIGHT}') as highlight_nickname,
                                            simple_highlight(FTSChatroom15, 2,  '{HIGHLIGHT_LABEL_LEFT}', '{HIGHLIGHT_LABEL_RIGHT}') as highlight_alias
                                            from FTSChatroom15_MetaData as c1
                                            inner join FTSChatroom15 as fts on FTSChatroom15 match simple_query(?) and c1.docid = fts.rowid
                                            inner join FTSContact__NameToId as talker on c1.talkerId = talker.ROWID
                                            inner join Contact as c3 on talker.userName = c3.userName
                                            inner join FTSContact__NameToId as groupTalker on c1.groupTalkerId = groupTalker.ROWID
                                            where groupTalker.userName in (?)'''


    def search(self, query: str, page=1, pagesize=10) -> AggregateSearchVo:
        """
        搜索群聊

        :param query: 关键词
        :param page: 当前页
        :param pagesize: 每页数量

        :return: 查询结果
        """

        total, result = self.execute_sql_page(self.__FTS_GROUP_CHAT_QUERY_SQL, (query,query,), page, pagesize)
        resultDic = self.to_dic(result)
        return AggregateSearchVo(total_count=total, page=page, page_size=pagesize,
                                 itemTypes=FTSType.GROUP_CHAT, items=self.dealSearchResultItem(resultDic, query))
        pass

    def dealSearchResultItem(self, resultDic, query: str):
        if not resultDic:
            return []
        # 获取所有的群聊
        groupTalkerIds = set()
        for item in resultDic:
            groupTalkerIds.add(item.get(self.GROUP_TALKER))
        # 查询群聊详情
        result = self.execute_sql(self.__FTS_GROUP_CHAT_DETAIL_QUERY_SQL, (query, groupTalkerIds,))
        result = self.to_dic(result)
        # 以talker为key的map
        talkerMap = {}
        for item in result:
            talkerMap[item.get(self.TALKER)] = item
        # 处理搜索结果
        result = []
        for item in resultDic:
            talker = item.get(self.TALKER)

            # 拼接note
            note = ""
            if talker:
                # talker是多个需要拆分
                talkerList = talker.split(',')
                for talker in talkerList:
                    talkerItem = talkerMap.get(talker)
                    if talkerItem:
                        note += f"{talkerItem.get(self.HIGH_LIGHT_ALIAS, '')} {talkerItem.get(self.HIGH_LIGHT_NICKNAME, '')} {talkerItem.get(self.HIGH_LIGHT_GROUP_REMARK, '')},"

            title = item.get(self.GROUP_TALKER_NICKNAME)
            if item.get(self.GROUP_TALKER_ALIAS):
                title += f'({item.get(self.GROUP_TALKER_ALIAS)})'
            result.append(SearchResultItem(thumbnail="群聊", title=title,
                                           note=note,bizId=item.get(self.GROUP_TALKER, '')))
        return result

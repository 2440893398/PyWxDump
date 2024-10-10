# -*- coding: utf-8 -*-#
# -------------------------------------------------------------------------------
# Name:         MSG.py
# Description:  负责处理消息数据库数据
# Author:       xaoyaoo
# Date:         2024/04/15
# -------------------------------------------------------------------------------
import json
import os
import re
import lz4.block
import blackboxprotobuf

from .dbbase import DatabaseBase
from .utils import db_error, timestamp2str, xml2dict, match_BytesExtra, type_converter, msg_utils


class MsgHandler(DatabaseBase):
    _class_name = "MSG"
    MSG_required_tables = ["MSG__MSG"]

    def Msg_add_index(self):
        """
        添加索引,加快查询速度
        """
        # 检查是否存在索引
        if not self.tables_exist("MSG*__MSG"):
            return
        self.execute("CREATE INDEX IF NOT EXISTS idx_MSG_StrTalker ON MSG__MSG(StrTalker);")
        self.execute("CREATE INDEX IF NOT EXISTS idx_MSG_CreateTime ON MSG__MSG(CreateTime);")
        self.execute("CREATE INDEX IF NOT EXISTS idx_MSG_StrTalker_CreateTime ON MSG__MSG(StrTalker, CreateTime);")

    @db_error
    def get_m_msg_count(self, wxids: list = ""):
        """
        获取聊天记录数量,根据wxid获取单个联系人的聊天记录数量，不传wxid则获取所有联系人的聊天记录数量
        :param wxids: wxid list
        :return: 聊天记录数量列表 {wxid: chat_count, total: total_count}
        """
        if isinstance(wxids, str) and wxids:
            wxids = [wxids]
        if wxids:
            wxids = "('" + "','".join(wxids) + "')"
            sql = f"SELECT StrTalker, COUNT(*) FROM MSG__MSG WHERE StrTalker IN {wxids} GROUP BY StrTalker ;"
        else:
            sql = f"SELECT StrTalker, COUNT(*) FROM MSG__MSG GROUP BY StrTalker ;"
        sql_total = f"SELECT COUNT(*) FROM MSG__MSG;"

        if not self.tables_exist("MSG*__MSG"):
            return {}
        result = self.execute(sql)
        total_ret = self.execute(sql_total)

        if not result:
            return {}
        total = 0
        if total_ret and len(total_ret) > 0:
            total = total_ret[0][0]

        msg_count = {"total": total}
        msg_count.update({row[0]: row[1] for row in result})
        return msg_count

    @db_error
    def get_msg_list(self, wxids: list or str = "", start_index=0,direction = "down", page_size=500, msg_type: str = "",
                     msg_sub_type: str = "", start_createtime=None, end_createtime=None, my_talker="我"):
        """
        获取聊天记录列表
        :param wxids: [wxid]
        :param start_index: 起始索引
        :param direction: 查询方向 down or up
        :param page_size: 页大小
        :param msg_type: 消息类型
        :param msg_sub_type: 消息子类型
        :param start_createtime: 开始时间
        :param end_createtime: 结束时间
        :param my_talker: 我
        :return: 聊天记录列表 {"id": _id, "MsgSvrID": str(MsgSvrID), "type_name": type_name, "is_sender": IsSender,
                    "talker": talker, "room_name": StrTalker, "msg": msg, "src": src, "extra": {},
                    "CreateTime": CreateTime, }
        """
        if not self.tables_exist("MSG*__MSG"):
            return [], []

        if isinstance(wxids, str) and wxids:
            wxids = [wxids]
        param = ()
        sql_wxid, param = (f"AND StrTalker in ({', '.join('?' for _ in wxids)}) ",
                           param + tuple(wxids)) if wxids else ("", param)
        if direction.find("down") != -1:
            if direction.startswith("include"):
                sql_MsgSequence, param = ("AND Sequence >= ? ", param + (start_index,)) if start_index else (
                "", param)
            else:
                sql_MsgSequence, param = ("AND Sequence > ? ", param + (start_index,)) if start_index else ("", param)
        else:
            sql_MsgSequence, param = ("AND Sequence < ? ", param + (start_index,)) if start_index else ("", param)

        sql_type, param = ("AND Type=? ", param + (msg_type,)) if msg_type else ("", param)
        sql_sub_type, param = ("AND SubType=? ", param + (msg_sub_type,)) if msg_type and msg_sub_type else ("", param)
        sql_start_createtime, param = ("AND CreateTime>=? ", param + (start_createtime,)) if start_createtime else (
            "", param)
        sql_end_createtime, param = ("AND CreateTime<=? ", param + (end_createtime,)) if end_createtime else ("", param)

        sql = (
            "SELECT localId,TalkerId,MsgSvrID,Type,SubType,CreateTime,IsSender,Sequence,StatusEx,FlagEx,Status,"
            "MsgSequence,StrContent,MsgServerSeq,StrTalker,DisplayContent,Reserved0,Reserved1,Reserved3,"
            "Reserved4,Reserved5,Reserved6,CompressContent,BytesExtra,BytesTrans,Reserved2,"
            "ROW_NUMBER() OVER (ORDER BY CreateTime ASC) AS id "
            "FROM MSG__MSG WHERE 1=1 "
            f"{sql_wxid}"
            f"{sql_MsgSequence}"
            f"{sql_type}"
            f"{sql_sub_type}"
            f"{sql_start_createtime}"
            f"{sql_end_createtime}"
            f"ORDER BY CreateTime ASC LIMIT {page_size} OFFSET 0;"
        )
        param = param + (page_size,)
        result = self.execute(sql, param)
        if not result:
            return [], []

        result_data = (msg_utils.get_msg_detail(row, my_talker=my_talker) for row in result)
        rdata = list(result_data)  # 转为列表
        wxid_list = {d['talker'] for d in rdata}  # 创建一个无重复的 wxid 列表
        return rdata, list(wxid_list)

    @db_error
    def get_date_count(self, wxid='', start_time: int = 0, end_time: int = 0, time_format='%Y-%m-%d'):
        """
        获取每日聊天记录数量，包括发送者数量、接收者数量和总数。
        """
        if not self.tables_exist("MSG*__MSG"):
            return {}
        if isinstance(start_time, str) and start_time.isdigit():
            start_time = int(start_time)
        if isinstance(end_time, str) and end_time.isdigit():
            end_time = int(end_time)

        # if start_time or end_time is not an integer and not a float, set both to 0
        if not (isinstance(start_time, (int, float)) and isinstance(end_time, (int, float))):
            start_time = 0
            end_time = 0
        params = ()

        sql_wxid = "AND StrTalker = ? " if wxid else ""
        params = params + (wxid,) if wxid else params

        sql_time = "AND CreateTime BETWEEN ? AND ? " if start_time and end_time else ""
        params = params + (start_time, end_time) if start_time and end_time else params

        sql = (f"SELECT strftime('{time_format}', CreateTime, 'unixepoch', 'localtime') AS date, "
               "       COUNT(*) AS total_count ,"
               "       SUM(CASE WHEN IsSender = 1 THEN 1 ELSE 0 END) AS sender_count, "
               "       SUM(CASE WHEN IsSender = 0 THEN 1 ELSE 0 END) AS receiver_count "
               "FROM MSG__MSG "
               "WHERE StrTalker NOT LIKE '%chatroom%' "
               f"{sql_wxid} {sql_time} "
               f"GROUP BY date ORDER BY date ASC;")
        result = self.execute(sql, params)

        if not result:
            return {}
        # 将查询结果转换为字典
        result_dict = {}
        for row in result:
            date, total_count, sender_count, receiver_count = row
            result_dict[date] = {
                "sender_count": sender_count,
                "receiver_count": receiver_count,
                "total_count": total_count
            }
        return result_dict

    @db_error
    def get_top_talker_count(self, top: int = 10, start_time: int = 0, end_time: int = 0):
        """
        获取聊天记录数量最多的联系人,他们聊天记录数量
        """
        if not self.tables_exist("MSG*__MSG"):
            return {}
        if isinstance(start_time, str) and start_time.isdigit():
            start_time = int(start_time)
        if isinstance(end_time, str) and end_time.isdigit():
            end_time = int(end_time)

        # if start_time or end_time is not an integer and not a float, set both to 0
        if not (isinstance(start_time, (int, float)) and isinstance(end_time, (int, float))):
            start_time = 0
            end_time = 0

        sql_time = f"AND CreateTime BETWEEN {start_time} AND {end_time} " if start_time and end_time else ""
        sql = (
            "SELECT StrTalker, COUNT(*) AS count,"
            "SUM(CASE WHEN IsSender = 1 THEN 1 ELSE 0 END) AS sender_count, "
            "SUM(CASE WHEN IsSender = 0 THEN 1 ELSE 0 END) AS receiver_count "
            "FROM MSG__MSG "
            "WHERE StrTalker NOT LIKE '%chatroom%' "
            f"{sql_time} "
            "GROUP BY StrTalker ORDER BY count DESC "
            f"LIMIT {top};"
        )
        result = self.execute(sql)
        if not result:
            return {}
        # 将查询结果转换为字典
        result_dict = {row[0]: {"total_count": row[1], "sender_count": row[2], "receiver_count": row[3]} for row in
                       result}
        return result_dict





from typing import List, Optional

from pydantic import BaseModel, Field

from pywxdump.db.fts.enums.QueryMsgCategory import QueryMsgCategory


class MsgByQuery(BaseModel):
    """
    消息查询请求
    :param chatRomeId 聊天室ID
    :param page 页码
    :param page_size 每页数量
    :param query_type 类型
    :param query 查询关键字
    :param startDate 开始时间
    :param endDate 结束时间
    """
    chatRomeId: str = Field(..., description="聊天室ID")
    page: int = Field(..., description="页码")
    page_size: int = Field(..., description="每页数量")
    query_type: QueryMsgCategory = Field(..., description="类型")
    query: Optional[str] = None
    startDate: Optional[str] = None
    endDate: Optional[str] = None
    pass







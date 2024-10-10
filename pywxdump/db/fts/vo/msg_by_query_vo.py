from typing import List

from pydantic import BaseModel


class MsgByQueryItemVo(BaseModel):
    """
    消息查询响应
    :param id 消息id
    :param talker 发送人
    :param content 消息内容
    :param thumbnail 缩略图
    :param content 消息内容
    :param send_time 发送时间
    :param file_size 文件大小
    """
    id: int
    talker: str
    content: str
    thumbnail: str
    send_time: str
    file_size: str
    pass


class MsgByQueryVo(BaseModel):
    """
    消息查询响应

     :param total_count 总数
    :param page 页码
    :param page_size 每页数量
    :param itemTypes 类型
    :param items 搜索结果列表
    """
    total_count: int
    page: int
    page_size: int
    items: List[MsgByQueryItemVo]





from abc import ABC
from typing import Optional

from pydantic import BaseModel


class SearchResultItem(BaseModel):
    """
    聚合搜索响应itme

    :param bizId 业务id
    :param thumbnail 缩略图
    :param title 标题
    :param note 备注

    :return SearchResultItem
    """
    bizId: str
    thumbnail: Optional[str] = None
    title: Optional[str] = None
    note: Optional[str] = None


from abc import ABC

from pydantic import BaseModel


class SearchResultItem(BaseModel):
    """
    聚合搜索响应itme

    :param thumbnail 缩略图
    :param title 标题
    :param note 备注

    :return SearchResultItem
    """

    thumbnail: str
    title: str
    note: str


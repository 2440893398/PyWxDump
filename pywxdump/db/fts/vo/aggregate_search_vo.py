from typing import List
from pydantic import BaseModel
from pydantic.v1 import validator

from pywxdump.db.fts.vo.search_result_item import SearchResultItem


class AggregateSearchVo(BaseModel):
    """
    聚合搜索响应

    :param total_count 总数
    :param page 页码
    :param page_size 每页数量
    :param itemTypes 类型
    :param items 搜索结果列表

    """
    total_count: int
    page: int
    page_size: int
    itemTypes: str
    items: List[SearchResultItem]

    @validator('items', each_item=True)
    def check_item_type(cls, v):
        if not isinstance(v, SearchResultItem):
            raise TypeError('All items must be instances of SearchResultItem or its subclasses')
        return v

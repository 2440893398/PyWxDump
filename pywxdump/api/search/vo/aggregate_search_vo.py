from typing import List, Any, Protocol
from pydantic import BaseModel
from pydantic.v1 import validator

from pywxdump.api.search.vo.search_result_item import SearchResultItem


class AggregateSearchVo(BaseModel):
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

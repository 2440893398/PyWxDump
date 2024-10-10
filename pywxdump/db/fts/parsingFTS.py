from abc import ABC, abstractmethod
from enum import Enum

from pywxdump.db.fts.vo.aggregate_search_vo import AggregateSearchVo
from pywxdump.db.dbbase import DatabaseBase

# 高亮标签
HIGHLIGHT_LABEL_LEFT = '<span style="color: blue;">'
HIGHLIGHT_LABEL_RIGHT = '</span>'


class FTSType(Enum):
    """
    聚合搜索响应item类型
    """
    # 联系人
    CONTACT = "contact"
    # 群聊
    GROUP_CHAT = "groupChat"
    # 聊天记录
    CHAT_RECORD = "chatRecord"


class ParsingFTS(DatabaseBase, ABC):
    registry = {}

    @classmethod
    def register(cls, fts_type: FTSType):
        def inner_wrapper(subclass):
            print(f"register {subclass} to {fts_type}")
            cls.registry[fts_type] = subclass
            return subclass

        return inner_wrapper

    @abstractmethod
    def search(self, query: str, page=1, pagesize=10) -> AggregateSearchVo:
        pass

from abc import ABC, abstractmethod

from pywxdump.api.search.vo.aggregate_search_vo import AggregateSearchVo
from pywxdump.common.fts.FTSType import FTSType
from pywxdump.dbpreprocess.dbbase import DatabaseBase


class ParsingFTS(DatabaseBase, ABC):
    registry = {}

    @classmethod
    def register(cls, fts_type: FTSType):
        def inner_wrapper(subclass):
            print(f"register {subclass} to {fts_type}")
            cls.registry[fts_type] = subclass
            return subclass

        return inner_wrapper

    def __new__(cls, db_path):
        return super().__new__(cls, db_path)

    def __init__(self, db_path):
        super().__init__(db_path)

    @abstractmethod
    def search(self, query: str, page=1, pagesize=10) -> AggregateSearchVo:
        pass

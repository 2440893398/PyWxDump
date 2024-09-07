import json
from abc import ABC

from pywxdump.common.fts.FTSType import FTSType
from pywxdump.dbpreprocess.fts.parsingFTS import ParsingFTS

class ParsingFTSFactory:
    registry = {}

    @classmethod
    def register(cls, fts_type: FTSType):
        def inner_wrapper(subclass):
            cls.registry[fts_type] = subclass
            return subclass

        return inner_wrapper

    @staticmethod
    def create(path: str, fts_type: FTSType) -> ParsingFTS:
        subclass = ParsingFTS.registry.get(fts_type)
        if subclass is None:
            raise ValueError(f'Unknown config type: {fts_type}')
        return subclass(path)

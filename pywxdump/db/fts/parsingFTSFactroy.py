from pywxdump.db.fts.parsingFTS import ParsingFTS, FTSType


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

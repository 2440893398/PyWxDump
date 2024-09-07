from enum import Enum


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
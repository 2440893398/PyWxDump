from dataclasses import dataclass
from typing import Dict, Any, List


@dataclass
class MsgInfo:
    """创建时间"""
    CreateTime: str
    """消息拓展信息"""
    extra: str
    """id"""
    id: str
    """是否消息发送者"""
    issender: str
    """消息内容"""
    msg: str
    MsgSequence: str
    MsgSvrID: str
    """聊天室名称"""
    roomname: str
    """消息顺序"""
    Sequence: str
    """文件的url"""
    src: str
    """消息发送人"""
    talker: str
    """消息类型名称"""
    typename: str


@dataclass
class UserInfo:
    """最新的微信id"""
    account: str
    """描述信息"""
    describe: str
    """拓展信息"""
    ExtraBuf: Dict[str, Any]
    """头像url"""
    headImgUrl: str
    """标签列表"""
    LabelIDList: List[str]
    """微信别名"""
    nickname: str
    """微信备注"""
    remark: str
    """微信id"""
    wxid: str


@dataclass
class FtsMsgListVo:
    """Request"""
    msg_list: List[MsgInfo]
    """微信号-用户信息"""
    user_list: Dict[str, UserInfo]

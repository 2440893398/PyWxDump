from dataclasses import dataclass


@dataclass
class FtsMsgListDto:
    """Request"""
    """数据加载方向，up-上 down-下"""
    direction: str
    """数量"""
    limit: int
    """开始序列"""
    start: int
    """微信号"""
    wxid: str
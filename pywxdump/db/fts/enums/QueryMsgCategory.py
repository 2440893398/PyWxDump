from enum import Enum


class QueryMsgCategory(Enum):
    """
    文件、图片与视频、链接、音乐与音频、小程序、视频号、日期
    """
    FILE = ["文件","文件6"]
    IMAGE_VIDEO = ["图片", "视频"]
    LINK = ["(分享)卡片式链接"]
    MUSIC_AUDIO = ["(分享)音乐"]
    MINI_PROGRAM = ["(分享)小程序"]
    VIDEO_NUMBER = ["(分享)视频号名片", "(分享)视频号视频"]
    DATE = []

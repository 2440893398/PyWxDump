# -*- coding: utf-8 -*-#
# -------------------------------------------------------------------------------
# Name:         __init__.py.py
# Description:  
# Author:       xaoyaoo
# Date:         2024/07/23
# -------------------------------------------------------------------------------

from .common_utils import verify_key, get_exe_version, get_exe_bit, wx_core_error
from .ctypes_utils import get_process_list, get_memory_maps, get_process_exe_path, \
    get_file_version_info
from .memory_search import search_memory
from ._loger import wx_core_loger

CORE_DB_TYPE = [
    "PublicMsgMedia",
    "Misc",
    "MSG",
    "MediaMSG",
    "FTSMSG",
    "Voip",
    "SyncMsg",
    "StoreEmotion",
    "Sns",
    "PublicMsg",
    "PreDownload",
    "OpenIMResource",
    "OpenIMMsg",
    "OpenIMMedia",
    "OpenIMContact",
    "NewTips",
    "MultiSearchChatMsg",
    "MicroMsg",
    "Media",
    "LinkHistory",
    "ImageTranslate",
    "HardLinkVideo",
    "HardLinkImage",
    "HardLinkFile",
    "FunctionMsg",
    "FTSFavorite",
    "FTSContact",
    "Favorite",
    "Emotion",
    "CustomerService",
    "ClientConfig",
    "ChatRoomUser",
    "ChatMsg",
    "BizChatMsg",
    "BizChat",
    "ClientGeneral"
]

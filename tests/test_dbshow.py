# -*- coding: utf-8 -*-#
# -------------------------------------------------------------------------------
# Name:         test_dbshow.py
# Description:  显示数据库聊天记录
# Author:       xaoyaoo
# Date:         2023/11/15
# -------------------------------------------------------------------------------
from pywxdump import start_server
from pywxdump.common.config.server_config import ServerConfig

if __name__ == '__main__':
    merge_path = r"****.db"
    wx_path = r"****"
    my_wxid = "****"
    server_config = ServerConfig.builder()
    server_config.merge_path("C:/Users/24408/AppData/Local/Temp/wechat_decrypted_files/merge_all.db")
    server_config.wx_path("C:/Users/24408/Documents/WeChat Files/wxid_fhded1nyrrdr22")
    server_config.my_wxid("wxid_fhded1nyrrdr22")
    server_config.port(5000)
    server_config.online(True)
    server_config.is_open_browser(False)
    #
    # s3Config = S3Config("AKIDaAjAh5JZmuwupiaTKAxfI1kR4gFdv67v", "wlT2ldSBkQBsXCh077va9e4Qh4fEev47",
    #                     "https://cos.ap-nanjing.myqcloud.com")
    # server_config.oss_config(s3Config)
    start_server(server_config.build())


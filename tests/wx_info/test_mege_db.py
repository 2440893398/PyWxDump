import ctypes
import gc
import re
import time
import unittest
from unittest.mock import patch, MagicMock
import os
import sqlite3
from pywxdump.wx_info.merge_db import merge_db


class TestMergeDB(unittest.TestCase):

    # 初始化
    def setUp(self):
        self.db_path = ['./test_data/de_FTSFavorite.db', './test_data/de_MicroMsg.db']
        self.out_path = './test_data/merge.db'
        # merge.db文件已存在
        if os.path.exists(self.out_path):
            # 运行垃圾回收
            gc.collect()
            # 等待一段时间以确保文件句柄释放
            time.sleep(1)
            # 删除重命名后的文件
            os.remove(self.out_path)

    # 清理
    def tearDown(self):
        # merge.db文件已存在
        if os.path.exists(self.out_path):
            # 运行垃圾回收
            gc.collect()
            # 等待一段时间以确保文件句柄释放
            time.sleep(1)
            # 删除重命名后的文件
            os.remove(self.out_path)

    def test_merge_db_creates_new_db(self):
        """
        Test that merge_db creates a new database

        """
        # merge_db不应该抛出任何异常
        result = merge_db(self.db_path, self.out_path)
        self.assertEqual(result, self.out_path)
        # 读取mege.db文件



if __name__ == '__main__':
    unittest.main()

# -*- coding: utf-8 -*-#
# -------------------------------------------------------------------------------
# Name:         merge_db.py
# Description:  
# Author:       xaoyaoo
# Date:         2023/12/03
# -------------------------------------------------------------------------------
import logging
import os
import re
import shutil
import sqlite3
import subprocess
import time
from collections import OrderedDict
from typing import List

from .decryption import batch_decrypt
from .wx_info import get_core_db
from .utils import wx_core_loger, wx_core_error, CORE_DB_TYPE


@wx_core_error
def execute_sql(connection, sql, params=None):
    """
    执行给定的SQL语句，返回结果。
    参数：
        - connection： SQLite连接
        - sql：要执行的SQL语句
        - params：SQL语句中的参数
    """
    try:
        # connection.text_factory = bytes
        cursor = connection.cursor()
        if params:
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)
        return cursor.fetchall()
    except Exception as e:
        try:
            connection.text_factory = bytes
            cursor = connection.cursor()
            if params:
                cursor.execute(sql, params)
            else:
                cursor.execute(sql)
            rdata = cursor.fetchall()
            connection.text_factory = str
            return rdata
        except Exception as e:
            wx_core_loger.error(f"**********\nSQL: {sql}\nparams: {params}\n{e}\n**********", exc_info=True)
            return None


@wx_core_error
def check_create_sync_log(connection):
    """
    检查是否存在表 sync_log,用于记录同步记录，包括微信数据库路径，表名，记录数，同步时间
    :param connection: SQLite连接
    :return: True or False
    """

    out_cursor = connection.cursor()
    # 检查是否存在表 sync_log,用于记录同步记录，包括微信数据库路径，表名，记录数，同步时间
    sync_log_status = execute_sql(connection, "SELECT name FROM sqlite_master WHERE type='table' AND name='sync_log'")
    if len(sync_log_status) < 1:
        #  db_path 微信数据库路径，tbl_name 表名，src_count 源数据库记录数，current_count 当前合并后的数据库对应表记录数
        sync_record_create_sql = ("CREATE TABLE sync_log ("
                                  "id INTEGER PRIMARY KEY AUTOINCREMENT,"
                                  "db_path TEXT NOT NULL,"
                                  "tbl_name TEXT NOT NULL,"
                                  "src_count INT,"
                                  "current_count INT,"
                                  "createTime INT DEFAULT (strftime('%s', 'now')), "
                                  "updateTime INT DEFAULT (strftime('%s', 'now'))"
                                  ");")
        out_cursor.execute(sync_record_create_sql)
        # 创建索引
        out_cursor.execute("CREATE INDEX idx_sync_log_db_path ON sync_log (db_path);")
        out_cursor.execute("CREATE INDEX idx_sync_log_tbl_name ON sync_log (tbl_name);")
        # 创建联合索引，防止重复
        out_cursor.execute("CREATE UNIQUE INDEX idx_sync_log_db_tbl ON sync_log (db_path, tbl_name);")
        connection.commit()
    out_cursor.close()
    return True


@wx_core_error
def check_create_file_md5(connection):
    """
    检查是否存在表 file_md5,用于记录文件信息，后续用于去重等操作，暂时闲置
    """
    pass


@wx_core_error
def merge_db(db_paths: List[dict], save_path: str = "merge.db", is_merge_data: bool = True,
             startCreateTime: int = 0, endCreateTime: int = 0):
    """
    合并数据库 会忽略主键以及重复的行。
    :param db_paths: [{"db_path": "xxx", "de_path": "xxx"},...]
                        db_path表示初始路径，de_path表示解密后的路径；初始路径用于保存合并的日志情况，解密后的路径用于读取数据
    :param save_path: str 输出文件路径
    :param is_merge_data: bool 是否合并数据(如果为False，则只解密，并创建表，不插入数据)
    :param startCreateTime: 开始时间戳 主要用于MSG数据库的合并
    :param endCreateTime:  结束时间戳 主要用于MSG数据库的合并
    :return:
    """
    if os.path.isdir(save_path):
        save_path = os.path.join(save_path, f"merge_{int(time.time())}.db")

    db_paths = _get_db_paths(db_paths)
    databases = _get_database_names(db_paths)

    with sqlite3.connect(save_path) as outdb:
        _setup_outdb(outdb)
        _merge_databases(outdb, databases, is_merge_data, startCreateTime, endCreateTime)

    return save_path


def _merge_databases(outdb, databases, is_merge_data, start_create_time, end_create_time):
    """合并数据库"""
    # 别名映射表
    alias_map = {
        'OpenIMMsg.ChatCRMsg': 'OpenIMMsg__ChatCRMsg',
        'CustmoerService.ChatCRMsg': 'CustmoerService__ChatCRMsg',
        'ChatMsg.ChatCRMsg': 'ChatMsg__ChatCRMsg',
        'OpenIMMsg.Name2ID': 'OpenIMMsg__Name2ID',
        'Emotion.Name2ID': 'Emotion__Name2ID',
        'SyncMsg.Name2ID': 'SyncMsg__Name2ID',
        'ChatMsg.Name2ID_v1': 'ChatMsg__Name2ID_v1',
        'CustmoerService.Name2ID': 'CustmoerService__Name2ID',
        'OpenIMMsg.TransCRTable': 'OpenIMMsg__TransCRTable',
        'CustmoerService.TransCRTable': 'CustmoerService__TransCRTable',
        'ChatMsg.TransCRTable': 'ChatMsg__TransCRTable',
        'MultiSearchChatMsg.NameToId': 'MultiSearchChatMsg__NameToId',
        'FTSFavorite.NameToId': 'FTSFavorite__NameToId',
        'FTSContact.NameToId': 'FTSContact__NameToId',
        'MicroMsg.ChatInfo': 'MicroMsg__ChatInfo',
        'BizChat.ChatInfo': 'BizChat__ChatInfo',
        'MicroMsg.Session': 'MicroMsg__Session',
        'CustmoerService.Session': 'CustmoerService__Session',
        'FTSFavorite.DBTableInfo': 'FTSFavorite__DBTableInfo',
        'FTSContact.DBTableInfo': 'FTSContact__DBTableInfo',
        'Favorite.Config': 'Favorite__Config',
        'ClientConfig.Config': 'ClientConfig__Config',
        'Favorite.Voice': 'Favorite__Voice',
        'ClientGeneral.Config': 'ClientGeneral__Config',
        'ChatMsg.ChatMsg': 'ChatMsg__ChatMsg',
        'BizChatMsg.ChatMsg': 'BizChatMsg__ChatMsg',
        'ChatMsg.TransTable': 'ChatMsg__TransTable',
        'BizChatMsg.TransTable': 'BizChatMsg__TransTable',
        'ClientGeneral.MmExptAppItem': 'ClientGeneral__MmExptAppItem',
        'ClientConfig.MmExptAppItem': 'ClientConfig__MmExptAppItem'
    }

    for alias, path in databases.items():
        _attach_database(outdb, alias, path)
        _merge_tables(outdb, path, alias, alias_map, is_merge_data, start_create_time, end_create_time)
        _detach_database(outdb, alias)


def _attach_database(outdb, alias, path):
    """附加数据库"""
    sql_attach = f"ATTACH DATABASE '{path}' AS {alias}"
    outdb.execute(sql_attach)
    outdb.commit()


def _insert_data_virtual_table(outdb, alias, real_table_name, table_name):
    sql_query_columns = f"PRAGMA table_info({real_table_name})"
    columns = execute_sql(outdb, sql_query_columns)
    # 使用 OrderedDict 来保持顺序
    col_type = OrderedDict(
        (i[1] if isinstance(i[1], str) else i[1].decode(),
         i[2] if isinstance(i[2], str) else i[2].decode())
        for i in columns
    )

    # 提取列名，保持顺序
    columns = [i[0] for i in col_type.items()]

    if not columns or len(columns) < 1:
        pass

    # 构建 SQL 语句
    sql = (f"INSERT OR IGNORE INTO {real_table_name} (rowid,{','.join([i for i in columns])}) "
           f"SELECT docid,{','.join([f'c{i}{columns[i]}' for i in range(len(columns))])} FROM {alias}.{table_name}_content")
    execute_sql(outdb, sql)
    pass


def _merge_tables(outdb, path, alias, alias_map, is_merge_data, start_create_time, end_create_time):
    """合并表"""
    is_multi = False
    multi_table = ["FTSMSG", "MediaMSG", "MSG"]
    for multi in multi_table:
        if multi in alias:
            is_multi = True
            break
    sql_query_tbl_name = f"SELECT name FROM {alias}.sqlite_master WHERE type='table' ORDER BY name;"

    tables = execute_sql(outdb, sql_query_tbl_name)
    for table in tables:
        table_name = table[0]
        tableKey = f"{alias}.{table_name}"
        real_table_name = alias_map.get(tableKey, table_name)
        if is_multi:
            real_table_name = f"{alias}__{real_table_name}"
            pass
        if table_name == "sqlite_sequence":
            continue
        if _is_ignore_table(table_name):
            continue
        _create_table(outdb, alias, real_table_name, table_name)
        if _is_virtual_table(outdb, alias, table_name):
            if is_merge_data:
                _insert_data_virtual_table(outdb, alias, real_table_name, table_name)
        else:
            if is_merge_data:
                _insert_data(outdb, alias, real_table_name, table_name, start_create_time, end_create_time)
                _cleanup_index(outdb, real_table_name)
        _update_sync_log(outdb, path, table_name)


def _is_ignore_table(table_name):
    """判断是否忽略的表"""
    ignore_tables = ["content", "docsize", "segdir", "segments", "stat"]
    for ignore_table in ignore_tables:
        if table_name.endswith(ignore_table):
            return True
    return False


def _create_table(outdb, alias, real_table_name, table_name):
    """创建表"""
    if _is_virtual_table(outdb, alias, table_name):
        sql_create_tbl = _get_virtual_table_create_sql(outdb, alias, table_name, real_table_name)
        execute_sql(outdb, sql_create_tbl)
    else:
        sql_create_tbl = f"CREATE TABLE IF NOT EXISTS {real_table_name} AS SELECT * FROM {alias}.{table_name} WHERE 0 = 1;"
        execute_sql(outdb, sql_create_tbl)

        # 复制索引
        _copy_indexes(outdb, alias, table_name, real_table_name)

        # 创建包含 NULL 值比较的 UNIQUE 索引
        _create_unique_index(outdb, real_table_name)


def _is_virtual_table(outdb, alias, table_name):
    """检查是否是虚拟表"""
    sql_query_tbl_sql = f"SELECT sql FROM {alias}.sqlite_master WHERE tbl_name='{table_name}' and type='table';"
    tbl_sql = execute_sql(outdb, sql_query_tbl_sql)
    return tbl_sql[0][0].find("VIRTUAL TABLE") != -1


def _get_virtual_table_create_sql(outdb, alias, table_name, real_table_name):
    """获取虚拟表的创建 SQL"""
    sql_query_tbl_info = f"SELECT sql FROM {alias}.sqlite_master WHERE tbl_name='{table_name}'"
    tbl_info = execute_sql(outdb, sql_query_tbl_info)
    sql_create_tbl = tbl_info[0][0]

    # 修改使用的分词器
    sql_create_tbl = re.sub(r'fts4', 'fts5', sql_create_tbl)
    sql_create_tbl = re.sub(r',\s*notindexed=[^,]+', '', sql_create_tbl)
    sql_create_tbl = re.sub(r'tokenize=mmTokenizer\s*"\w+"', "tokenize='simple'", sql_create_tbl)
    sql_create_tbl = sql_create_tbl.replace(table_name, real_table_name).replace("CREATE VIRTUAL TABLE",
                                                                                 "CREATE VIRTUAL TABLE IF NOT EXISTS")
    return sql_create_tbl


def _copy_indexes(outdb, alias, table_name, real_table_name):
    """复制索引"""
    sql_query_index = f"PRAGMA {alias}.index_list({table_name})"
    index_list = execute_sql(outdb, sql_query_index)
    for index in index_list:
        index_name = index[1]
        sql_query_index_sql = f"SELECT sql FROM {alias}.sqlite_master WHERE name='{index_name}'"
        index_sql = execute_sql(outdb, sql_query_index_sql)
        if index_sql and index_sql[0][0]:
            create_index_sql = index_sql[0][0].replace(table_name, real_table_name)
            create_index_sql = re.sub(f"CREATE INDEX", "CREATE INDEX IF NOT EXISTS", create_index_sql)
            execute_sql(outdb, create_index_sql)


def _create_unique_index(outdb, real_table_name):
    """创建包含 NULL 值比较的 UNIQUE 索引"""
    sql_query_columns = f"PRAGMA table_info({real_table_name})"
    columns = [col[1] for col in execute_sql(outdb, sql_query_columns)]
    coalesce_columns = ','.join(f"COALESCE({column}, '')" for column in columns)
    index_name = f"{real_table_name}_unique_index"
    sql = f"CREATE UNIQUE INDEX IF NOT EXISTS {index_name} ON {real_table_name} ({coalesce_columns})"
    execute_sql(outdb, sql)


def _insert_data(outdb, alias, real_table_name, table_name, start_create_time, end_create_time):
    """插入数据"""
    cursor = outdb.cursor()

    # 查询 sync_log 获取上次同步的记录数
    src_count = _get_sync_log_count(outdb, table_name, alias)

    # 获取源数据库的记录数
    sql_count = f"SELECT COUNT(*) FROM {alias}.{table_name}"
    count = execute_sql(outdb, sql_count)[0][0]
    if count <= src_count:
        return

    # 构建 INSERT 语句
    sql_query_columns = f"PRAGMA {alias}.table_info({table_name})"
    columns = [col[1] for col in execute_sql(outdb, sql_query_columns)]
    sql_base = f"SELECT {','.join(columns)} FROM {alias}.{table_name} "
    where_clauses, params = [], []
    if "CreateTime" in columns:
        if start_create_time > 0:
            where_clauses.append("CreateTime > ?")
            params.append(start_create_time)
        if end_create_time > 0:
            where_clauses.append("CreateTime < ?")
            params.append(end_create_time)
    sql = f"{sql_base} WHERE {' AND '.join(where_clauses)}" if where_clauses else sql_base
    src_data = execute_sql(outdb, sql, tuple(params))

    # 插入数据
    sql_insert = f"INSERT OR IGNORE INTO {real_table_name} ({','.join(columns)}) VALUES ({','.join(['?'] * len(columns))})"
    cursor.executemany(sql_insert, src_data)
    outdb.commit()


def _get_sync_log_count(outdb, table_name, db_path):
    """获取 sync_log 中记录的上次同步的记录数"""
    cursor = outdb.cursor()
    sql_query_sync_log = f"SELECT src_count FROM sync_log WHERE db_path=? AND tbl_name=?"
    sync_log = execute_sql(outdb, sql_query_sync_log, (db_path, table_name))
    return sync_log[0][0] if sync_log else 0


def _update_sync_log(outdb, db_path, table_name):
    """更新 sync_log 中的记录"""
    sql_query_sync_log = f"SELECT * FROM sync_log WHERE db_path=? AND tbl_name=?"
    sync_log = execute_sql(outdb, sql_query_sync_log, (db_path, table_name))
    if not sync_log or len(sync_log) < 1:
        sql_insert_sync_log = "INSERT INTO sync_log (db_path, tbl_name, src_count, current_count) VALUES (?, ?, ?, ?)"
        execute_sql(outdb, sql_insert_sync_log, (db_path, table_name, 0, 0))
    outdb.commit()


def _cleanup_index(outdb, real_table_name):
    """删除无用索引"""
    cursor = outdb.cursor()
    index_name = f"{real_table_name}_unique_index"
    sql = f"DROP INDEX IF EXISTS {index_name}"
    cursor.execute(sql)


def _detach_database(outdb, alias):
    """分离数据库"""
    sql_detach = f"DETACH DATABASE {alias}"
    outdb.execute(sql_detach)
    outdb.commit()


def _get_db_paths(db_paths):
    """获取要合并的数据库路径列表"""

    # [{"db_path": "xxx", "de_path": "xxx"},...]
    #                         db_path表示初始路径，de_path表示解密后的路径；初始路径用于保存合并的日志情况，解密后的路径用于读取数据
    _db_paths = []
    if isinstance(db_paths, list):
        # alias, file_path
        _db_paths = [db.get('de_path') for i, db in enumerate(db_paths)]
    else:
        raise TypeError("db_paths 类型错误")
    return _db_paths


def _get_database_names(db_paths):
    """获取数据库名称映射"""
    databases = {}
    for db_path in db_paths:
        db_name = os.path.basename(db_path).split(".")[0].replace("de_", "")
        databases[db_name] = db_path
    return databases


def _setup_outdb(outdb):
    """设置输出数据库"""
    # 加载分词器
    current_path = os.path.dirname(__file__)
    real_time_exe_path = os.path.join(current_path, "tools", "libsimple-windows-x64", "simple.dll")
    outdb.enable_load_extension(True)
    outdb.load_extension(real_time_exe_path)

    # 检查是否存在表 sync_log,用于记录同步记录
    _create_sync_log_table(outdb)


def _create_sync_log_table(outdb):
    """创建 sync_log 表"""
    cursor = outdb.cursor()
    sync_log_status = execute_sql(outdb, "SELECT name FROM sqlite_master WHERE type='table' AND name='sync_log'")
    if len(sync_log_status) < 1:
        sync_record_create_sql = (
            "CREATE TABLE sync_log ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT,"
            "db_path TEXT NOT NULL,"
            "tbl_name TEXT NOT NULL,"
            "src_count INT,"
            "current_count INT,"
            "createTime INT DEFAULT (strftime('%s', 'now')), "
            "updateTime INT DEFAULT (strftime('%s', 'now'))"
            ");"
        )
        cursor.execute(sync_record_create_sql)

        # 创建索引
        cursor.execute("CREATE INDEX idx_sync_log_db_path ON sync_log (db_path);")
        cursor.execute("CREATE INDEX idx_sync_log_tbl_name ON sync_log (tbl_name);")
        cursor.execute("CREATE UNIQUE INDEX idx_sync_log_db_tbl ON sync_log (db_path, tbl_name);")
        outdb.commit()



@wx_core_error
def decrypt_merge(wx_path: str, key: str, outpath: str = "",
                  merge_save_path: str = None,
                  is_merge_data=True, is_del_decrypted: bool = True,
                  startCreateTime: int = 0, endCreateTime: int = 0,
                  db_type=None) -> (bool, str):
    """
    解密合并数据库 msg.db, microMsg.db, media.db,注意：会删除原数据库
    :param wx_path: 微信路径 eg: C:\\*******\\WeChat Files\\wxid_*********
    :param key: 解密密钥
    :param outpath: 输出路径
    :param merge_save_path: 合并后的数据库路径
    :param is_merge_data: 是否合并数据(如果为False，则只解密，并创建表，不插入数据)
    :param is_del_decrypted: 是否删除解密后的数据库（除了合并后的数据库）
    :param startCreateTime: 开始时间戳 主要用于MSG数据库的合并
    :param endCreateTime:  结束时间戳 主要用于MSG数据库的合并
    :param db_type: 数据库类型，从核心数据库中选择
    :return: (true,解密后的数据库路径) or (false,错误信息)
    """
    if db_type is None:
        db_type = []

    outpath = outpath if outpath else "decrypt_merge_tmp"
    merge_save_path = os.path.join(outpath,
                                   f"merge_{int(time.time())}.db") if merge_save_path is None else merge_save_path
    decrypted_path = os.path.join(outpath, "decrypted")

    if not wx_path or not key or not os.path.exists(wx_path):
        wx_core_loger.error("参数错误", exc_info=True)
        return False, "参数错误"

    # 解密
    code, wxdbpaths = get_core_db(wx_path, db_type)
    if not code:
        wx_core_loger.error(f"获取数据库路径失败{wxdbpaths}", exc_info=True)
        return False, wxdbpaths

    # 判断out_path是否为空目录
    if os.path.exists(decrypted_path) and os.listdir(decrypted_path):
        for root, dirs, files in os.walk(decrypted_path, topdown=False):
            for name in files:
                os.remove(os.path.join(root, name))
            for name in dirs:
                os.rmdir(os.path.join(root, name))

    if not os.path.exists(decrypted_path):
        os.makedirs(decrypted_path)

    wxdbpaths = {i["db_path"]: i for i in wxdbpaths}

    # 调用 decrypt 函数，并传入参数   # 解密
    code, ret = batch_decrypt(key=key, db_path=list(wxdbpaths.keys()), out_path=decrypted_path, is_print=False)
    if not code:
        wx_core_loger.error(f"解密失败{ret}", exc_info=True)
        return False, ret

    out_dbs = []
    for code1, ret1 in ret:
        if code1:
            out_dbs.append(ret1)

    parpare_merge_db_path = []
    for db_path, out_path, _ in out_dbs:
        parpare_merge_db_path.append({"db_path": db_path, "de_path": out_path})
    merge_save_path = merge_db(parpare_merge_db_path, merge_save_path, is_merge_data=is_merge_data,
                               startCreateTime=startCreateTime, endCreateTime=endCreateTime)
    if is_del_decrypted:
        shutil.rmtree(decrypted_path, True)
    if isinstance(merge_save_path, str):
        return True, merge_save_path
    else:
        return False, "未知错误"


@wx_core_error
def merge_real_time_db(key, merge_path: str, db_paths: [dict] or dict, real_time_exe_path: str = None):
    """
    合并实时数据库消息,暂时只支持64位系统
    :param key:  解密密钥
    :param merge_path:  合并后的数据库路径
    :param db_paths:  [dict] or dict eg: {'wxid': 'wxid_***', 'db_type': 'MicroMsg',
                        'db_path': 'C:\**\wxid_***\Msg\MicroMsg.db', 'wxid_dir': 'C:\***\wxid_***'}
    :param real_time_exe_path:  实时数据库合并工具路径
    :return:
    """
    try:
        import platform
    except:
        raise ImportError("未找到模块 platform")
    # 判断系统位数是否为64位，如果不是则抛出异常
    if platform.architecture()[0] != '64bit':
        raise Exception("System is not 64-bit.")

    if isinstance(db_paths, dict):
        db_paths = [db_paths]

    merge_path = os.path.abspath(merge_path)  # 合并后的数据库路径，必须为绝对路径
    merge_path_base = os.path.dirname(merge_path)  # 合并后的数据库路径
    if not os.path.exists(merge_path_base):
        os.makedirs(merge_path_base)

    endbs = []
    for db_info in db_paths:
        db_path = os.path.abspath(db_info['db_path'])
        if not os.path.exists(db_path):
            # raise FileNotFoundError("数据库不存在")
            continue
        endbs.append(os.path.abspath(db_path))
    endbs = '" "'.join(list(set(endbs)))

    if not os.path.exists(real_time_exe_path if real_time_exe_path else ""):
        current_path = os.path.dirname(__file__)  # 获取当前文件夹路径
        real_time_exe_path = os.path.join(current_path, "tools", "realTime.exe")
    if not os.path.exists(real_time_exe_path):
        raise FileNotFoundError("未找到实时数据库合并工具")
    real_time_exe_path = os.path.abspath(real_time_exe_path)

    # 调用cmd命令
    cmd = f'{real_time_exe_path} "{key}" "{merge_path}" "{endbs}"'
    # os.system(cmd)
    # wx_core_loger.info(f"合并实时数据库命令：{cmd}")
    p = subprocess.Popen(cmd, shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd=merge_path_base,
                         creationflags=subprocess.CREATE_NO_WINDOW)
    out, err = p.communicate()  # 查看返回值
    if out and out.decode("utf-8").find("SUCCESS") >= 0:
        wx_core_loger.info(f"合并实时数据库成功{out}")
        return True, merge_path
    else:
        wx_core_loger.error(f"合并实时数据库失败\n{out}\n{err}")
        return False, (out, err)


@wx_core_error
def all_merge_real_time_db(key, wx_path, merge_path: str, real_time_exe_path: str = None):
    """
    合并所有实时数据库
    注：这是全量合并，会有可能产生重复数据，需要自行去重
    :param key:  解密密钥
    :param wx_path:  微信路径
    :param merge_path:  合并后的数据库路径 eg: C:\\*******\\WeChat Files\\wxid_*********\\merge.db
    :param real_time_exe_path:  实时数据库合并工具路径
    :return:
    """
    if not merge_path or not key or not wx_path or not wx_path:
        return False, "msg_path or media_path or wx_path or key is required"
    try:
        from pywxdump import get_core_db
    except ImportError:
        return False, "未找到模块 pywxdump"
    db_paths = get_core_db(wx_path, CORE_DB_TYPE)
    if not db_paths[0]:
        return False, db_paths[1]
    db_paths = db_paths[1]
    code, ret = merge_real_time_db(key=key, merge_path=merge_path, db_paths=db_paths,
                                   real_time_exe_path=real_time_exe_path)
    if code:
        return True, merge_path
    else:
        return False, ret

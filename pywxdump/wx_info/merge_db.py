# -*- coding: utf-8 -*-#
# -------------------------------------------------------------------------------
# Name:         merge_db.py
# Description:  
# Author:       xaoyaoo
# Date:         2023/12/03
# -------------------------------------------------------------------------------
import logging
import os
import random
import re
import shutil
import sqlite3
import subprocess
import time
from collections import OrderedDict
from typing import List


def merge_copy_db(db_path, save_path):
    logging.warning("merge_copy_db is deprecated, use merge_db instead, will be removed in the future.")
    if isinstance(db_path, list) and len(db_path) == 1:
        db_path = db_path[0]
    if not os.path.exists(db_path):
        raise FileNotFoundError("目录不存在")
    shutil.move(db_path, save_path)


# 合并相同名称的数据库 MSG0-MSG9.db
def merge_msg_db(db_path: list, save_path: str, CreateTime: int = 0):  # CreateTime: 从这个时间开始的消息 10位时间戳
    logging.warning("merge_msg_db is deprecated, use merge_db instead, will be removed in the future.")
    # 判断save_path是否为文件夹
    if os.path.isdir(save_path):
        save_path = os.path.join(save_path, "merge_MSG.db")

    merged_conn = sqlite3.connect(save_path)
    merged_cursor = merged_conn.cursor()

    for db_file in db_path:
        c_tabels = merged_cursor.execute(
            "select tbl_name from sqlite_master where  type='table' and tbl_name!='sqlite_sequence'")
        tabels_all = c_tabels.fetchall()  # 所有表名
        tabels_all = [row[0] for row in tabels_all]

        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()

        # 创建表
        if len(tabels_all) < 4:
            cursor.execute(
                "select tbl_name,sql from sqlite_master where type='table' and tbl_name!='sqlite_sequence'")
            c_part = cursor.fetchall()

            for tbl_name, sql in c_part:
                if tbl_name in tabels_all:
                    continue
                try:
                    merged_cursor.execute(sql)
                    tabels_all.append(tbl_name)
                except Exception as e:
                    print(f"error: {db_file}\n{tbl_name}\n{sql}\n{e}\n**********")
                    raise e
                merged_conn.commit()

        # 写入数据
        for tbl_name in tabels_all:
            if tbl_name == "MSG":
                MsgSvrIDs = merged_cursor.execute(
                    f"select MsgSvrID from MSG where CreateTime>{CreateTime} and MsgSvrID!=0").fetchall()

                cursor.execute(f"PRAGMA table_info({tbl_name})")
                columns = cursor.fetchall()
                columns = [column[1] for column in columns[1:]]

                ex_sql = f"select {','.join(columns)} from {tbl_name} where CreateTime>{CreateTime} and MsgSvrID not in ({','.join([str(MsgSvrID[0]) for MsgSvrID in MsgSvrIDs])})"
                cursor.execute(ex_sql)

                insert_sql = f"INSERT INTO {tbl_name} ({','.join(columns)}) VALUES ({','.join(['?' for _ in range(len(columns))])})"
                try:
                    merged_cursor.executemany(insert_sql, cursor.fetchall())
                except Exception as e:
                    print(
                        f"error: {db_file}\n{tbl_name}\n{insert_sql}\n{cursor.fetchall()}\n{len(cursor.fetchall())}\n{e}\n**********")
                    raise e
                merged_conn.commit()
            else:
                ex_sql = f"select * from {tbl_name}"
                cursor.execute(ex_sql)

                for r in cursor.fetchall():
                    cursor.execute(f"PRAGMA table_info({tbl_name})")
                    columns = cursor.fetchall()
                    if len(columns) > 1:
                        columns = [column[1] for column in columns[1:]]
                        values = r[1:]
                    else:
                        columns = [columns[0][1]]
                        values = [r[0]]

                        query_1 = "select * from " + tbl_name + " where " + columns[0] + "=?"  # 查询语句 用于判断是否存在
                        c2 = merged_cursor.execute(query_1, values)
                        if len(c2.fetchall()) > 0:  # 已存在
                            continue
                    query = "INSERT INTO " + tbl_name + " (" + ",".join(columns) + ") VALUES (" + ",".join(
                        ["?" for _ in range(len(values))]) + ")"

                    try:
                        merged_cursor.execute(query, values)
                    except Exception as e:
                        print(f"error: {db_file}\n{tbl_name}\n{query}\n{values}\n{len(values)}\n{e}\n**********")
                        raise e
                merged_conn.commit()

        conn.close()
    sql = '''delete from MSG where localId in (SELECT localId from MSG
       where MsgSvrID != 0  and MsgSvrID in (select MsgSvrID  from MSG
                          where MsgSvrID != 0 GROUP BY MsgSvrID  HAVING COUNT(*) > 1)
         and localId not in (select min(localId)  from MSG
                             where MsgSvrID != 0  GROUP BY MsgSvrID HAVING COUNT(*) > 1))'''
    c = merged_cursor.execute(sql)
    merged_conn.commit()
    merged_conn.close()
    return save_path


def merge_media_msg_db(db_path: list, save_path: str):
    logging.warning("merge_media_msg_db is deprecated, use merge_db instead, will be removed in the future.")
    # 判断save_path是否为文件夹
    if os.path.isdir(save_path):
        save_path = os.path.join(save_path, "merge_Media.db")
    merged_conn = sqlite3.connect(save_path)
    merged_cursor = merged_conn.cursor()

    for db_file in db_path:

        s = "select tbl_name,sql from sqlite_master where  type='table' and tbl_name!='sqlite_sequence'"
        have_tables = merged_cursor.execute(s).fetchall()
        have_tables = [row[0] for row in have_tables]

        conn_part = sqlite3.connect(db_file)
        cursor = conn_part.cursor()

        if len(have_tables) < 1:
            cursor.execute(s)
            table_part = cursor.fetchall()
            tblname, sql = table_part[0]

            sql = "CREATE TABLE Media(localId INTEGER  PRIMARY KEY AUTOINCREMENT,Key TEXT,Reserved0 INT,Buf BLOB,Reserved1 INT,Reserved2 TEXT)"
            try:
                merged_cursor.execute(sql)
                have_tables.append(tblname)
            except Exception as e:
                print(f"error: {db_file}\n{tblname}\n{sql}\n{e}\n**********")
                raise e
            merged_conn.commit()

        for tblname in have_tables:
            s = "select Reserved0 from " + tblname
            merged_cursor.execute(s)
            r0 = merged_cursor.fetchall()

            ex_sql = f"select `Key`,Reserved0,Buf,Reserved1,Reserved2 from {tblname} where Reserved0 not in ({','.join([str(r[0]) for r in r0])})"
            cursor.execute(ex_sql)
            data = cursor.fetchall()

            insert_sql = f"INSERT INTO {tblname} (Key,Reserved0,Buf,Reserved1,Reserved2) VALUES ({','.join(['?' for _ in range(5)])})"
            try:
                merged_cursor.executemany(insert_sql, data)
            except Exception as e:
                print(f"error: {db_file}\n{tblname}\n{insert_sql}\n{data}\n{len(data)}\n{e}\n**********")
                raise e
            merged_conn.commit()
        conn_part.close()

    merged_conn.close()
    return save_path


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
            logging.error(f"**********\nSQL: {sql}\nparams: {params}\n{e}\n**********", exc_info=True)
            return None


def merge_db(db_paths, save_path="merge.db", startCreateTime: int = 0, endCreateTime: int = 0):
    """
    合并数据库 会忽略主键以及重复的行。

    :param db_paths: 要合并的数据库路径或路径列表
    :param save_path: 合并后的数据库保存路径
    :param start_create_time: 按创建时间过滤, 大于该时间的数据会被合并
    :param end_create_time: 按创建时间过滤, 小于该时间的数据会被合并
    :return: 合并后的数据库路径
    """
    if os.path.isdir(save_path):
        save_path = os.path.join(save_path, f"merge_{int(time.time())}.db")

    db_paths = _get_db_paths(db_paths)
    databases = _get_database_names(db_paths)

    with sqlite3.connect(save_path) as outdb:
        _setup_outdb(outdb)
        _merge_databases(outdb, databases, startCreateTime, endCreateTime)

    return save_path


def _merge_databases(outdb, databases, start_create_time, end_create_time):
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
        _merge_tables(outdb, path, alias, alias_map, start_create_time, end_create_time)
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


def _merge_tables(outdb, path, alias, alias_map, start_create_time, end_create_time):
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
            _insert_data_virtual_table(outdb, alias, real_table_name, table_name)
        else:
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
    _db_paths = []
    if isinstance(db_paths, str):
        if os.path.isdir(db_paths):
            _db_paths = [os.path.join(db_paths, i) for i in os.listdir(db_paths) if i.endswith(".db")]
        elif os.path.isfile(db_paths):
            _db_paths = [db_paths]
        else:
            raise FileNotFoundError("db_paths 不存在")
    elif isinstance(db_paths, list):
        _db_paths = db_paths
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


def decrypt_merge(wx_path, key, outpath="", CreateTime: int = 0, endCreateTime: int = 0, db_type: List[str] = []) -> (
        bool, str):
    """
    解密合并数据库 msg.db, microMsg.db, media.db,注意：会删除原数据库
    :param wx_path: 微信路径 eg: C:\\*******\\WeChat Files\\wxid_*********
    :param key: 解密密钥
    :return: (true,解密后的数据库路径) or (false,错误信息)
    """
    from .decryption import batch_decrypt
    from .get_wx_info import get_core_db

    outpath = outpath if outpath else "decrypt_merge_tmp"
    merge_save_path = os.path.join(outpath, "merge_all.db")
    decrypted_path = os.path.join(outpath, "decrypted")

    if not wx_path or not key:
        return False, "参数错误"

    # 分割wx_path的文件名和父目录
    db_type_set: set[str] = {"PublicMsgMedia",
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
                             "ClientGeneral"}
    if len(db_type) == 0:
        db_type = list(db_type_set)
    else:
        for i in db_type:
            if i not in db_type_set:
                return False, f"db_type参数错误, 可用选项 {db_type_set}"
    # 解密
    code, wxdbpaths = get_core_db(wx_path, db_type)
    if not code:
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

    # 调用 decrypt 函数，并传入参数   # 解密
    code, ret = batch_decrypt(key, wxdbpaths, decrypted_path, False)
    if not code:
        return False, ret

    out_dbs = []
    for code1, ret1 in ret:
        if code1:
            out_dbs.append(ret1[1])
    parpare_merge_db_path = []
    for i in out_dbs:
        for j in db_type:
            if j in i:
                parpare_merge_db_path.append(i)
                break
    de_db_type = [f"de_{i}" for i in db_type]
    parpare_merge_db_path = [i for i in out_dbs if any(keyword in i for keyword in de_db_type)]

    merge_save_path = merge_db(parpare_merge_db_path, merge_save_path, startCreateTime=CreateTime,
                               endCreateTime=endCreateTime)

    return True, merge_save_path


def merge_real_time_db(key, merge_path: str, db_paths: [str] or str):
    """
    合并实时数据库消息,暂时只支持64位系统
    :param key:  解密密钥
    :param db_paths:  数据库路径
    :param merge_path:  合并后的数据库路径
    :return:
    """
    try:
        import platform
    except:
        raise ImportError("未找到模块 platform")
    # 判断系统位数是否为64位，如果不是则抛出异常
    if platform.architecture()[0] != '64bit':
        raise Exception("System is not 64-bit.")

    if isinstance(db_paths, str):
        db_paths = [db_paths]

    endbs = []

    for db_path in db_paths:
        if not os.path.exists(db_path):
            # raise FileNotFoundError("数据库不存在")
            continue
        if "MSG" not in db_path and "MicroMsg" not in db_path and "MediaMSG" not in db_path:
            # raise FileNotFoundError("数据库不是消息数据库")  # MicroMsg实时数据库
            continue
        endbs.append(db_path)
    endbs = '" "'.join(list(set(endbs)))

    merge_path_base = os.path.dirname(merge_path)  # 合并后的数据库路径

    # 获取当前文件夹路径
    current_path = os.path.dirname(__file__)
    real_time_exe_path = os.path.join(current_path, "tools", "realTime.exe")

    # 调用cmd命令
    cmd = f'{real_time_exe_path} "{key}" "{merge_path}" "{endbs}"'
    # os.system(cmd)
    p = subprocess.Popen(cmd, shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd=merge_path_base,
                         creationflags=subprocess.CREATE_NO_WINDOW)
    p.communicate()
    return True, merge_path


def all_merge_real_time_db(key, wx_path, merge_path):
    """
    合并所有实时数据库
    注：这是全量合并，会有可能产生重复数据，需要自行去重
    :param key:  解密密钥
    :param wx_path:  微信路径
    :param merge_path:  合并后的数据库路径 eg: C:\\*******\\WeChat Files\\wxid_*********\\merge.db
    :return:
    """
    if not merge_path or not key or not wx_path or not wx_path:
        return False, "msg_path or media_path or wx_path or key is required"
    try:
        from pywxdump import get_core_db
    except ImportError:
        return False, "未找到模块 pywxdump"

    db_paths = get_core_db(wx_path, ["MediaMSG", "MSG", "MicroMsg"])
    if not db_paths[0]:
        return False, db_paths[1]
    db_paths = db_paths[1]
    merge_real_time_db(key=key, merge_path=merge_path, db_paths=db_paths)
    return True, merge_path

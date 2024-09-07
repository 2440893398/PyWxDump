import sys
import traceback
from typing import Dict

import sqlglot
from sqlalchemy import create_engine, text, CursorResult, Result
from sqlalchemy.engine.result import SimpleResultMetaData, IteratorResult, MergedResult
from sqlalchemy.orm import sessionmaker, scoped_session
from contextlib import contextmanager
import logging
import tempfile
import uuid
import os
import glob

from sqlglot.expressions import Table, AggFunc, Group

from pywxdump.common.tools.sqlglot_utils import SqlglotUtils
from pywxdump.dbpreprocess.aggregate.MutableRow import MutableRow
from pywxdump.dbpreprocess.aggregate.aggregate_factory import AggregateStrategyFactory
from pywxdump.file import AttachmentContext


class DatabaseBase:
    _singleton_instances = {}  # 使用字典存储不同db_path对应的单例实例
    _connection_pool = {}  # 使用字典存储不同db_path对应的连接池
    _class_name = "DatabaseBase"
    _multi_table = {"FTSMSG", "MediaMSG", "MSG"}
    _multi_base_delimiter = "__"

    def __new__(cls, db_path):
        if cls.__name__ not in cls._singleton_instances:
            cls._singleton_instances[cls.__name__] = super().__new__(cls)
        return cls._singleton_instances[cls.__name__]

    def __init__(self, db_path):
        self._db_path = db_path
        self._engine = self._create_engine(db_path)
        self._SessionFactory = scoped_session(sessionmaker(bind=self._engine))
        self._setup()

    @classmethod
    def _create_engine(cls, db_path):
        if not AttachmentContext.exists(db_path):
            raise FileNotFoundError(f"File does not exist: {db_path}")

        if db_path in cls._connection_pool and cls._connection_pool[db_path] is not None:
            return cls._connection_pool[db_path]

        if not AttachmentContext.isLocalPath(db_path):
            temp_dir = tempfile.gettempdir()
            local_path = os.path.join(temp_dir, f"{uuid.uuid1()}.db")
            logging.info(f"下载文件到本地: {db_path} -> {local_path}")
            AttachmentContext.download_file(db_path, local_path)
        else:
            local_path = db_path

        engine = create_engine(f'sqlite:///{local_path}', connect_args={'check_same_thread': False})
        logging.info(f"Engine created for {db_path}")
        cls._connection_pool[db_path] = engine
        return engine

    @contextmanager
    def session_scope(self):
        session = self._SessionFactory()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            logging.error(f"Session rollback due to exception: {str(e)}")
            raise
        finally:
            session.close()

    def execute_sql(self, sql, params: tuple = ()) -> [any]:
        execute_result = self.__execute(sql, params)
        if execute_result is None:
            return []
        return execute_result.fetchall()


    def execute_sql(self, sql, params: tuple = ()) -> [any]:
        execute_result = self.execute(sql, params)
        if execute_result is None:
            return []
        return execute_result.fetchall()

    def execute(self, sql, params: [dict, tuple]):
        """
        执行sql语句

        :param sql: sql语句
        :param params: 参数

        :return: 执行结果
        """
        if not sql:
            raise ValueError("SQL statement cannot be empty")

        with self.session_scope() as session:
            try:
                sqlTree = SqlglotUtils.parse_one(sql)
                originalSqlTree = sqlTree
                # 判断是否存在分表
                exist_multi, multi_table = self.__exist_multi_base(sqlTree)
                if exist_multi:
                    real_multi_table = {}
                    for table in multi_table:
                        base, table_name = table.split(self._multi_base_delimiter)
                        partitioned_tables = self.__get_partitioned_tables(base, table_name)
                        real_multi_table[table] = partitioned_tables

                    # 校验分库数量是否一致
                    table_counts = [len(v) for v in real_multi_table.values()]
                    if len(set(table_counts)) != 1:
                        raise ValueError(f"分表数量不一致: {real_multi_table}")
                    table_count = table_counts[0]
                    if SqlglotUtils.is_aggregate(originalSqlTree):
                        # 解析聚合策略
                        strategy = AggregateStrategyFactory.getFieldAggregates(sqlTree)
                        # sql 预处理
                        for ele in strategy.values():
                            sqlTree = ele.sqlPreprocessing(sqlTree)
                        new_sql = SqlglotUtils.sql(sqlTree)
                        table_counts[0] = table_counts[0] if table_counts[0] > 0 else 1
                        real_result = {}
                        for i in range(table_count):
                            for tableName in multi_table:
                                realTableName = real_multi_table[tableName][i]
                                new_sql = new_sql.replace(tableName, realTableName)
                            # 如果是tuple，则转换为字典
                            if isinstance(params, tuple):
                                new_sql, params = self._deal_sql_params(new_sql, params)
                            result = session.execute(text(new_sql), params)
                            real_result = self.__aggregate_result(real_result, result, strategy)
                        # 创建元数据
                        fieldNames = SqlglotUtils.get_select_fields_name(originalSqlTree)
                        metadata = SimpleResultMetaData(fieldNames)
                        data_iterator = iter(real_result.values())
                        # 创建 IteratorResult
                        result = IteratorResult(metadata, data_iterator)
                        return result
                    else:
                        # 创建元数据
                        fieldNames = SqlglotUtils.get_select_fields_name(originalSqlTree)
                        metadata = SimpleResultMetaData(fieldNames)
                        real_result = MergedResult(metadata, [])
                        for i in range(table_count):
                            for tableName in multi_table:
                                realTableName = real_multi_table[tableName][i]
                                sql = sql.replace(tableName, realTableName)
                                # 如果是tuple，则转换为字典
                            if isinstance(params, tuple):
                                sql, params = self._deal_sql_params(sql, params)
                            result = session.execute(text(sql), params)
                            real_result.merge(result)
                        return real_result
                else:
                    # 如果是tuple，则转换为字典
                    if isinstance(params, tuple):
                        sql, params = self._deal_sql_params(sql, params)
                    result = session.execute(text(sql), params)
                return result
            except Exception as e:
                # 堆栈打印
                exc_type, exc_value, exc_traceback = sys.exc_info()
                print("".join(traceback.format_exception(exc_type, exc_value, exc_traceback)))
                logging.error(f"**********\nSQL: {sql}\nparams: {params}\n{str(e)}\n**********")
                raise e

    def execute_sql_page(self, sql, params: tuple = (), page=1, pagesize=10) -> [int, [Result]]:
        """
        分页查询

        :param sql: 查询语句
        :param params: 查询参数
        :param page: 页码
        :param pagesize: 每页数量

        :return: 总数，查询结果
        """
        #去掉；符合
        sql = sql.strip().rstrip(";")
        # 查询总数
        count_sql = f"SELECT COUNT(1) FROM ({sql})"
        count_result = self.execute(count_sql, params)
        total_count = count_result.scalar()
        if count_result == 0:
            return 0, []
        originalSqlTree = SqlglotUtils.parse_one(sql)
        offset = (page - 1) * pagesize
        # 查询分页数据
        exist_multi_base,_ = SqlglotUtils.exist_multi_base(originalSqlTree)
        if not exist_multi_base:
            paginated_sql = f"{sql} LIMIT ? OFFSET ?"
            paginated_params = params + (pagesize, offset)
            execute_result = self.execute(paginated_sql, paginated_params)
            return total_count, execute_result
        else:
            execute_result = self.execute(sql, params)
            # 创建元数据
            fieldNames = SqlglotUtils.get_field_name(originalSqlTree)
            metadata = SimpleResultMetaData(fieldNames)
            real_result = execute_result.fetchall()[offset:offset + pagesize]
            data_iterator = iter(real_result)
            # 创建 IteratorResult
            result = IteratorResult(metadata, data_iterator)
            return total_count, result

    def close_connection(self):
        if self._db_path in self._connection_pool:
            self._connection_pool[self._db_path].dispose()
            logging.info(f"Closed database connection - {self._db_path}")
            del self._connection_pool[self._db_path]
            if not AttachmentContext.isLocalPath(self._db_path):
                self._clearTmpDb()

    @classmethod
    def terminate_connection(cls, db_path: str):
        if db_path in cls._connection_pool:
            cls._connection_pool[db_path].dispose()
            logging.info(f"Closed database connection - {db_path}")
            del cls._connection_pool[db_path]
            if not AttachmentContext.isLocalPath(db_path):
                cls._clearTmpDb()

    def close_all_connections(self):
        for db_path, engine in self._connection_pool.items():
            engine.dispose()
            logging.info(f"Closed database connection - {db_path}")
        self._connection_pool.clear()
        self._clearTmpDb()

    @staticmethod
    def _clearTmpDb():
        temp_dir = tempfile.gettempdir()
        db_files = glob.glob(os.path.join(temp_dir, '*.db'))
        for db_file in db_files:
            try:
                os.remove(db_file)
            except Exception as e:
                logging.error(f"Error deleting {db_file}: {e}")

    def show_singleton_instances(self):
        print(self._singleton_instances)

    def __del__(self):
        self.close_connection()

    def _deal_sql_params(self, sql, params: tuple) -> (str, dict):

        """
        处理sql中的参数，将tuple类型的参数转换为dict类型

        :param sql: sql语句
        :param params: 参数

        :return: 处理后的sql和参数
        """

        # ? -> 1, 2, 3
        new_params = {}
        new_sql = sql
        for i in range(len(params)):
            if isinstance(params[i], tuple) | isinstance(params[i], list) | isinstance(params[i], set):
                new_params.update({f"ele{i + 1}": v for i, v in enumerate(params[i])})
                eleStr = ','.join(f":ele{c + 1}" for c in range(len(params[i])))
                new_sql = new_sql.replace("?", eleStr, 1)
            else:
                new_params[str(i)] = params[i]
                new_sql = new_sql.replace("?", f":{i}", 1)
        return new_sql, new_params

    def _setup(self):
        # todo 加载分词器
        # current_path = os.path.dirname(__file__)
        real_time_exe_path = ("C:\\Users\\24408\\IdeaProjects\\PyWxDump\\pywxdump\\wx_info\\tools\\"
                              "libsimple-windows-x64\\simple.dll")
        conn = self._engine.raw_connection()
        conn.enable_load_extension(True)
        conn.load_extension(real_time_exe_path)
        pass

    def to_dic(self, result: Result):
        if result is None:
            return []

        # 获取列名
        rows = result.mappings().all()

        # 将行数据转换为字典列表
        dict_list = [dict(row) for row in rows]

        return dict_list

    def __exist_multi_base(self, tree):
        """
        判断是否存在分库表

        :param tree: 语法树
        :return: 是否存在多表查询
        """
        multi_tables = []

        for node in tree.walk():
            if isinstance(node, Table):
                base = node.name.split(self._multi_base_delimiter)
                if len(base) > 1:
                    if base[0] in self._multi_table:
                        multi_tables.append(node.name)
        return len(multi_tables) > 0, multi_tables

    def __get_partitioned_tables(self, base_name, table_name):
        """
        获取分表

        :param base_name: 基础表名

        :return: 分表列表
        """
        partitioned_tables = self.execute_sql(
            f"SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '{base_name}%__{table_name}'")
        return [table[0] for table in partitioned_tables]

    def __aggregate_result(self, real_result, result, strategy) -> Dict[str, MutableRow]:
        """
        聚合结果

        :param real_result: 聚合结果
        :param result: 查询结果
        :param strategy: 聚合策略

        :return: 聚合结果
        """

        if real_result is None:
            real_result = {}

        if result is None:
            return real_result

        for row in result:
            for key, field in strategy.items():
                rowKey = field.getRowKey(row)
                if rowKey in real_result:
                    field.aggregate(real_result[rowKey], row)
                else:
                    real_result[rowKey] = MutableRow(row)

        return real_result


if __name__ == '__main__':
    a = DatabaseBase("C:/Users/24408/AppData/Local/Temp/wechat_decrypted_files/merge_all.db")
    total, d1 = a.execute_sql_page("select * from FTSContact__NameToId where rowid in (?)", ((1, 2, 3),), 1, 10)
    print(total)
    column_names = d1.keys()
    # 查看对象的类型
    print(type(d1))
    print("Column names:", column_names)
    print(a.to_dic(d1))
    print([i[1] for i in d1])
    rows = d1.mappings().all()
    # print(rows)
    # print([row.items() for row in d1.fetchall()])
    a.close_connection()

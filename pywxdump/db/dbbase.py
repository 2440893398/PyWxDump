# -*- coding: utf-8 -*-
# -------------------------------------------------------------------------------
# Name:         dbbase.py
# Description:  数据库基础类，实现数据库连接与操作
# Author:       xaoyaoo
# Date:         2024/04/15
# -------------------------------------------------------------------------------

import fnmatch
import logging
import os
import re
import sqlite3
import sys
import traceback
from contextlib import contextmanager
from pprint import pformat
from typing import Dict, List, Tuple, Union, Any

from sqlalchemy import create_engine, text, event
from sqlalchemy.engine import Engine, Result
from sqlalchemy.engine.result import SimpleResultMetaData, IteratorResult
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlglot.expressions import Table, AggFunc, Group, Limit, Offset, Placeholder

# 假设这些模块存在于项目中，如果不存在需要相应地修改导入路径
from .aggregate.MutableRow import MutableRow
from .aggregate.aggregate_factory import AggregateStrategyFactory
from .utils import db_loger
from pywxdump.common.tools.sqlglot_utils import SqlglotUtils

# 设置日志格式
logging.basicConfig(level=logging.DEBUG,
                    style='{',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    format='[{levelname[0]}] {asctime} [{name}:{levelno}] {pathname}:{lineno} {message}'
                    )


class DatabaseSingletonBase:
    """
    数据库单例基类，负责管理数据库连接池
    """
    _db_pool: Dict[str, Engine] = {}  # 使用字典存储不同db_key对应的连接池

    @classmethod
    def connect(cls, db_config: Dict[str, Any]) -> Engine:
        """
        连接数据库，如果需要增加其他数据库连接，则重写该方法
        :param db_config: 数据库配置
        :return: 数据库引擎（连接池）
        """
        if not db_config:
            raise ValueError("db_config 不能为空")

        db_key = db_config.get("key", "xaoyaoo_741852963")
        db_type = db_config.get("type", "sqlite")

        if db_engine := cls._db_pool.get(db_key):
            return db_engine

        if db_type == "sqlite":
            db_path = db_config.get("path", "")
            if not os.path.exists(db_path):
                raise FileNotFoundError(f"文件不存在: {db_path}")
            engine = create_engine(f'sqlite:///{db_path}', connect_args={'check_same_thread': False})
            cls._setup(engine)
        elif db_type == "mysql":
            mysql_config = {
                'user': db_config['user'],
                'host': db_config['host'],
                'password': db_config['password'],
                'database': db_config['database'],
                'port': db_config['port']
            }
            engine = create_engine(
                f"mysql+pymysql://{mysql_config['user']}:{mysql_config['password']}"
                f"@{mysql_config['host']}:{mysql_config['port']}/{mysql_config['database']}")
        else:
            raise ValueError(f"不支持的数据库类型: {db_type}")

        db_loger.info(f"{engine} 连接句柄创建 {db_config}")
        cls._db_pool[db_key] = engine
        return engine

    @classmethod
    def _setup(cls, engine: Engine):
        """
        初始化数据库，针对 SQLite 引擎加载扩展
        :param engine: 数据库引擎
        """

        @event.listens_for(engine, "connect")
        def set_sqlite_pragma(dbapi_connection, _):
            if isinstance(dbapi_connection, sqlite3.Connection):
                dbapi_connection.enable_load_extension(True)
                # 此处的路径需要根据实际情况修改
                extension_path = "C:\\Users\\24408\\IdeaProjects\\PyWxDump\\pywxdump\\wx_core\\tools\\libsimple-windows-x64\\simple.dll"
                dbapi_connection.load_extension(extension_path)


class DatabaseBase(DatabaseSingletonBase):
    """
    数据库基础类，实现数据库的基本操作
    """
    _multi_table = {"FTSMSG", "MediaMSG", "MSG"}
    _multi_base_delimiter = "__"

    def __init__(self, db_config: Dict[str, Any]):
        """
        初始化数据库连接和会话工厂
        :param db_config: 数据库配置
        """
        self.config = db_config
        self._engine = self.connect(self.config)
        self._SessionFactory = scoped_session(sessionmaker(bind=self._engine))
        self.existed_tables = self.__get_existed_tables()

    @contextmanager
    def session_scope(self):
        """
        上下文管理器，用于管理数据库会话的提交和回滚
        """
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

    def __get_existed_tables(self) -> List[str]:
        """
        获取数据库中已存在的表名列表
        :return: 表名列表
        """
        sql = "SELECT tbl_name FROM sqlite_master WHERE type = 'table' AND tbl_name != 'sqlite_sequence';"
        existing_tables = self.execute(sql)
        return [row[0].lower() for row in existing_tables] if existing_tables else []

    def tables_exist(self, required_tables: Union[str, List[str]]) -> bool:
        """
        判断所需的表是否存在，支持通配符匹配
        :param required_tables: 表名或表名列表，支持 '*' 和 '?' 通配符
        :return: 如果所有表都存在，则返回 True，否则返回 False
        """
        if isinstance(required_tables, str):
            required_tables = [required_tables]

        lowercase_existing_tables = {table.lower() for table in self.existed_tables}

        def table_matches(pattern):
            return any(fnmatch.fnmatch(existing, pattern.lower()) for existing in lowercase_existing_tables)

        all_exist = all(table_matches(table) for table in required_tables)

        if not all_exist:
            db_loger.warning(f"所需的表不存在: {required_tables=}, {self.existed_tables=}, {all_exist=}")

        return all_exist

    def execute(self, sql: str, params: Tuple = ()) -> List[Any]:
        """
        执行 SQL 查询并获取所有结果
        :param sql: SQL 查询语句
        :param params: 查询参数
        :return: 查询结果列表
        """
        execute_result = self.__execute(sql, params)
        return execute_result.fetchall() if execute_result else []

    def execute_sql(self, sql: str, params: Tuple = ()) ->Result:
        """
        执行 SQL 查询并获取所有结果
        :param sql: SQL 查询语句
        :param params: 查询参数
        :return: 查询结果列表
        """
        execute_result = self.__execute(sql, params)
        return execute_result


    def __execute(self, sql: str, params: Union[Dict, Tuple]) -> Result:
        """
        实际执行 SQL 查询的内部方法
        :param sql: SQL 查询语句
        :param params: 查询参数
        :return: 查询结果 Result 对象
        """
        if not sql:
            raise ValueError("SQL 语句不能为空")

        with self.session_scope() as session:
            try:
                sql_tree = SqlglotUtils.parse_one(sql)

                # 判断是否存在分表
                exist_multi, multi_tables = SqlglotUtils.exist_multi_base(sql_tree)
                if exist_multi:
                    return self._execute_multi_table_query(session, sql_tree, multi_tables, sql, params)
                else:
                    # 如果是 tuple，则转换为字典
                    new_sql, new_params = self._deal_sql_params(sql, params) if isinstance(params, tuple) else (
                        sql, params)
                    self.__log_sql_and_params(new_sql, new_params)
                    result = session.execute(text(new_sql), new_params)
                    return result
            except Exception as e:
                # 堆栈打印
                exc_type, exc_value, exc_traceback = sys.exc_info()
                traceback.print_exception(exc_type, exc_value, exc_traceback)
                logging.error(f"**********\nSQL: {sql}\nparams: {params}\nError: {e}\n**********")
                raise

    def _execute_multi_table_query(self, session, sql_tree, multi_tables, sql, params):
        """
        执行涉及多表的查询，包括分表的处理和结果的合并
        :param session: 数据库会话
        :param sql_tree: 解析后的 SQL 语法树
        :param multi_tables: 多表名列表
        :param sql: 原始 SQL 语句
        :param params: 查询参数
        :return: 查询结果 Result 对象
        """
        params = list(params)
        # 原始的 SQL 语法树（用于保留分页等信息）
        original_sql_tree = sql_tree.copy()
        real_multi_tables = {}
        for table in multi_tables:
            base, table_name = table.split(self._multi_base_delimiter)
            partitioned_tables = self.__get_partitioned_tables(base, table_name)
            real_multi_tables[table] = partitioned_tables

        # 校验分表数量是否一致
        table_counts = [len(v) for v in real_multi_tables.values()]
        if len(set(table_counts)) != 1:
            raise ValueError(f"分表数量不一致: {real_multi_tables}")
        table_count = table_counts[0]

        # 处理分页
        limit_config, sql_tree = self.__deal_pagination(sql_tree, sql, params)

        # 获取排序规则
        order_fields = SqlglotUtils.multi_key_sort(sql_tree)
        field_names = SqlglotUtils.get_select_fields_name(original_sql_tree)
        order_fields_index = []
        for field, sort_type in order_fields:
            order_fields_index.append((field_names.index(field), sort_type))

        if SqlglotUtils.is_aggregate(original_sql_tree):
            # 处理聚合查询
            return self._execute_aggregate_query(session, sql_tree, original_sql_tree, real_multi_tables,
                                                 multi_tables, table_count, order_fields_index, limit_config, tuple(params))
        else:
            # 非聚合查询
            return self._execute_regular_query(session, sql_tree, original_sql_tree, real_multi_tables,
                                               multi_tables, table_count, order_fields_index, limit_config,  tuple(params))

    def _execute_aggregate_query(self, session, sql_tree, original_sql_tree, real_multi_tables, multi_tables,
                                 table_count, order_fields_index, limit_config, params):
        """
        执行聚合查询并合并结果
        """
        # 解析聚合策略
        strategy = AggregateStrategyFactory.get_field_aggregates(sql_tree)
        # SQL 预处理
        for ele in strategy.values():
            sql_tree = ele.sql_preprocessing(sql_tree)
        new_sql_template = SqlglotUtils.sql(sql_tree)

        real_result = {}
        for i in range(table_count):
            new_sql = new_sql_template
            for table_name in multi_tables:
                real_table_name = real_multi_tables[table_name][i]
                new_sql = new_sql.replace(table_name, real_table_name)
            # 如果是 tuple，则转换为字典
            new_sql, new_params = self._deal_sql_params(new_sql, params) if isinstance(params, tuple) else (
                new_sql, params)
            self.__log_sql_and_params(new_sql, new_params)
            result = session.execute(text(new_sql), new_params)
            real_result = self.__aggregate_result(real_result, result, strategy)

        # 创建元数据
        field_names = SqlglotUtils.get_select_fields_name(original_sql_tree)
        metadata = SimpleResultMetaData(field_names)
        all_results = list(real_result.values())
        # 排序
        self.merge_sorted_sequences(all_results, order_fields_index)
        # 分页处理
        if limit_config:
            all_results = all_results[limit_config['offset']: limit_config['offset'] + limit_config['limit']]
        # 返回结果
        return IteratorResult(metadata, iter(all_results))

    def _execute_regular_query(self, session, sql_tree, original_sql_tree, real_multi_tables, multi_tables,
                               table_count, order_fields_index, limit_config, params):
        """
        执行非聚合查询并合并结果
        """
        real_result = []
        for i in range(table_count):
            new_sql = SqlglotUtils.sql(sql_tree)
            for table_name in multi_tables:
                real_table_name = real_multi_tables[table_name][i]
                new_sql = new_sql.replace(table_name, real_table_name)
            # 如果是 tuple，则转换为字典
            new_sql, new_params = self._deal_sql_params(new_sql, params) if isinstance(params, tuple) else (
                new_sql, params)
            self.__log_sql_and_params(new_sql, new_params)
            result = session.execute(text(new_sql), new_params)
            real_result.extend(result.fetchall())

        # 排序
        self.merge_sorted_sequences(real_result, order_fields_index)
        # 创建元数据
        field_names = SqlglotUtils.get_select_fields_name(original_sql_tree)
        metadata = SimpleResultMetaData(field_names)
        # 分页处理
        if limit_config:
            real_result = real_result[limit_config['offset']: limit_config['offset'] + limit_config['limit']]
        # 返回结果
        return IteratorResult(metadata, iter(real_result))

    @staticmethod
    def merge_sorted_sequences(sequences, order_fields):
        """
        合并并排序序列，支持复杂的 ORDER BY 字段及其排序方向。

        :param sequences: 项目列表，例如 [(1, 2), (1, 3)]
        :param order_fields: 排序依据，例如 [(0, 'asc'), (1, 'desc')]
        :return: 单个已排序序列
        """
        from functools import total_ordering

        @total_ordering
        class SortKey:
            def __init__(self, value, ascending):
                self.value = value
                self.ascending = ascending

            def __eq__(self, other):
                return self.value == other.value

            def __lt__(self, other):
                if self.ascending:
                    return self.value < other.value
                else:
                    return self.value > other.value

        def sort_key(item):
            return tuple(
                SortKey(item[index], direction == 'asc')
                for index, direction in order_fields
            )

        # 就地对序列进行排序
        sequences.sort(key=sort_key)
        return sequences

    @staticmethod
    def __log_sql_and_params(sql: str, params: Dict):
        """
        格式化并记录 SQL 语句和参数
        :param sql: SQL 语句
        :param params: 参数字典
        """
        formatted_sql = re.sub(r'\s+', ' ', sql).strip()

        # 替换参数
        for key, value in params.items():
            placeholder = f":{key}"
            if isinstance(value, str):
                formatted_value = f"'{value}'"
            elif value is None:
                formatted_value = 'NULL'
            else:
                formatted_value = str(value)
            formatted_sql = formatted_sql.replace(placeholder, formatted_value)

        # 打印格式化后的 SQL
        logging.info("Executing SQL:")
        logging.info(formatted_sql)

        # 打印参数
        logging.info("\nParameters:")
        logging.info(pformat(params, indent=4))
        logging.info("\n" + "=" * 50 + "\n")

    def execute_sql_page(self, sql: str, params: Tuple = (), page: int = 1, pagesize: int = 10) -> Tuple[int, Result]:
        """
        分页查询
        :param sql: 查询语句
        :param params: 查询参数
        :param page: 页码
        :param pagesize: 每页数量
        :return: 总数，查询结果
        """
        sql = sql.strip().rstrip(";")
        original_sql_tree = SqlglotUtils.parse_one(sql)
        # 查询总数
        count_sql = f"SELECT COUNT(1) FROM ({sql})"
        count_result = self.__execute(count_sql, params)
        total_count = count_result.scalar() if count_result else 0
        if total_count == 0:
            field_names = SqlglotUtils.get_select_fields_name(original_sql_tree)
            return 0, IteratorResult(SimpleResultMetaData(field_names), iter([]))

        offset = (page - 1) * pagesize
        # 查询分页数据
        exist_multi_base, _ = SqlglotUtils.exist_multi_base(original_sql_tree)
        if not exist_multi_base:
            paginated_sql = f"{sql} LIMIT ? OFFSET ?"
            paginated_params = params + (pagesize, offset)
            execute_result = self.__execute(paginated_sql, paginated_params)
            return total_count, execute_result
        else:
            execute_result = self.__execute(sql, params)
            # 分页处理
            real_result = execute_result.fetchall()[offset: offset + pagesize]
            field_names = SqlglotUtils.get_select_fields_name(original_sql_tree)
            metadata = SimpleResultMetaData(field_names)
            result = IteratorResult(metadata, iter(real_result))
            return total_count, result

    @staticmethod
    def _deal_sql_params(sql: str, params: Tuple) -> Tuple[str, Dict]:
        """
        将 SQL 中的 '?' 参数占位符转换为命名参数，生成新的 SQL 和参数字典
        :param sql: SQL 语句
        :param params: 参数元组
        :return: 新的 SQL 语句和参数字典
        """
        new_params = {}
        param_index = 0
        new_sql = sql
        for param in params:
            if isinstance(param, (tuple, list, set)):
                placeholders = []
                for ele in param:
                    key = f"param_{param_index}"
                    new_params[key] = ele
                    placeholders.append(f":{key}")
                    param_index += 1
                placeholder_str = ', '.join(placeholders)
                new_sql = new_sql.replace("?", placeholder_str, 1)
            else:
                key = f"param_{param_index}"
                new_params[key] = param
                new_sql = new_sql.replace("?", f":{key}", 1)
                param_index += 1
        return new_sql, new_params

    @staticmethod
    def to_dic(result: Result) -> List[Dict]:
        """
        将查询结果转换为字典列表
        :param result: 查询结果 Result 对象
        :return: 字典列表
        """
        return [dict(row) for row in result.mappings().all()] if result else []

    def __get_partitioned_tables(self, base_name: str, table_name: str) -> List[str]:
        """
        获取分表名称列表
        :param base_name: 基础表名
        :param table_name: 表名
        :return: 分表名称列表
        """
        pattern = f"{base_name}%{self._multi_base_delimiter}{table_name}"
        partitioned_tables = self.execute(
            f"SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '{pattern}'")
        return [table[0] for table in partitioned_tables]

    def __aggregate_result(self, real_result: Dict[str, MutableRow], result: Result, strategy) -> Dict[str, MutableRow]:
        """
        聚合查询结果
        :param real_result: 已有的聚合结果
        :param result: 新查询的结果
        :param strategy: 聚合策略
        :return: 更新后的聚合结果
        """
        if real_result is None:
            real_result = {}

        if result is None:
            return real_result

        for row in result:
            row_key = strategy[0].get_row_key(row)
            if row_key in real_result:
                for key, field in strategy.items():
                    field.aggregate(real_result[row_key], row)
            else:
                real_result[row_key] = MutableRow(row)
        return real_result

    def close(self):
        """
        关闭数据库连接
        """
        if self.config.get("type", "sqlite") == "sqlite":
            self._engine.dispose()
        self._engine = None
        self._SessionFactory.remove()
        self.config = None

    def __del__(self):
        self.close()

    def __deal_pagination(self, sql_tree, sql, params):
        """
        处理分页信息
        :param sql_tree: SQL 语法树
        :param sql: 原始 SQL 查询字符串
        :param params: SQL 查询参数列表
        :return: 分页配置信息和处理过的 SQL 语法树
        """
        limit_config = {}
        if SqlglotUtils.is_page(sql_tree):
            limit_expr = sql_tree.args.get('limit')
            offset_expr = sql_tree.args.get('offset')

            # 处理 OFFSET
            if offset_expr:
                offset_value_ele = offset_expr.expression
                if isinstance(offset_value_ele, Placeholder):
                    # 获取占位符在参数列表中的索引
                    offset_placeholder_index = len(params) - 2
                    offset_value = int(params[offset_placeholder_index])
                    # 删除 OFFSET 参数
                    params.pop(offset_placeholder_index)
                else:
                    offset_value = int(offset_value_ele.this) if offset_value_ele.this else 0
            else:
                offset_value = 0  # 默认值


            # 处理 LIMIT
            if limit_expr:
                limit_value_ele = limit_expr.expression
                if isinstance(limit_value_ele, Placeholder):
                    # 获取占位符在参数列表中的索引
                    limit_placeholder_index = len(params) - 1
                    limit_value = int(params[limit_placeholder_index])
                    # 删除 LIMIT 参数
                    params.pop(limit_placeholder_index)
                else:
                    limit_value = int(limit_value_ele.this) if limit_value_ele.this else 10
            else:
                limit_value = 10  # 默认值



            limit_config['limit'] = limit_value
            limit_config['offset'] = offset_value

            # 为了避免遗漏数据，可以适当增加每个分表的 LIMIT
            if SqlglotUtils.is_aggregate(sql_tree):
                # 移除分页
                sql_tree = SqlglotUtils.remove_page(sql_tree)
            else:
                sql_tree = SqlglotUtils.remove_page(sql_tree)
                per_table_limit = limit_value + offset_value
                sql_tree = sql_tree.copy()
                sql_tree.set(Limit, per_table_limit)
                sql_tree.set(Offset, 0)
        else:
            limit_config = None
        return limit_config, sql_tree


class MsgDb(DatabaseBase):
    """
    消息数据库类，继承自 DatabaseBase，可根据需要扩展
    """

    def p(self):
        sel = "SELECT MsgSvrID, CreateTime FROM MSG__MSG WHERE StrTalker = 'wxid_0605816058212' ORDER BY CreateTime DESC LIMIT 10 OFFSET 0"
        data = self.execute(sel)
        return data


if __name__ == '__main__':
    config1 = {
        "key": "test1",
        "type": "sqlite",
        "path": r"C:\Users\24408\AppData\Local\Temp\wechat_decrypted_files\merge_all.db"
    }
    db = MsgDb(config1)
    print(db.p())

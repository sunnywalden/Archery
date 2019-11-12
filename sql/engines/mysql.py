# -*- coding: UTF-8 -*-
import logging
import traceback
import MySQLdb
import re
import os
import sqlparse
from MySQLdb.connections import numeric_part
from DBUtils.PooledDB import PooledDB, SharedDBConnection
import asyncio
from MySQLdb.constants import FIELD_TYPE

from sql.utils.multi_thread import multi_thread
from sql.utils.async_tasks import async_tasks
from sql.utils.sql_conn import setup_conn, shutdown_conn
from sql.engines.goinception import GoInceptionEngine
from sql.utils.sql_utils import get_syntax_type, remove_comments
from . import EngineBase
from .models import ResultSet, ReviewResult, ReviewSet
from .inception import InceptionEngine
from sql.utils.data_masking import data_masking
from common.config import SysConfig
from common.utils.get_logger import get_logger


class MysqlEngine(EngineBase):
    def __init__(self, instance=None):
        super().__init__(instance=instance)
        self.logger = get_logger()

    def get_connection(self, db_name=None):
        if self.pool: return self.pool
        if db_name:
            self.pool = setup_conn(self.host, self.port, user=self.user, password=self.password, database=db_name, charset='utf8mb4')
        else:
            self.pool = setup_conn(self.host, self.port, user=self.user, password=self.password,
                                   charset='utf8mb4')
        return self.pool

    def close(self, pool=None):
        if self.pool:
            self.pool.close()


    @property
    def name(self):
        return 'MySQL'

    @property
    def info(self):
        return 'MySQL engine'

    @property
    def auto_backup(self):
        """是否支持备份"""
        return True

    @property
    def seconds_behind_master(self):
        slave_status = self.query(sql='show slave status', close_conn=False)
        return slave_status.rows[0][32] if slave_status.rows else None

    @property
    def server_version(self):
        version = self.query(sql="select @@version").rows[0][0]
        return tuple([numeric_part(n) for n in version.split('.')[:3]])

    def kill_connection(self, thread_id):
        """终止数据库连接"""
        self.query(sql=f'kill {thread_id}')

    def get_all_databases(self):
        """获取数据库列表, 返回一个ResultSet"""
        sql = "show databases"
        result = self.query(sql=sql)
        db_list = [row[0] for row in result.rows
                   if row[0] not in ('information_schema', 'performance_schema', 'mysql', 'test', 'sys')]
        result.rows = db_list
        return result

    def get_all_tables(self, db_name):
        """获取table 列表, 返回一个ResultSet"""
        sql = "show tables"
        result = self.query(db_name=db_name, sql=sql)
        tb_list = [row[0] for row in result.rows if row[0] not in ['test']]
        result.rows = tb_list
        return result

    def get_all_columns_by_tb(self, db_name, tb_name):
        """获取所有字段, 返回一个ResultSet"""
        sql = f"""SELECT 
            COLUMN_NAME,
            COLUMN_TYPE,
            CHARACTER_SET_NAME,
            IS_NULLABLE,
            COLUMN_KEY,
            EXTRA,
            COLUMN_COMMENT
        FROM
            information_schema.COLUMNS
        WHERE
            TABLE_SCHEMA = '{db_name}'
                AND TABLE_NAME = '{tb_name}'
        ORDER BY ORDINAL_POSITION;"""
        result = self.query(db_name=db_name, sql=sql)
        column_list = [row[0] for row in result.rows]
        result.rows = column_list
        return result

    def describe_table(self, db_name, tb_name):
        """return ResultSet 类似查询"""
        sql = f"show create table {tb_name};"
        result = self.query(db_name=db_name, sql=sql)
        return result

    def query(self, db_name='', sql='', limit_num=0, close_conn=False):
        """返回 ResultSet """
        result_set = ResultSet(full_sql=sql)
        try:
            # 连接池获取连接
            pool = self.get_connection(db_name=db_name)
            conn = pool.connection()
            cursor = conn.cursor()
            effect_row = cursor.execute(sql)
            if int(limit_num) > 0:
                rows = cursor.fetchmany(size=int(limit_num))
            else:
                rows = cursor.fetchall()
            fields = cursor.description
            cursor.close()
            conn.close()

            result_set.column_list = [i[0] for i in fields] if fields else []
            result_set.rows = rows
            result_set.affected_rows = effect_row
        except Exception as e:
            self.logger.error(f"MySQL语句执行报错，语句：{sql}，错误信息{traceback.format_exc()}")
            result_set.error = str(e)
        finally:
            if close_conn:
                self.close()
        return result_set

    def query_check(self, db_name='', sql=''):
        # 查询语句的检查、注释去除、切分
        result = {'msg': '', 'bad_query': False, 'filtered_sql': sql, 'has_star': False}
        # 删除注释语句，进行语法判断，执行第一条有效sql
        try:
            sql = sqlparse.format(sql, strip_comments=True)
            sql = sqlparse.split(sql)[0]
            result['filtered_sql'] = sql.strip()
        except IndexError:
            result['bad_query'] = True
            result['msg'] = '没有有效的SQL语句'
        if re.match(r"^select|^show|^explain", sql, re.I) is None:
            result['bad_query'] = True
            result['msg'] = '不支持的查询语法类型!'
        if '*' in sql:
            result['has_star'] = True
            result['msg'] = 'SQL语句中含有 * '
        return result

    def filter_sql(self, sql='', limit_num=0):
        # 对查询sql增加limit限制,limit n 或 limit n,n 或 limit n offset n统一改写成limit n
        sql = sql.rstrip(';').strip()
        if re.match(r"^select", sql, re.I):
            # LIMIT N
            limit_n = re.compile(r'limit([\s]*\d+[\s]*)$', re.I)
            # LIMIT N, N 或LIMIT N OFFSET N
            limit_offset = re.compile(r'limit([\s]*\d+[\s]*)(,|offset)([\s]*\d+[\s]*)$', re.I)
            if limit_n.search(sql):
                sql_limit = limit_n.search(sql).group(1)
                limit_num = min(int(limit_num), int(sql_limit))
                sql = limit_n.sub(f'limit {limit_num};', sql)
            elif limit_offset.search(sql):
                sql_limit = limit_offset.search(sql).group(3)
                limit_num = min(int(limit_num), int(sql_limit))
                sql = limit_offset.sub(f'limit {limit_num};', sql)
            else:
                sql = f'{sql} limit {limit_num};'
        else:
            sql = f'{sql};'
        return sql

    def query_masking(self, db_name='', sql='', resultset=None):
        """传入 sql语句, db名, 结果集,
        返回一个脱敏后的结果集"""
        # 仅对select语句脱敏
        if re.match(r"^select", sql, re.I):
            mask_result = data_masking(self.instance, db_name, sql, resultset)
        else:
            mask_result = resultset
        return mask_result

    def execute_check(self, db_name='', sql=''):
        """上线单执行前的检查, 返回Review set"""
        config = SysConfig()
        # 进行Inception检查，获取检测结果
        if not config.get('inception'):
            self.logger.info("SQL check via goinception")
            self.logger.info("Debug db_name in mysql.execute_check {0}".format(db_name))
            go_inception_engine = GoInceptionEngine()
            try:
                inc_check_result = go_inception_engine.execute_check(db_name=db_name, instance=self.instance, sql=sql)
            except Exception as e:
                self.logger.error("goInception检测语句报错：错误信息{}".format(traceback.format_exc()))
                raise RuntimeError(f"goInception检测语句报错，请注意检查系统配置中goInception配置，错误信息：\n{e}")
            else:
                self.logger.info('Debug sql check res for database {0}: {1}'.format(db_name, inc_check_result.to_dict()))
        else:
            self.logger.info("SQL check via inception")
            inception_engine = InceptionEngine()
            try:
                inc_check_result = inception_engine.execute_check(db_name=db_name, instance=self.instance,  sql=sql)
            except Exception as e:
                self.logger.info(f"Inception检测语句报错：错误信息{traceback.format_exc()}")
                raise RuntimeError(f"Inception检测语句报错，请注意检查系统配置中Inception配置，错误信息：\n{e}")
        # 判断Inception检测结果
        if inc_check_result.error:
            self.logger.info(f"Inception检测语句报错：错误信息{inc_check_result.error}")
            raise RuntimeError(f"Inception检测语句报错，错误信息：\n{inc_check_result.error}")

        # 禁用/高危语句检查
        check_critical_result = ReviewSet(full_sql=sql)
        line = 1
        critical_ddl_regex = config.get('critical_ddl_regex', '')
        p = re.compile(critical_ddl_regex)
        check_critical_result.syntax_type = 2  # TODO 工单类型 0、其他 1、DDL，2、DML

        for row in inc_check_result.rows:
            statement = row.sql
            # 去除注释
            statement = remove_comments(statement, db_type='mysql')
            # 禁用语句
            if re.match(r"^select", statement.lower()):
                check_critical_result.is_critical = True
                result = ReviewResult(id=line, errlevel=2,
                                      stagestatus='驳回不支持语句',
                                      errormessage='仅支持DML和DDL语句，查询语句请使用SQL查询功能！',
                                      sql=statement)
            # 高危语句
            elif critical_ddl_regex and p.match(statement.strip().lower()):
                check_critical_result.is_critical = True
                result = ReviewResult(id=line, errlevel=2,
                                      stagestatus='驳回高危SQL',
                                      errormessage='禁止提交匹配' + critical_ddl_regex + '条件的语句！',
                                      sql=statement)
            # 正常语句
            else:
                result = ReviewResult(id=line, errlevel=0,
                                      stagestatus='Audit completed',
                                      errormessage='None',
                                      sql=statement,
                                      affected_rows=0,
                                      execute_time=0, )

            # 没有找出DDL语句的才继续执行此判断
            if check_critical_result.syntax_type == 2:
                if get_syntax_type(statement, parser=False, db_type='mysql') == 'DDL':
                    check_critical_result.syntax_type = 1
            check_critical_result.rows += [result]

            # 遇到禁用和高危语句直接返回
            if check_critical_result.is_critical:
                check_critical_result.error_count += 1
                return check_critical_result
            line += 1
        return inc_check_result

    def execute_workflow(self, workflow):
        """执行上线单，返回Review set"""
        self.logger.info("Entering execute_workflow, start execute workflow {0} ".format(workflow))
        self.logger.info("Debug sql content {0}".format(workflow.sqlworkflowcontent.sql_content))

        # 判断实例是否只读
        db_names = workflow.db_names.split(',') if workflow.db_names else []
        read_only = self.query(db_name=db_names[0], sql='select @@read_only;').rows[0][0]
        if read_only:
            result = {}
            for db_name in db_names:
                result[db_name] = (
                    ReviewResult(
                        id=len(result) + 1,
                        errlevel=2,
                        stagestatus='Execute Failed',
                        errormessage='实例read_only=1，禁止执行变更语句!',
                        sql=workflow.sqlworkflowcontent.sql_content,
                        db_name=db_name
                    )
                )

            result.error = '实例read_only=1，禁止执行变更语句!'

            return result

        global execute_res
        execute_res = {}

        # 原生执行
        if workflow.is_manual == 1:
            self.logger.info('SQL execute via mysql client directly!')
            # 多线程执行SQL
            # multi_thread(self.execute, db_names, (workflow.sqlworkflowcontent.sql_content, True))
            # asyncio.run(self.async_execute(db_names, workflow.sqlworkflowcontent.sql_content, True))
            asyncio.run(async_tasks(self.execute, db_names, workflow.sqlworkflowcontent.sql_content, True))
            return execute_res
        # goinception执行
        elif not SysConfig().get('inception'):
            self.logger.info('SQL execute via goinception!')
            go_inception_engine = GoInceptionEngine()
            return go_inception_engine.execute(workflow)
        # inception执行
        else:
            self.logger.info('SQL execute via inception!')
            inception_engine = InceptionEngine()
            return inception_engine.execute(workflow)

    # async def async_execute(self, db_names, *args):
    #     tasks = [asyncio.create_task(self.execute(db_name, *args)) for db_name in db_names]
    #     await asyncio.gather(*tasks)

    async def execute(self, db_name=None, sql='', close_conn=False):
        """原生执行语句"""
        result = ResultSet(full_sql=sql)

        pool = self.get_connection(db_name=db_name)
        conn = pool.connection()
        try:
            cursor = conn.cursor()
            for statement in sqlparse.split(sql):
                cursor.execute(statement)
            conn.commit()
            cursor.close()
            conn.close()
        except Exception as e:
            self.logger.info(f"MySQL语句执行报错，语句：{sql}，错误信息{traceback.format_exc()}")
            result.error = str(e)
        finally:
            self.logger.info("Debug SQL execute result once execution has been done.{}".format(result.to_dict()))
        if close_conn:
            shutdown_conn(pool=self.pool)

        # 执行结果写入全局变量
        global execute_res
        execute_res[db_name] = result.to_dict()

    def get_rollback(self, workflow):
        """通过inception获取回滚语句列表"""
        inception_engine = InceptionEngine()
        return inception_engine.get_rollback(workflow)

    def get_variables(self, variables=None):
        """获取实例参数"""
        if variables:
            variables = "','".join(variables) if isinstance(variables, list) else "','".join(list(variables))
            db = 'performance_schema' if self.server_version > (5, 7) else 'information_schema'
            sql = f"""select * from {db}.global_variables where variable_name in ('{variables}');"""
        else:
            sql = "show global variables;"
        return self.query(sql=sql)

    def set_variable(self, variable_name, variable_value):
        """修改实例参数值"""
        sql = f"""set global {variable_name}={variable_value};"""
        return self.query(sql=sql)

    def osc_control(self, **kwargs):
        """控制osc执行，获取进度、终止、暂停、恢复等
            get、kill、pause、resume
        """
        if not SysConfig().get('inception'):
            go_inception_engine = GoInceptionEngine()
            return go_inception_engine.osc_control(**kwargs)
        else:
            inception_engine = InceptionEngine()
            return inception_engine.osc_control(**kwargs)


# -*- coding: UTF-8 -*-

import json

from common.utils.const import WorkflowDict
from common.utils.get_logger import get_logger
from sql.engines import get_engine
from sql.engines.models import ReviewResult, ReviewSet
from sql.models import SqlWorkflow
from sql.notify import notify_for_execute
from sql.utils.workflow_audit import Audit

logger = get_logger()


def execute(workflow_id):
    """为延时或异步任务准备的execute, 传入工单ID即可"""
    logger.debug("Entering execute func!")
    workflow_detail = SqlWorkflow.objects.get(id=workflow_id)

    # 给定时执行的工单增加执行日志
    if workflow_detail.status == 'workflow_timingtask':
        # 将工单状态修改为执行中
        SqlWorkflow(id=workflow_id, status='workflow_executing').save(update_fields=['status'])
        audit_id = Audit.detail_by_workflow_id(workflow_id=workflow_id,
                                               workflow_type=WorkflowDict.workflow_type['sqlreview']).audit_id
        Audit.add_log(audit_id=audit_id,
                      operation_type=5,
                      operation_type_desc='执行工单',
                      operation_info='系统定时执行',
                      operator='',
                      operator_display='系统'
                      )

    execute_engine = get_engine(instance=workflow_detail.instance)
    return execute_engine.execute_workflow(workflow=workflow_detail)


def execute_callback(task):
    """异步任务的回调, 将结果填入数据库等等
    使用django-q的hook, 传入参数为整个task
    task.result 是真正的结果
    """
    workflow_id = task.args[0]
    workflow = SqlWorkflow.objects.get(id=workflow_id)
    workflow.finish_time = task.stopped

    logger.info("Debug task result in callback {0}".format(task.result))

    task_res = task.result
    execute_result = []
    result_error = []

    error_level = 0
    for database, exe_results in task_res.items():
        logger.debug("Debug SQL execute task result for database {0}".format(database))
        logger.debug("Debug result {0}, {1}".format(type(exe_results), exe_results))
        for exe_result in exe_results:
            res_error = exe_result["errormessage"]
            err_level = int(exe_result["errlevel"])
            if err_level > error_level: error_level = err_level
            if res_error:
                logger.error("Execute sql error:{0}".format(res_error))
                result_error.append(res_error)

        execute_result.extend(exe_results)

    if not task.success:
        # 不成功会返回错误堆栈信息，构造一个错误信息
        workflow.status = 'workflow_exception'
        execute_result = ReviewSet(full_sql=workflow.sqlworkflowcontent.sql_content)
        execute_result.rows = [ReviewResult(
            stage='Execute failed',
            errlevel=2,
            stagestatus='异常终止',
            errormessage=task.result,
            sql=workflow.sqlworkflowcontent.sql_content)]
    elif result_error and error_level == 2:
        execute_result = task.result
        workflow.status = 'workflow_exception'
    else:
        workflow.status = 'workflow_finish'
    # 保存执行结果
    logger.info("Final execute result save to mysql {0}".format(execute_result))
    execute_result = json.dumps(execute_result)
    workflow.sqlworkflowcontent.execute_result = execute_result
    workflow.sqlworkflowcontent.save()
    workflow.save()

    # 增加工单日志
    audit_id = Audit.detail_by_workflow_id(workflow_id=workflow_id,
                                           workflow_type=WorkflowDict.workflow_type['sqlreview']).audit_id
    Audit.add_log(audit_id=audit_id,
                  operation_type=6,
                  operation_type_desc='执行结束',
                  operation_info='执行结果：{}'.format(workflow.get_status_display()),
                  operator='',
                  operator_display='系统'
                  )

    # 发送消息
    notify_for_execute(workflow)

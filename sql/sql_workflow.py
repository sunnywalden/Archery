# -*- coding: UTF-8 -*-

import asyncio
import datetime
import traceback
import re

import simplejson as json
from django.contrib.auth.decorators import permission_required
from django.core.exceptions import PermissionDenied
from django.db import transaction
from django.db.models import Q
from django.http import HttpResponse, HttpResponseRedirect, JsonResponse
from django.shortcuts import render, get_object_or_404
from django.urls import reverse
from django.utils import timezone
from django_q.tasks import async_task

from common.config import SysConfig
from common.utils.const import Const, WorkflowDict
from common.utils.extend_json_encoder import ExtendJSONEncoder
from common.utils.get_logger import get_logger
from sql.engines import get_engine
from sql.models import ResourceGroup
from sql.notify import notify_for_audit
from sql.utils.async_tasks import async_tasks
from sql.utils.resource_group import user_groups, user_instances
from sql.utils.sql_review import can_timingtask, can_cancel, can_execute, on_correct_time_period
from sql.utils.tasks import add_sql_schedule, del_schedule
from sql.utils.workflow_audit import Audit
from .models import SqlWorkflow, SqlWorkflowContent, Instance

logger = get_logger()


@permission_required('sql.menu_sqlworkflow', raise_exception=True)
def sql_workflow_list(request):
    """
    获取审核列表
    :param request:
    :return:
    """
    nav_status = request.POST.get('navStatus')
    instance_id = request.POST.get('instance_id')
    resource_group_id = request.POST.get('group_id')
    start_date = request.POST.get('start_date')
    end_date = request.POST.get('end_date')
    limit = int(request.POST.get('limit'))
    offset = int(request.POST.get('offset'))
    limit = offset + limit
    search = request.POST.get('search')
    user = request.user

    # 组合筛选项
    filter_dict = dict()
    # 工单状态
    if nav_status:
        filter_dict['status'] = nav_status
    # 实例
    if instance_id:
        filter_dict['instance_id'] = instance_id
    # 资源组
    if resource_group_id:
        filter_dict['group_id'] = resource_group_id
    # 时间
    if start_date and end_date:
        end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d') + datetime.timedelta(days=1)
        filter_dict['create_time__range'] = (start_date, end_date)
    # 管理员，可查看所有工单
    if user.is_superuser:
        pass
    # 非管理员，拥有审核权限、资源组粒度执行权限的，可以查看组内所有工单
    elif user.has_perm('sql.sql_review') or user.has_perm('sql.sql_execute_for_resource_group'):
        # 先获取用户所在资源组列表
        group_list = user_groups(user)
        group_ids = [group.group_id for group in group_list]
        filter_dict['group_id__in'] = group_ids
    # 其他人只能查看自己提交的工单
    else:
        filter_dict['engineer'] = user.username

    # 过滤组合筛选项
    workflow = SqlWorkflow.objects.filter(**filter_dict)

    # 过滤搜索项，模糊检索项包括提交人名称、工单名
    if search:
        workflow = workflow.filter(Q(engineer_display__icontains=search) | Q(workflow_name__icontains=search))

    count = workflow.count()
    workflow_list = workflow.order_by('-create_time')[offset:limit].values(
        "id", "workflow_name", "engineer_display",
        "status", "is_backup", "create_time",
        "instance__instance_name", "db_names",
        "group_name", "syntax_type")

    # QuerySet 序列化
    rows = [row for row in workflow_list]
    result = {"total": count, "rows": rows}
    # 返回查询结果
    return HttpResponse(json.dumps(result, cls=ExtendJSONEncoder, bigint_as_string=True),
                        content_type='application/json')


async def sql_check(db_name, instance, sql_content):
    """SQL检测"""

    logger.debug("Debug db_name in custom sql_check {0}".format(db_name))
    result = {}
    # 交给engine进行检测
    check_engine = get_engine(instance=instance)
    # 替换sql语句中双引号为单引号，规避json转换异常问题
    sql_content = re.sub('"(\w.+)"', "'\\1'", sql_content.strip())
    try:
        check_result = check_engine.execute_check(db_name=db_name, sql=sql_content)
    except Exception as e:
        result['status'] = 1
        result['msg'] = str(e)
        logger.error('Sql check error {0}'.format(e))
        return HttpResponse(json.dumps(result), content_type='application/json')
    else:
        logger.debug('Debug sql check result for {0}: {1}'.format(db_name, check_result.to_dict()))

    # 检测结果写入全局变量
    global all_check_res
    all_check_res['data']['rows'].extend(check_result.to_dict())
    all_check_res['data']['CheckWarningCount'] += check_result.warning_count
    all_check_res['data']['CheckErrorCount'] += check_result.error_count


@permission_required('sql.sql_submit', raise_exception=True)
def check(request):
    """SQL检测按钮, 此处没有产生工单"""
    sql_content = request.POST.get('sql_content')
    instance_name = request.POST.get('instance_name')
    instance = Instance.objects.get(instance_name=instance_name)
    db_names = request.POST.get('db_names', default='')
    db_names = db_names.split(',') if db_names else []

    logger.debug("Debug db_names in simplecheck api {0}".format(db_names))
    global all_check_res
    all_check_res = {'status': 0, 'msg': 'ok', 'data': {"rows": [], "CheckWarningCount": 0, "CheckErrorCount": 0}}

    # 服务器端参数验证
    if sql_content is None or instance_name is None or db_names is None:
        all_check_res['status'] = 1
        all_check_res['msg'] = '页面提交参数可能为空'
        return HttpResponse(json.dumps(all_check_res), content_type='application/json')

    # 异步执行
    asyncio.run(async_tasks(sql_check, db_names, instance, sql_content))

    return HttpResponse(json.dumps(all_check_res), content_type='application/json')


def check_backup(instance):
    """检查备份设置"""
    is_backup = False
    check_engine = get_engine(instance=instance)
    # 未开启备份选项，并且engine支持备份，强制设置备份
    sys_config = SysConfig()
    if not sys_config.get('enable_backup_switch') and check_engine.auto_backup:
        is_backup = True

    return is_backup


def workflow_check(db_name, instance, sql_content):
    """工单审核"""
    # 再次交给engine进行检测，防止绕过
    logger.debug('Debug db_name in workflow_check {0}'.format(db_name))
    print('Check database {} for SQL {}'.format(db_name, sql_content))
    logger.debug("Debug sql content {}: {}".format(type(sql_content), sql_content))
    check_engine = get_engine(instance=instance)
    try:
        check_result = check_engine.execute_check(db_name=db_name, sql=sql_content.strip())
    except Exception as e:
        logger.error("Error catched while execute sql for database {}: {}".format(db_name, str(e)))
        return False, str(e)

    # 按照系统配置确定是自动驳回还是放行
    sys_config = SysConfig()
    auto_review_wrong = sys_config.get('auto_review_wrong', '')  # 1表示出现警告就驳回，2和空表示出现错误才驳回
    workflow_status = 'workflow_manreviewing'
    if check_result.warning_count > 0 and auto_review_wrong == '1':
        workflow_status = 'workflow_autoreviewwrong'
    elif check_result.error_count > 0 and auto_review_wrong in ('', '1', '2'):
        workflow_status = 'workflow_autoreviewwrong'
    else:
        pass

    logger.debug("Debug sql review result in workflow_check {}".format(check_result.to_dict()))
    print("Debug sql review result in workflow_check {}".format(check_result))

    return check_result, workflow_status


def sql_submit(db_names, request, instance, sql_content, workflow_title, group_id, group_name, cc_users,
               run_date_start, run_date_end):
    """提交SQL工单"""

    logger.debug('Debug db_names in sql_submit {0}'.format(db_names))
    is_backup = check_backup(instance)
    check_result, workflow_status = {}, {}
    for db_name in db_names:
        check_result[db_name], workflow_status[db_name] = workflow_check(db_name, instance, sql_content)
        if check_result[db_name] is False:
            logger.error(workflow_status[db_name])
            context = {'errMsg': workflow_status[db_name]}
            return context

    workflow_status = 'workflow_autoreviewwrong' \
        if 'workflow_autoreviewwrong' in workflow_status.values() \
        else 'workflow_manreviewing'

    if db_names:
        tenant_first = db_names[0]
        syntax_type = check_result[tenant_first].syntax_type
    else:
        syntax_type = check_result.syntax_type

    # 获取对象的值
    check_result = {k: v.to_dict() for k, v in check_result.items()}
    logger.debug('SQL check result {}'.format(check_result))

    # 调用工作流生成工单
    # 使用事务保持数据一致性
    try:
        logger.debug('Start running sql for tenant {}'.format(db_names))
        with transaction.atomic():
            # 存进数据库里
            sql_workflow = SqlWorkflow.objects.create(
                workflow_name=workflow_title,
                group_id=group_id,
                group_name=group_name,
                engineer=request.user.username,
                engineer_display=request.user.display,
                audit_auth_groups=Audit.settings(group_id, WorkflowDict.workflow_type['sqlreview']),
                status=workflow_status,
                is_backup=is_backup,
                instance=instance,
                db_names=','.join(db_names) if db_names else '',
                is_manual=0,
                syntax_type=syntax_type,
                create_time=timezone.now(),
                run_date_start=run_date_start or None,
                run_date_end=run_date_end or None
            )
            SqlWorkflowContent.objects.create(workflow=sql_workflow,
                                              sql_content=sql_content,
                                              review_content=json.dumps(check_result),
                                              execute_result=''
                                              )
        workflow_id = sql_workflow.id
        # 自动审核通过了，才调用工作流
        if workflow_status == 'workflow_manreviewing':
            # 调用工作流插入审核信息, 查询权限申请workflow_type=2
            Audit.add(WorkflowDict.workflow_type['sqlreview'], workflow_id)
    except Exception as msg:
        logger.error(f"提交工单报错，错误信息：{traceback.format_exc()}")
        context = {'errMsg': msg}
        return context
    else:
        # 自动审核通过才进行消息通知
        if workflow_status == 'workflow_manreviewing':
            # 获取审核信息
            audit_id = Audit.detail_by_workflow_id(workflow_id=workflow_id,
                                                   workflow_type=WorkflowDict.workflow_type['sqlreview']).audit_id
            async_task(notify_for_audit, audit_id=audit_id, cc_users=cc_users, timeout=60)

        # 结果写入全局变量
        return workflow_id


@permission_required('sql.sql_submit', raise_exception=True)
def submit(request):
    """正式提交SQL, 此处生成工单"""
    sql_content = request.POST.get('sql_content').strip()
    workflow_title = request.POST.get('workflow_name')
    # 检查用户是否有权限涉及到资源组等， 比较复杂， 可以把检查权限改成一个独立的方法
    group_name = request.POST.get('group_name')
    group_id = ResourceGroup.objects.get(group_name=group_name).group_id
    instance_name = request.POST.get('instance_name')
    instance = Instance.objects.get(instance_name=instance_name)
    db_names = request.POST.get('db_names', default='')
    is_backup = True if request.POST.get('is_backup') == 'True' else False
    cc_users = request.POST.getlist('cc_users')
    run_date_start = request.POST.get('run_date_start')
    run_date_end = request.POST.get('run_date_end')

    db_names = db_names.split(',') if db_names else []

    logger.debug("Debug db_names in execute {0}".format(db_names))

    # 服务器端参数验证
    if None in [sql_content, db_names, instance_name, db_names, is_backup]:
        context = {'errMsg': '页面提交参数可能为空'}
        return render(request, 'error.html', context)

    # 验证组权限（用户是否在该组、该组是否有指定实例）
    try:
        user_instances(request.user, tag_codes=['can_write']).get(instance_name=instance_name)
    except instance.DoesNotExist:
        context = {'errMsg': '你所在组未关联该实例！'}
        return render(request, 'error.html', context)

    # 替换sql语句中双引号为单引号，规避json转换异常问题
    sql_content = re.sub('"(\w.+)"', "'\\1'", sql_content)

    workflow_id = sql_submit(db_names, request, instance, sql_content, workflow_title, group_id,
                             group_name, cc_users, run_date_start, run_date_end)

    if not isinstance(workflow_id, int):
        logger.error('Got error while audit workflow {}'.format(workflow_id))
        return render(request, 'error.html', workflow_id)
    else:
        all_check_res = {'status': 0, 'msg': 'ok', 'data': workflow_id}

    print('All SQL check result: {}'.format(all_check_res))
    return HttpResponse(json.dumps(all_check_res), content_type='application/json')


@permission_required('sql.sql_review', raise_exception=True)
def alter_run_date(request):
    """
    审核人修改可执行时间
    :param request:
    :return:
    """
    workflow_id = int(request.POST.get('workflow_id', 0))
    run_date_start = request.POST.get('run_date_start')
    run_date_end = request.POST.get('run_date_end')
    if workflow_id == 0:
        context = {'errMsg': 'workflow_id参数为空.'}
        return render(request, 'error.html', context)

    user = request.user
    if Audit.can_review(user, workflow_id, 2) is False:
        context = {'errMsg': '你无权操作当前工单！'}
        return render(request, 'error.html', context)

    try:
        # 存进数据库里
        SqlWorkflow(id=workflow_id,
                    run_date_start=run_date_start or None,
                    run_date_end=run_date_end or None
                    ).save(update_fields=['run_date_start', 'run_date_end'])
    except Exception as msg:
        context = {'errMsg': msg}
        return render(request, 'error.html', context)

    return HttpResponseRedirect(reverse('sql:detail', args=(workflow_id,)))


@permission_required('sql.sql_review', raise_exception=True)
def passed(request):
    """
    审核通过，不执行
    :param request:
    :return:
    """
    workflow_id = int(request.POST.get('workflow_id', 0))
    audit_remark = request.POST.get('audit_remark', '')
    if workflow_id == 0:
        context = {'errMsg': 'workflow_id参数为空.'}
        return render(request, 'error.html', context)

    user = request.user
    if Audit.can_review(user, workflow_id, 2) is False:
        context = {'errMsg': '你无权操作当前工单！'}
        return render(request, 'error.html', context)

    # 使用事务保持数据一致性
    try:
        with transaction.atomic():
            # 调用工作流接口审核
            audit_id = Audit.detail_by_workflow_id(workflow_id=workflow_id,
                                                   workflow_type=WorkflowDict.workflow_type['sqlreview']).audit_id
            audit_result = Audit.audit(audit_id, WorkflowDict.workflow_status['audit_success'],
                                       user.username, audit_remark)

            # 按照审核结果更新业务表审核状态
            if audit_result['data']['workflow_status'] == WorkflowDict.workflow_status['audit_success']:
                # 将流程状态修改为审核通过
                SqlWorkflow(id=workflow_id, status='workflow_review_pass').save(update_fields=['status'])
    except Exception as msg:
        logger.error(f"审核工单报错，错误信息：{traceback.format_exc()}")
        context = {'errMsg': msg}
        return render(request, 'error.html', context)
    else:
        # 消息通知
        async_task(notify_for_audit, audit_id=audit_id, audit_remark=audit_remark, timeout=60)

    return HttpResponseRedirect(reverse('sql:detail', args=(workflow_id,)))


def perm_check(request, workflow_id):
    """
    SQL上线权限校验
    :param request:
    :param workflow_id:
    :return:
    """
    if not (request.user.has_perm('sql.sql_execute') or request.user.has_perm('sql.sql_execute_for_resource_group')):
        raise PermissionDenied

    if workflow_id == 0:
        context = {'errMsg': 'workflow_id参数为空.'}
        return render(request, 'error.html', context)

    if can_execute(request.user, workflow_id) is False:
        context = {'errMsg': '你无权操作当前工单！'}
        return render(request, 'error.html', context)

    if on_correct_time_period(workflow_id) is False:
        context = {'errMsg': '不在可执行时间范围内，如果需要修改执行时间请重新提交工单!'}
        return render(request, 'error.html', context)


def get_operation_info(mode):
    """
    根据执行模式更新工单审计信息
    :param mode: str
    :return:
    """
    # 根据执行模式进行对应修改
    if mode == "auto":
        status = "workflow_executing"
        operation_type = 5
        operation_type_desc = '执行工单'
        operation_info = "自动操作执行"
        finish_time = None
    else:
        status = "workflow_finish"
        operation_type = 6
        operation_type_desc = '手工工单'
        operation_info = "确认手工执行结束"
        finish_time = datetime.datetime.now()

    return status, operation_type, operation_type_desc, operation_info, finish_time


def execute(request):
    """
    执行SQL
    :param request:
    :return:
    """

    workflow_id = int(request.POST.get('workflow_id', 0))
    mode = request.POST.get('mode')

    # 校验多个权限
    perm_check(request, workflow_id)

    # 根据执行模式修改
    status, operation_type, operation_type_desc, operation_info, finish_time = get_operation_info(mode)

    # 将流程状态修改为对应状态
    SqlWorkflow(id=workflow_id, status=status, finish_time=finish_time).save(update_fields=['status', 'finish_time'])

    # 增加工单日志
    audit_id = Audit.detail_by_workflow_id(workflow_id=workflow_id,
                                           workflow_type=WorkflowDict.workflow_type['sqlreview']).audit_id
    Audit.add_log(audit_id=audit_id,
                  operation_type=operation_type,
                  operation_type_desc=operation_type_desc,
                  operation_info=operation_info,
                  operator=request.user.username,
                  operator_display=request.user.display
                  )

    if mode == "auto":
        # 加入执行队列
        async_task('sql.utils.execute_sql.execute', workflow_id,
                   hook='sql.utils.execute_sql.execute_callback', timeout=-1)

    return HttpResponseRedirect(reverse('sql:detail', args=(workflow_id,)))


def timing_task(request):
    """
    定时执行SQL
    :param request:
    :return:
    """
    # 校验多个权限
    if not (request.user.has_perm('sql.sql_execute') or request.user.has_perm('sql.sql_execute_for_resource_group')):
        raise PermissionDenied
    workflow_id = request.POST.get('workflow_id')
    run_date = request.POST.get('run_date')
    if run_date is None or workflow_id is None:
        context = {'errMsg': '时间不能为空'}
        return render(request, 'error.html', context)
    elif run_date < datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'):
        context = {'errMsg': '时间不能小于当前时间'}
        return render(request, 'error.html', context)
    workflow_detail = SqlWorkflow.objects.get(id=workflow_id)

    if can_timingtask(request.user, workflow_id) is False:
        context = {'errMsg': '你无权操作当前工单！'}
        return render(request, 'error.html', context)

    run_date = datetime.datetime.strptime(run_date, "%Y-%m-%d %H:%M")
    task_name = f"{Const.workflowJobprefix['sqlreview']}-{workflow_id}"

    if on_correct_time_period(workflow_id, run_date) is False:
        context = {'errMsg': '不在可执行时间范围内，如果需要修改执行时间请重新提交工单!'}
        return render(request, 'error.html', context)

    # 使用事务保持数据一致性
    try:
        with transaction.atomic():
            # 将流程状态修改为定时执行
            workflow_detail.status = 'workflow_timingtask'
            workflow_detail.save()
            # 调用添加定时任务
            add_sql_schedule(task_name, run_date, workflow_id)
            # 增加工单日志
            audit_id = Audit.detail_by_workflow_id(workflow_id=workflow_id,
                                                   workflow_type=WorkflowDict.workflow_type[
                                                       'sqlreview']).audit_id
            Audit.add_log(audit_id=audit_id,
                          operation_type=4,
                          operation_type_desc='定时执行',
                          operation_info="定时执行时间：{}".format(run_date),
                          operator=request.user.username,
                          operator_display=request.user.display
                          )
    except Exception as msg:
        logger.error(f"定时执行工单报错，错误信息：{traceback.format_exc()}")
        context = {'errMsg': msg}
        return render(request, 'error.html', context)
    return HttpResponseRedirect(reverse('sql:detail', args=(workflow_id,)))


def cancel(request):
    """
    终止流程
    :param request:
    :return:
    """
    workflow_id = int(request.POST.get('workflow_id', 0))
    if workflow_id == 0:
        context = {'errMsg': 'workflow_id参数为空.'}
        return render(request, 'error.html', context)
    workflow_detail = SqlWorkflow.objects.get(id=workflow_id)
    audit_remark = request.POST.get('cancel_remark')
    if audit_remark is None:
        context = {'errMsg': '终止原因不能为空'}
        return render(request, 'error.html', context)

    user = request.user
    if can_cancel(request.user, workflow_id) is False:
        context = {'errMsg': '你无权操作当前工单！'}
        return render(request, 'error.html', context)

    # 使用事务保持数据一致性
    try:
        with transaction.atomic():
            # 调用工作流接口取消或者驳回
            audit_id = Audit.detail_by_workflow_id(workflow_id=workflow_id,
                                                   workflow_type=WorkflowDict.workflow_type[
                                                       'sqlreview']).audit_id
            # 仅待审核的需要调用工作流，审核通过的不需要
            if workflow_detail.status != 'workflow_manreviewing':
                # 增加工单日志
                if user.username == workflow_detail.engineer:
                    Audit.add_log(audit_id=audit_id,
                                  operation_type=3,
                                  operation_type_desc='取消执行',
                                  operation_info="取消原因：{}".format(audit_remark),
                                  operator=request.user.username,
                                  operator_display=request.user.display
                                  )
                else:
                    Audit.add_log(audit_id=audit_id,
                                  operation_type=2,
                                  operation_type_desc='审批不通过',
                                  operation_info="审批备注：{}".format(audit_remark),
                                  operator=request.user.username,
                                  operator_display=request.user.display
                                  )
            else:
                if user.username == workflow_detail.engineer:
                    Audit.audit(audit_id,
                                WorkflowDict.workflow_status['audit_abort'],
                                user.username, audit_remark)
                # 非提交人需要校验审核权限
                elif user.has_perm('sql.sql_review'):
                    Audit.audit(audit_id,
                                WorkflowDict.workflow_status['audit_reject'],
                                user.username, audit_remark)
                else:
                    raise PermissionDenied

            # 删除定时执行task
            if workflow_detail.status == 'workflow_timingtask':
                task_name = f"{Const.workflowJobprefix['sqlreview']}-{workflow_id}"
                del_schedule(task_name)
            # 将流程状态修改为人工终止流程
            workflow_detail.status = 'workflow_abort'
            workflow_detail.save()
    except Exception as msg:
        logger.error(f"取消工单报错，错误信息：{traceback.format_exc()}")
        context = {'errMsg': msg}
        return render(request, 'error.html', context)
    else:
        # 仅未审核通过又取消的工单需要发送消息，审核通过的不发送
        audit_detail = Audit.detail_by_workflow_id(workflow_id=workflow_id,
                                                   workflow_type=WorkflowDict.workflow_type['sqlreview'])
        if audit_detail.current_status == WorkflowDict.workflow_status['audit_abort']:
            async_task(notify_for_audit, audit_id=audit_detail.audit_id, audit_remark=audit_remark, timeout=60)
    return HttpResponseRedirect(reverse('sql:detail', args=(workflow_id,)))


def get_workflow_status(request):
    """
    获取某个工单的当前状态
    """
    workflow_id = request.POST['workflow_id']
    if workflow_id == '' or workflow_id is None:
        context = {"status": -1, 'msg': 'workflow_id参数为空.', "data": ""}
        return HttpResponse(json.dumps(context), content_type='application/json')

    workflow_id = int(workflow_id)
    workflow_detail = get_object_or_404(SqlWorkflow, pk=workflow_id)
    result = {"status": workflow_detail.status, "msg": "", "data": ""}
    return JsonResponse(result)


def osc_control(request):
    """用于mysql控制osc执行"""
    workflow_id = request.POST.get('workflow_id')
    sqlsha1 = request.POST.get('sqlsha1')
    command = request.POST.get('command')
    workflow = SqlWorkflow.objects.get(id=workflow_id)
    execute_engine = get_engine(workflow.instance)
    try:
        execute_result = execute_engine.osc_control(command=command, sqlsha1=sqlsha1)
        rows = execute_result.to_dict()
        error = execute_result.error
    except Exception as e:
        rows = []
        error = str(e)
    result = {"total": len(rows), "rows": rows, "msg": error}
    return HttpResponse(json.dumps(result, cls=ExtendJSONEncoder, bigint_as_string=True),
                        content_type='application/json')

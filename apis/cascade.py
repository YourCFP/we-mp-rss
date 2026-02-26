"""级联系统API接口"""

from fastapi import APIRouter, Depends, HTTPException, status, Query, Body, Request
from typing import Optional, List
from pydantic import BaseModel
from datetime import datetime
from core.auth import get_current_user_or_ak
from core.db import DB
from core.cascade import cascade_manager, CascadeClient
from core.models.cascade_node import CascadeNode, CascadeSyncLog
from core.models.feed import Feed
from core.models.message_task import MessageTask
from .base import success_response, error_response
from core.print import print_info, print_success, print_error
import json

router = APIRouter(prefix="/cascade", tags=["级联管理"])


# ===== 请求模型 =====

class CreateNodeRequest(BaseModel):
    """创建节点请求"""
    node_type: int  # 0=父节点, 1=子节点
    name: str
    description: Optional[str] = ""
    api_url: Optional[str] = None  # 子节点配置父节点地址时使用


class UpdateNodeRequest(BaseModel):
    """更新节点请求"""
    name: Optional[str] = None
    description: Optional[str] = None
    api_url: Optional[str] = None
    is_active: Optional[bool] = None
    sync_config: Optional[dict] = None


class NodeCredentialRequest(BaseModel):
    """获取节点凭证请求"""
    node_id: str


class TestConnectionRequest(BaseModel):
    """测试连接请求"""
    api_url: str
    api_key: str
    api_secret: str


class SyncDataRequest(BaseModel):
    """同步数据请求"""
    node_id: str
    data_type: str  # feeds, tasks, all


class ReportResultRequest(BaseModel):
    """上报任务结果请求"""
    task_id: str
    results: List[dict]
    timestamp: str


# ===== 节点管理接口 =====

@router.post("/nodes", summary="创建级联节点")
async def create_node(
    req: CreateNodeRequest,
    current_user: dict = Depends(get_current_user_or_ak)
):
    """
    创建级联节点
    
    - node_type=0: 父节点 (本节点)
    - node_type=1: 子节点 (需要连接到父节点)
    """
    session = DB.get_session()
    try:
        node = cascade_manager.create_node(
            node_type=req.node_type,
            name=req.name,
            description=req.description,
            api_url=req.api_url
        )
        
        return success_response(
            {
                "node_id": node.id,
                "node_type": node.node_type,
                "name": node.name,
                "is_active": node.is_active,
                "created_at": node.created_at.isoformat()
            },
            "节点创建成功"
        )
    except Exception as e:
        return error_response(code=500, message=str(e))


@router.get("/nodes", summary="获取节点列表")
async def list_nodes(
    node_type: Optional[int] = Query(None),
    current_user: dict = Depends(get_current_user_or_ak)
):
    """
    获取级联节点列表
    
    参数:
        node_type: 可选，按节点类型筛选 (0=父节点, 1=子节点)
    """
    session = DB.get_session()
    try:
        query = session.query(CascadeNode)
        
        if node_type is not None:
            query = query.filter(CascadeNode.node_type == node_type)
        
        nodes = query.all()
        
        # 返回时不暴露api_secret_hash
        node_list = []
        for node in nodes:
            node_data = {
                "id": node.id,
                "node_type": node.node_type,
                "name": node.name,
                "description": node.description,
                "api_url": node.api_url,
                "api_key": node.api_key,
                "parent_id": node.parent_id,
                "status": node.status,
                "is_active": node.is_active,
                "last_sync_at": node.last_sync_at.isoformat() if node.last_sync_at else None,
                "last_heartbeat_at": node.last_heartbeat_at.isoformat() if node.last_heartbeat_at else None,
                "created_at": node.created_at.isoformat(),
                "updated_at": node.updated_at.isoformat()
            }
            
            if node.sync_config:
                try:
                    node_data["sync_config"] = json.loads(node.sync_config)
                except:
                    node_data["sync_config"] = {}
            
            node_list.append(node_data)
        
        return success_response(node_list)
    except Exception as e:
        return error_response(code=500, message=str(e))


@router.get("/nodes/{node_id}", summary="获取节点详情")
async def get_node(
    node_id: str,
    current_user: dict = Depends(get_current_user_or_ak)
):
    session = DB.get_session()
    try:
        node = session.query(CascadeNode).filter(CascadeNode.id == node_id).first()
        
        if not node:
            raise HTTPException(status_code=404, detail="节点不存在")
        
        return success_response(node)
    except HTTPException:
        raise
    except Exception as e:
        return error_response(code=500, message=str(e))


@router.put("/nodes/{node_id}", summary="更新节点")
async def update_node(
    node_id: str,
    req: UpdateNodeRequest,
    current_user: dict = Depends(get_current_user_or_ak)
):
    session = DB.get_session()
    try:
        node = session.query(CascadeNode).filter(CascadeNode.id == node_id).first()
        
        if not node:
            raise HTTPException(status_code=404, detail="节点不存在")
        
        if req.name is not None:
            node.name = req.name
        if req.description is not None:
            node.description = req.description
        if req.api_url is not None:
            node.api_url = req.api_url
        if req.is_active is not None:
            node.is_active = req.is_active
        if req.sync_config is not None:
            node.sync_config = json.dumps(req.sync_config)
        
        node.updated_at = datetime.utcnow()
        session.commit()
        
        return success_response(message="节点更新成功")
    except HTTPException:
        raise
    except Exception as e:
        session.rollback()
        return error_response(code=500, message=str(e))


@router.delete("/nodes/{node_id}", summary="删除节点")
async def delete_node(
    node_id: str,
    current_user: dict = Depends(get_current_user_or_ak)
):
    session = DB.get_session()
    try:
        node = session.query(CascadeNode).filter(CascadeNode.id == node_id).first()
        
        if not node:
            raise HTTPException(status_code=404, detail="节点不存在")
        
        session.delete(node)
        session.commit()
        
        return success_response(message="节点删除成功")
    except HTTPException:
        raise
    except Exception as e:
        session.rollback()
        return error_response(code=500, message=str(e))


@router.post("/nodes/{node_id}/credentials", summary="生成节点凭证")
async def generate_node_credentials(
    node_id: str,
    current_user: dict = Depends(get_current_user_or_ak)
):
    """
    为子节点生成连接父节点的凭证 (AK/SK)
    仅返回一次，请妥善保存
    """
    try:
        credentials = cascade_manager.generate_node_credentials(node_id)
        return success_response(credentials, "凭证生成成功")
    except HTTPException:
        raise
    except Exception as e:
        return error_response(code=500, message=str(e))


@router.post("/nodes/{node_id}/test-connection", summary="测试节点连接")
async def test_node_connection(
    node_id: str,
    req: TestConnectionRequest = None,
    current_user: dict = Depends(get_current_user_or_ak)
):
    """
    测试子节点到父节点的连接
    
    如果提供req参数，使用提供的凭证测试
    否则使用节点配置中的凭证
    """
    session = DB.get_session()
    try:
        node = session.query(CascadeNode).filter(CascadeNode.id == node_id).first()
        
        if not node:
            raise HTTPException(status_code=404, detail="节点不存在")
        
        if req:
            api_url = req.api_url
            api_key = req.api_key
            api_secret = req.api_secret
        else:
            api_url = node.api_url
            api_key = node.api_key
            api_secret = ""  # 无法获取原始secret
        
        # 创建客户端并测试连接
        client = CascadeClient(api_url, api_key, api_secret)
        result = await client.send_heartbeat()
        
        return success_response(
            {"connected": True, "parent_response": result},
            "连接测试成功"
        )
        
    except Exception as e:
        print_error(f"连接测试失败: {str(e)}")
        return success_response(
            {"connected": False, "error": str(e)},
            "连接测试失败"
        )


# ===== 数据同步接口（子节点调用）=====

@router.get("/feeds", summary="获取父节点公众号数据")
async def get_feeds(
    request: Request,
    current_user: dict = Depends(get_current_user_or_ak)
):
    """
    子节点从父节点拉取公众号数据
    
    需要级联认证
    """
    session = DB.get_session()
    try:
        feeds = session.query(Feed).all()
        
        feed_list = []
        for feed in feeds:
            feed_list.append({
                "id": feed.id,
                "faker_id": feed.faker_id,
                "mp_name": feed.mp_name,
                "mp_cover": feed.mp_cover,
                "mp_intro": feed.mp_intro,
                "status": feed.status,
                "created_at": feed.created_at.isoformat() if feed.created_at else None,
                "updated_at": feed.updated_at.isoformat() if feed.updated_at else None
            })
        
        return success_response(feed_list)
    except Exception as e:
        return error_response(code=500, message=str(e))


@router.get("/message-tasks", summary="获取父节点消息任务")
async def get_message_tasks(
    request: Request,
    current_user: dict = Depends(get_current_user_or_ak)
):
    """
    子节点从父节点拉取消息任务
    
    需要级联认证
    """
    session = DB.get_session()
    try:
        tasks = session.query(MessageTask).filter(
            MessageTask.status == 0  # 只返回启用状态的任务
        ).all()
        
        task_list = []
        for task in tasks:
            task_list.append({
                "id": task.id,
                "name": task.name,
                "message_type": task.message_type,
                "message_template": task.message_template,
                "web_hook_url": task.web_hook_url,
                "mps_id": task.mps_id,
                "cron_exp": task.cron_exp,
                "status": task.status,
                "headers": task.headers,
                "cookies": task.cookies,
                "created_at": task.created_at.isoformat() if task.created_at else None,
                "updated_at": task.updated_at.isoformat() if task.updated_at else None
            })
        
        return success_response(task_list)
    except Exception as e:
        return error_response(code=500, message=str(e))


@router.post("/report-result", summary="上报任务执行结果")
async def report_task_result(
    req: ReportResultRequest,
    request: Request,
    current_user: dict = Depends(get_current_user_or_ak)
):
    """
    子节点向父节点上报任务执行结果
    
    需要级联认证
    """
    session = DB.get_session()
    try:
        # TODO: 实现结果上报逻辑
        # 可以将结果存储到单独的结果表或日志表中
        
        print_info(f"收到任务结果上报: task_id={req.task_id}, results数量={len(req.results)}")
        
        # 创建同步日志
        from core.cascade import cascade_manager
        node_id = current_user.get("node_id", "unknown")
        log = cascade_manager.create_sync_log(
            node_id=node_id,
            operation="report_result",
            direction="push",
            extra_data={
                "task_id": req.task_id,
                "result_count": len(req.results)
            }
        )
        
        if log:
            cascade_manager.update_sync_log(log.id, status=1, data_count=len(req.results))
        
        return success_response(message="结果上报成功")
    except Exception as e:
        return error_response(code=500, message=str(e))


@router.post("/heartbeat", summary="心跳接口")
async def heartbeat(
    request: Request,
    current_user: dict = Depends(get_current_user_or_ak)
):
    """
    子节点心跳接口
    
    用于保持连接活跃
    """
    try:
        # 获取认证信息中的节点ID
        auth_header = request.headers.get("Authorization", "")
        if auth_header.startswith("AK-SK "):
            credentials = auth_header[6:].strip()
            api_key = credentials.split(':')[0] if ':' in credentials else credentials
            
            # 查找对应节点并更新状态
            session = DB.get_session()
            node = session.query(CascadeNode).filter(
                CascadeNode.api_key == api_key
            ).first()
            
            if node:
                node.status = 1  # 在线
                node.last_heartbeat_at = datetime.utcnow()
                session.commit()
        
        return success_response({"status": "alive"})
    except Exception as e:
        return error_response(code=500, message=str(e))


# ===== 同步日志接口 =====

@router.get("/sync-logs", summary="获取同步日志")
async def list_sync_logs(
    node_id: Optional[str] = Query(None),
    operation: Optional[str] = Query(None),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    current_user: dict = Depends(get_current_user_or_ak)
):
    """
    获取同步日志
    
    参数:
        node_id: 可选，按节点ID筛选
        operation: 可选，按操作类型筛选
        limit: 每页数量
        offset: 偏移量
    """
    session = DB.get_session()
    try:
        query = session.query(CascadeSyncLog)
        
        if node_id:
            query = query.filter(CascadeSyncLog.node_id == node_id)
        if operation:
            query = query.filter(CascadeSyncLog.operation == operation)
        
        total = query.count()
        logs = query.order_by(CascadeSyncLog.started_at.desc()).limit(limit).offset(offset).all()
        
        log_list = []
        for log in logs:
            log_data = {
                "id": log.id,
                "node_id": log.node_id,
                "operation": log.operation,
                "direction": log.direction,
                "status": log.status,
                "data_count": log.data_count,
                "error_message": log.error_message,
                "extra_data": json.loads(log.extra_data) if log.extra_data else {},
                "started_at": log.started_at.isoformat() if log.started_at else None,
                "completed_at": log.completed_at.isoformat() if log.completed_at else None
            }
            log_list.append(log_data)
        
        return success_response({
            "list": log_list,
            "page": {
                "limit": limit,
                "offset": offset
            },
            "total": total
        })
    except Exception as e:
        return error_response(code=500, message=str(e))


from datetime import datetime

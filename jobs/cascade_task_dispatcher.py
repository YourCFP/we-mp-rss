"""级联任务分发器 - 父节点分配公众号任务给子节点"""

import sys
import os

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import asyncio
import json
import uuid
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from core.db import DB
from core.models.cascade_node import CascadeNode
from core.models.message_task import MessageTask
from core.models.feed import Feed
from core.cascade import CascadeManager, cascade_manager
from core.print import print_info, print_success, print_error, print_warning


class TaskAllocation:
    """任务分配记录"""
    def __init__(
        self,
        allocation_id: str,
        node_id: str,
        task_id: str,
        feed_ids: List[str],
        status: str = "pending"
    ):
        self.allocation_id = allocation_id
        self.node_id = node_id
        self.task_id = task_id
        self.feed_ids = feed_ids
        self.status = status  # pending, executing, completed, failed
        self.created_at = datetime.utcnow()
        self.updated_at = datetime.utcnow()

    def to_dict(self):
        return {
            "allocation_id": self.allocation_id,
            "node_id": self.node_id,
            "task_id": self.task_id,
            "feed_ids": self.feed_ids,
            "status": self.status,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat()
        }


class NodeStatus:
    """节点状态"""
    def __init__(self, node: CascadeNode):
        self.node_id = node.id
        self.node_name = node.name
        self.api_url = node.api_url
        self.status = node.status  # 0=离线, 1=在线
        self.last_heartbeat = node.last_heartbeat_at
        self.is_active = node.is_active
        
        # 节点负载状态
        self.current_tasks = 0
        self.max_capacity = 10  # 默认最大并发任务数
        self.feed_quota = {}  # 公众号配额 {mp_id: count}
        
        # 从sync_config读取配置
        if node.sync_config:
            try:
                config = json.loads(node.sync_config)
                self.max_capacity = config.get("max_capacity", 10)
                self.feed_quota = config.get("feed_quota", {})
            except:
                pass

    @property
    def is_online(self) -> bool:
        """节点是否在线"""
        if not self.is_active:
            return False
        if self.status != 1:
            return False
        # 检查心跳是否超时（超过3分钟）
        if self.last_heartbeat:
            heartbeat_age = (datetime.utcnow() - self.last_heartbeat).total_seconds()
            if heartbeat_age > 180:
                return False
        return True

    @property
    def available_capacity(self) -> int:
        """可用容量"""
        return max(0, self.max_capacity - self.current_tasks)

    @property
    def is_available(self) -> bool:
        """是否可用"""
        return self.is_online and self.available_capacity > 0


class CascadeTaskDispatcher:
    """级联任务分发器 - 父节点使用"""
    
    def __init__(self):
        self.manager = cascade_manager
        self.allocations: Dict[str, TaskAllocation] = {}  # allocation_id -> TaskAllocation
        self.node_statuses: Dict[str, NodeStatus] = {}  # node_id -> NodeStatus
        self.task_assignments: Dict[str, List[str]] = {}  # task_id -> [allocation_ids]

    def refresh_node_statuses(self):
        """刷新所有子节点状态"""
        session = DB.get_session()
        try:
            child_nodes = session.query(CascadeNode).filter(
                CascadeNode.node_type == 1,
                CascadeNode.is_active == True
            ).all()
            
            online_count = 0
            for node in child_nodes:
                self.node_statuses[node.id] = NodeStatus(node)
                if self.node_statuses[node.id].is_online:
                    online_count += 1
            
            print_success(f"刷新节点状态完成: 共{len(child_nodes)}个节点, {online_count}个在线")
            return online_count
        except Exception as e:
            print_error(f"刷新节点状态失败: {str(e)}")
            return 0

    def select_node_for_feed(self, mp_id: str) -> Optional[str]:
        """
        为指定公众号选择合适的节点
        
        策略:
        1. 检查节点配额配置
        2. 选择可用容量最大的在线节点
        3. 负载均衡：避免单个节点过载
        """
        available_nodes = [
            (node_id, status)
            for node_id, status in self.node_statuses.items()
            if status.is_available
        ]
        
        if not available_nodes:
            return None
        
        # 检查是否有配额配置
        for node_id, status in available_nodes:
            quota = status.feed_quota.get(mp_id, 0)
            if quota > 0:
                # 有配额的节点优先
                return node_id
        
        # 无配额配置，选择负载最轻的节点
        available_nodes.sort(key=lambda x: x[1].current_tasks)
        return available_nodes[0][0]

    def allocate_feeds_to_node(
        self,
        task_id: str,
        feeds: List[Feed],
        node_id: str
    ) -> Optional[TaskAllocation]:
        """
        将公众号分配给指定节点
        
        参数:
            task_id: 任务ID
            feeds: 公众号列表
            node_id: 目标节点ID
        
        返回:
            任务分配记录
        """
        if node_id not in self.node_statuses:
            print_error(f"节点不存在: {node_id}")
            return None
        
        node_status = self.node_statuses[node_id]
        if not node_status.is_available:
            print_warning(f"节点不可用: {node_status.node_name}")
            return None
        
        # 检查容量
        required_capacity = len(feeds)
        if required_capacity > node_status.available_capacity:
            print_warning(f"节点 {node_status.node_name} 容量不足: 需要{required_capacity}, 可用{node_status.available_capacity}")
            return None
        
        # 创建分配记录
        allocation_id = str(uuid.uuid4())
        allocation = TaskAllocation(
            allocation_id=allocation_id,
            node_id=node_id,
            task_id=task_id,
            feed_ids=[feed.id for feed in feeds]
        )
        
        # 更新状态
        self.allocations[allocation_id] = allocation
        node_status.current_tasks += required_capacity
        
        if task_id not in self.task_assignments:
            self.task_assignments[task_id] = []
        self.task_assignments[task_id].append(allocation_id)
        
        print_success(f"分配任务: {len(feeds)}个公众号 -> {node_status.node_name} (allocation_id: {allocation_id})")
        return allocation

    def dispatch_task_to_children(self, task: MessageTask) -> Dict[str, List[Feed]]:
        """
        将任务分发给子节点
        
        参数:
            task: 消息任务对象
        
        返回:
            {node_id: [Feed]} 分配结果
        """
        print_info(f"开始分发任务: {task.name}")
        
        # 刷新节点状态
        self.refresh_node_statuses()
        
        # 获取任务关联的公众号
        session = DB.get_session()
        try:
            mps_list = json.loads(task.mps_id) if task.mps_id else []
            feed_ids = [mp["id"] for mp in mps_list]
            
            feeds = session.query(Feed).filter(Feed.id.in_(feed_ids)).all()
            
            if not feeds:
                print_warning(f"任务 {task.name} 没有关联公众号")
                return {}
            
            print_info(f"任务 {task.name} 包含 {len(feeds)} 个公众号")
            
            # 分配公众号
            allocations = {}
            remaining_feeds = feeds.copy()
            
            while remaining_feeds:
                # 为每个公众号选择节点
                allocation_map = {}  # {node_id: [feeds]}
                
                for feed in remaining_feeds[:]:
                    node_id = self.select_node_for_feed(feed.id)
                    if node_id:
                        if node_id not in allocation_map:
                            allocation_map[node_id] = []
                        allocation_map[node_id].append(feed)
                        remaining_feeds.remove(feed)
                
                if not allocation_map:
                    print_warning("没有可用节点，停止分配")
                    break
                
                # 执行分配
                for node_id, node_feeds in allocation_map.items():
                    allocation = self.allocate_feeds_to_node(task.id, node_feeds, node_id)
                    if allocation:
                        allocations[node_id] = node_feeds
                    else:
                        # 分配失败，公众号放回待分配列表
                        remaining_feeds.extend(node_feeds)
                        print_warning(f"分配失败，公众号放回待分配列表")
                        break
            
            if allocations:
                total_allocated = sum(len(feeds) for feeds in allocations.values())
                print_success(f"任务分发完成: {total_allocated}/{len(feeds)} 个公众号已分配给 {len(allocations)} 个节点")
            else:
                print_warning(f"任务 {task.name} 分发失败，没有可用节点")
            
            return allocations
            
        except Exception as e:
            print_error(f"分发任务失败: {str(e)}")
            return {}

    def create_task_package(
        self,
        task: MessageTask,
        feeds: List[Feed]
    ) -> dict:
        """
        创建任务包（发送给子节点）
        
        参数:
            task: 任务对象
            feeds: 公众号列表
        
        返回:
            任务包字典
        """
        return {
            "task_id": task.id,
            "task_name": task.name,
            "message_type": task.message_type,
            "message_template": task.message_template,
            "web_hook_url": task.web_hook_url,
            "cron_exp": task.cron_exp,
            "headers": task.headers,
            "cookies": task.cookies,
            "feeds": [
                {
                    "id": feed.id,
                    "faker_id": feed.faker_id,
                    "mp_name": feed.mp_name,
                    "mp_cover": feed.mp_cover,
                    "mp_intro": feed.mp_intro,
                    "status": feed.status
                }
                for feed in feeds
            ],
            "dispatched_at": datetime.utcnow().isoformat()
        }

    async def dispatch_to_child_node(
        self,
        node_id: str,
        task_package: dict
    ) -> bool:
        """
        将任务包推送到子节点
        
        参数:
            node_id: 子节点ID
            task_package: 任务包
        
        返回:
            是否成功
        """
        if node_id not in self.node_statuses:
            print_error(f"节点不存在: {node_id}")
            return False
        
        node_status = self.node_statuses[node_id]
        
        # 这里需要调用子节点的API来推送任务
        # TODO: 实现子节点的任务接收API
        # POST /api/v1/cascade/receive-task
        
        print_info(f"推送到节点 {node_status.node_name}: {len(task_package['feeds'])} 个公众号")
        
        # 临时：记录分配信息到sync_log
        log = self.manager.create_sync_log(
            node_id=node_id,
            operation="dispatch_task",
            direction="push",
            extra_data={
                "task_id": task_package["task_id"],
                "feed_count": len(task_package["feeds"]),
                "task_name": task_package["task_name"]
            }
        )
        
        if log:
            self.manager.update_sync_log(log.id, status=1, data_count=len(task_package["feeds"]))
        
        return True

    async def execute_dispatch(self, task_id: str = None):
        """
        执行任务分发
        
        参数:
            task_id: 任务ID，None则分发所有启用任务
        """
        session = DB.get_session()
        try:
            # 获取任务
            query = session.query(MessageTask).filter(MessageTask.status == 0)
            if task_id:
                query = query.filter(MessageTask.id == task_id)
            
            tasks = query.all()
            
            print_info(f"开始分发 {len(tasks)} 个任务")
            
            for task in tasks:
                # 分发任务
                allocations = self.dispatch_task_to_children(task)
                
                # 推送到各个子节点
                for node_id, feeds in allocations.items():
                    task_package = self.create_task_package(task, feeds)
                    success = await self.dispatch_to_child_node(node_id, task_package)
                    if not success:
                        print_error(f"推送任务到节点 {node_id} 失败")
            
            print_success("所有任务分发完成")
            
        except Exception as e:
            print_error(f"执行分发失败: {str(e)}")


# 全局分发器实例
cascade_task_dispatcher = CascadeTaskDispatcher()


# ========== 子节点接收任务的相关功能 ==========

async def fetch_task_from_parent() -> Optional[dict]:
    """
    子节点从父节点获取分配的任务
    
    返回:
        任务包字典，无任务则返回None
    """
    from jobs.cascade_sync import cascade_sync_service
    
    if not cascade_sync_service.client:
        print_warning("级联客户端未初始化")
        return None
    
    try:
        # 使用新的get_pending_tasks方法
        task_package = await cascade_sync_service.client.get_pending_tasks(limit=1)
        
        if task_package:
            print_info(f"从父节点获取到任务: {task_package.get('task_name')}")
            return task_package
        
        return None
        
    except Exception as e:
        print_error(f"从父节点获取任务失败: {str(e)}")
        return None


async def execute_parent_task(task_package: dict):
    """
    子节点执行父节点分配的任务
    
    参数:
        task_package: 任务包
    """
    from jobs.mps import get_feeds, add_job
    from jobs.webhook import web_hook
    import core.db as db
    
    print_info(f"开始执行父节点任务: {task_package['task_name']}")
    
    try:
        # 创建任务对象
        task_data = task_package
        
        # 创建本地Feed对象（如果不存在）
        session = DB.get_session()
        feeds_list = []
        for feed_data in task_data.get("feeds", []):
            feed = session.query(Feed).filter(Feed.id == feed_data["id"]).first()
            if not feed:
                # 创建本地Feed记录
                feed = Feed(
                    id=feed_data["id"],
                    faker_id=feed_data.get("faker_id"),
                    mp_name=feed_data["mp_name"],
                    mp_cover=feed_data["mp_cover"],
                    mp_intro=feed_data["mp_intro"],
                    status=feed_data["status"],
                    created_at=datetime.utcnow(),
                    updated_at=datetime.utcnow()
                )
                session.add(feed)
            feeds_list.append(feed)
        
        session.commit()
        
        # 执行任务（使用现有的jobs.mps逻辑）
        from jobs.mps import do_job
        
        # 创建MessageTask对象
        task = MessageTask(
            id=task_data["task_id"],
            name=task_data["task_name"],
            message_type=task_data["message_type"],
            message_template=task_data["message_template"],
            web_hook_url=task_data["web_hook_url"],
            cron_exp=task_data.get("cron_exp", ""),
            headers=task_data.get("headers", ""),
            cookies=task_data.get("cookies", ""),
            status=0,
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow()
        )
        
        # 为每个公众号执行任务
        results = []
        for feed in feeds_list:
            try:
                print_info(f"处理公众号: {feed.mp_name}")
                do_job(mp=feed, task=task, isTest=False)
                results.append({
                    "mp_id": feed.id,
                    "mp_name": feed.mp_name,
                    "status": "success"
                })
            except Exception as e:
                print_error(f"处理公众号失败 {feed.mp_name}: {str(e)}")
                results.append({
                    "mp_id": feed.id,
                    "mp_name": feed.mp_name,
                    "status": "failed",
                    "error": str(e)
                })
        
        # 上报结果到父节点
        await cascade_sync_service.report_task_result(
            task_data["task_id"],
            results
        )
        
        print_success(f"任务执行完成: {task_data['task_name']}")
        
    except Exception as e:
        print_error(f"执行父节点任务失败: {str(e)}")


async def start_child_task_worker(poll_interval: int = 30):
    """
    子节点任务拉取器 - 定期从父节点拉取任务
    
    参数:
        poll_interval: 轮询间隔（秒）
    """
    print_info("启动子节点任务拉取器")
    
    while True:
        try:
            # 获取任务
            task_package = await fetch_task_from_parent()
            
            if task_package:
                # 执行任务
                await execute_parent_task(task_package)
            else:
                print_info("暂无任务，等待下次轮询...")
            
        except Exception as e:
            print_error(f"任务拉取器错误: {str(e)}")
        
        # 等待下次轮询
        await asyncio.sleep(poll_interval)


if __name__ == "__main__":
    import sys
    
    # 测试分发器
    if len(sys.argv) > 1 and sys.argv[1] == "parent":
        # 父节点模式
        print_info("启动父节点任务分发器")
        dispatcher = CascadeTaskDispatcher()
        dispatcher.refresh_node_statuses()
        asyncio.run(dispatcher.execute_dispatch())
    
    elif len(sys.argv) > 1 and sys.argv[1] == "child":
        # 子节点模式
        print_info("启动子节点任务拉取器")
        asyncio.run(start_child_task_worker())
    
    else:
        print("用法:")
        print("  python jobs/cascade_task_dispatcher.py parent  # 父节点分发任务")
        print("  python jobs/cascade_task_dispatcher.py child   # 子节点拉取任务")

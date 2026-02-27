"""级联同步服务 - 子节点向父节点同步数据"""

import asyncio
import json
from datetime import datetime
from typing import Optional
from core.cascade import CascadeClient, cascade_manager
from core.models.cascade_node import CascadeNode
from core.models.feed import Feed
from core.models.message_task import MessageTask
from core.db import DB
from core.print import print_info, print_success, print_error, print_warning


class CascadeSyncService:
    """级联同步服务"""
    
    def __init__(self):
        self.client: Optional[CascadeClient] = None
        self.sync_enabled = False
        self.sync_interval = 300  # 默认5分钟同步一次
        self.running = False
    
    def initialize(self):
        """初始化同步服务，从配置读取父节点信息"""
        from core.config import cfg
        
        # 检查是否启用了级联模式
        cascade_config = cfg.get("cascade", {})
        if not cfg.get("cascade.enabled", False):
            print_warning("级联模式未启用")
            return
        
        # 获取父节点配置
        parent_url = str(cfg.get("cascade.parent_api_url"))
        api_key = str(cfg.get("cascade.api_key"))
        api_secret = str(cfg.get("cascade.api_secret"))
        
        if not all([parent_url, api_key, api_secret]):
            print_warning("级联配置不完整，请检查配置文件")
            return
        
        # 创建客户端
        self.client = CascadeClient(parent_url, api_key, api_secret)
        self.sync_enabled = True
        self.sync_interval = cascade_config.get("sync_interval", 300)
        
        print_success(f"级联同步服务初始化成功，父节点地址: {parent_url}")
    
    async def sync_feeds(self) -> int:
        """
        从父节点同步公众号数据
        
        返回: 同步的数据条数
        """
        if not self.client:
            return 0
        
        try:
            # 创建同步日志
            log = cascade_manager.create_sync_log(
                node_id="self",
                operation="sync_feeds",
                direction="pull",
                extra_data={"timestamp": datetime.utcnow().isoformat()}
            )
            
            # 拉取数据
            feeds_data = await self.client.pull_feeds()
            count = 0
            
            # 更新或创建公众号记录
            session = DB.get_session()
            for feed_data in feeds_data:
                try:
                    existing_feed = session.query(Feed).filter(
                        Feed.id == feed_data["id"]
                    ).first()
                    
                    if existing_feed:
                        # 更新现有记录
                        existing_feed.mp_name = feed_data["mp_name"]
                        existing_feed.mp_cover = feed_data["mp_cover"]
                        existing_feed.mp_intro = feed_data["mp_intro"]
                        existing_feed.faker_id = feed_data.get("faker_id")
                        existing_feed.updated_at = datetime.utcnow()
                    else:
                        # 创建新记录
                        new_feed = Feed(
                            id=feed_data["id"],
                            faker_id=feed_data.get("faker_id"),
                            mp_name=feed_data["mp_name"],
                            mp_cover=feed_data["mp_cover"],
                            mp_intro=feed_data["mp_intro"],
                            status=feed_data["status"],
                            created_at=datetime.utcnow(),
                            updated_at=datetime.utcnow()
                        )
                        session.add(new_feed)
                    
                    count += 1
                except Exception as e:
                    print_error(f"同步公众号失败: {feed_data.get('mp_name')} - {str(e)}")
            
            session.commit()
            
            # 更新同步日志
            if log:
                cascade_manager.update_sync_log(log.id, status=1, data_count=count)
            
            print_success(f"公众号同步完成，共同步 {count} 条")
            return count
            
        except Exception as e:
            print_error(f"同步公众号失败: {str(e)}")
            if log:
                cascade_manager.update_sync_log(log.id, status=2, error_message=str(e))
            return 0
    
    async def sync_message_tasks(self) -> int:
        """
        从父节点同步消息任务
        
        返回: 同步的数据条数
        """
        if not self.client:
            return 0
        
        try:
            # 创建同步日志
            log = cascade_manager.create_sync_log(
                node_id="self",
                operation="sync_tasks",
                direction="pull",
                extra_data={"timestamp": datetime.utcnow().isoformat()}
            )
            
            # 拉取数据
            tasks_data = await self.client.pull_message_tasks()
            count = 0
            
            # 更新或创建任务记录
            session = DB.get_session()
            for task_data in tasks_data:
                try:
                    existing_task = session.query(MessageTask).filter(
                        MessageTask.id == task_data["id"]
                    ).first()
                    
                    if existing_task:
                        # 更新现有任务
                        existing_task.name = task_data["name"]
                        existing_task.message_type = task_data["message_type"]
                        existing_task.message_template = task_data["message_template"]
                        existing_task.web_hook_url = task_data["web_hook_url"]
                        existing_task.mps_id = task_data["mps_id"]
                        existing_task.cron_exp = task_data["cron_exp"]
                        existing_task.status = task_data["status"]
                        existing_task.headers = task_data.get("headers", "")
                        existing_task.cookies = task_data.get("cookies", "")
                        existing_task.updated_at = datetime.utcnow()
                    else:
                        # 创建新任务
                        new_task = MessageTask(
                            id=task_data["id"],
                            name=task_data["name"],
                            message_type=task_data["message_type"],
                            message_template=task_data["message_template"],
                            web_hook_url=task_data["web_hook_url"],
                            mps_id=task_data["mps_id"],
                            cron_exp=task_data["cron_exp"],
                            status=task_data["status"],
                            headers=task_data.get("headers", ""),
                            cookies=task_data.get("cookies", ""),
                            created_at=datetime.utcnow(),
                            updated_at=datetime.utcnow()
                        )
                        session.add(new_task)
                    
                    count += 1
                except Exception as e:
                    print_error(f"同步任务失败: {task_data.get('name')} - {str(e)}")
            
            session.commit()
            
            # 更新同步日志
            if log:
                cascade_manager.update_sync_log(log.id, status=1, data_count=count)
            
            print_success(f"消息任务同步完成，共同步 {count} 条")
            return count
            
        except Exception as e:
            print_error(f"同步消息任务失败: {str(e)}")
            if log:
                cascade_manager.update_sync_log(log.id, status=2, error_message=str(e))
            return 0
    
    async def report_task_result(self, task_id: str, results: list):
        """
        向父节点上报任务执行结果
        
        参数:
            task_id: 任务ID
            results: 执行结果列表
        """
        if not self.client:
            return
        
        try:
            # 创建同步日志
            log = cascade_manager.create_sync_log(
                node_id="self",
                operation="report_result",
                direction="push",
                extra_data={
                    "task_id": task_id,
                    "result_count": len(results)
                }
            )
            
            # 上报结果
            await self.client.report_task_result(task_id, results)
            
            # 更新同步日志
            if log:
                cascade_manager.update_sync_log(log.id, status=1, data_count=len(results))
            
            print_success(f"任务结果上报成功: {task_id}")
            
        except Exception as e:
            print_error(f"上报任务结果失败: {str(e)}")
            if log:
                cascade_manager.update_sync_log(log.id, status=2, error_message=str(e))
    
    async def send_heartbeat(self):
        """发送心跳到父节点"""
        if not self.client:
            return
        
        try:
            await self.client.send_heartbeat()
        except Exception as e:
            print_error(f"心跳发送失败: {str(e)}")
    
    async def full_sync(self):
        """执行完整同步"""
        print_info("开始完整同步...")
        
        # 1. 发送心跳
        await self.send_heartbeat()
        
        # # 2. 同步公众号
        # await self.sync_feeds()
        
        # # 3. 同步消息任务
        # await self.sync_message_tasks()
        
        print_success("完整同步完成")
    
    async def start_periodic_sync(self):
        """启动定期同步"""
        if not self.sync_enabled or self.running:
            return
        
        self.running = True
        print_info(f"启动定期同步服务，间隔: {self.sync_interval}秒")
        
        try:
            while self.running:
                try:
                    await self.full_sync()
                except Exception as e:
                    print_error(f"定期同步出错: {str(e)}")
                
                # 等待下次同步
                await asyncio.sleep(int(self.sync_interval))
        except asyncio.CancelledError:
            print_info("定期同步服务已停止")
    
    def stop(self):
        """停止同步服务"""
        self.running = False
        print_info("级联同步服务已停止")


# 全局同步服务实例
cascade_sync_service = CascadeSyncService()

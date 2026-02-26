"""测试级联任务分发系统"""

import sys
import os

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def test_dispatcher_initialization():
    """测试分发器初始化"""
    print("="*60)
    print("测试: 分发器初始化")
    print("="*60)

    from jobs.cascade_task_dispatcher import CascadeTaskDispatcher

    dispatcher = CascadeTaskDispatcher()
    print(f"✓ 分发器创建成功")
    print(f"  - allocations: {len(dispatcher.allocations)}")
    print(f"  - node_statuses: {len(dispatcher.node_statuses)}")
    print(f"  - task_assignments: {len(dispatcher.task_assignments)}")
    print()


def test_node_status():
    """测试节点状态"""
    print("="*60)
    print("测试: 节点状态")
    print("="*60)

    from jobs.cascade_task_dispatcher import cascade_task_dispatcher
    from core.db import DB
    from core.config import cfg

    DB.create_tables()

    # 刷新节点状态
    count = cascade_task_dispatcher.refresh_node_statuses()
    print(f"✓ 节点状态刷新完成: {count} 个在线节点")
    print()

    # 显示节点详情
    for node_id, status in cascade_task_dispatcher.node_statuses.items():
        print(f"节点: {status.node_name}")
        print(f"  ID: {node_id}")
        print(f"  在线: {status.is_online}")
        print(f"  可用: {status.is_available}")
        print(f"  容量: {status.current_tasks}/{status.max_capacity}")
        print()


def test_task_dispatch():
    """测试任务分发"""
    print("="*60)
    print("测试: 任务分发")
    print("="*60)

    from jobs.cascade_task_dispatcher import cascade_task_dispatcher
    from core.db import DB
    from core.models.message_task import MessageTask

    # 刷新节点状态
    cascade_task_dispatcher.refresh_node_statuses()

    # 获取任务
    session = DB.get_session()
    task = session.query(MessageTask).filter(MessageTask.status == 0).first()

    if task:
        print(f"✓ 找到任务: {task.name}")
        print(f"  任务ID: {task.id}")
        print(f"  公众号数: {len(task.mps_id)}")
        print()

        # 分发任务
        allocations = cascade_task_dispatcher.dispatch_task_to_children(task)
        print(f"✓ 分发结果: {len(allocations)} 个节点获得任务")

        for node_id, feeds in allocations.items():
            node_status = cascade_task_dispatcher.node_statuses[node_id]
            print(f"\n节点: {node_status.node_name}")
            print(f"  公众号数量: {len(feeds)}")
            for feed in feeds:
                print(f"    - {feed.mp_name}")
    else:
        print("✗ 没有找到启用的任务")

    print()


def test_allocation_management():
    """测试分配管理"""
    print("="*60)
    print("测试: 分配管理")
    print("="*60)

    from jobs.cascade_task_dispatcher import cascade_task_dispatcher

    if cascade_task_dispatcher.allocations:
        print(f"✓ 当前有 {len(cascade_task_dispatcher.allocations)} 个分配记录")
        print()

        for alloc_id, allocation in cascade_task_dispatcher.allocations.items():
            node_name = cascade_task_dispatcher.node_statuses.get(
                allocation.node_id
            )
            if node_name:
                node_name = node_name.node_name
            print(f"分配ID: {alloc_id}")
            print(f"  节点: {node_name or allocation.node_id}")
            print(f"  任务ID: {allocation.task_id}")
            print(f"  公众号数: {len(allocation.feed_ids)}")
            print(f"  状态: {allocation.status}")
            print()
    else:
        print("  暂无分配记录")


def test_api_endpoints():
    """测试API端点"""
    print("="*60)
    print("测试: API端点")
    print("="*60)

    import requests
    from core.config import cfg

    api_base = f"http://localhost:{cfg.get('port', 8001)}"
    api_url = f"{api_base}/api/v1"

    try:
        # 获取节点列表
        response = requests.get(
            f"{api_url}/cascade/nodes",
            timeout=5
        )
        if response.status_code == 200:
            data = response.json()
            print(f"✓ 节点列表API正常")
            if data.get("data"):
                nodes = data["data"]
                print(f"  节点数量: {len(nodes)}")
        else:
            print(f"✗ 节点列表API失败: {response.status_code}")

        # 获取分配记录
        response = requests.get(
            f"{api_url}/cascade/allocations",
            timeout=5
        )
        if response.status_code == 200:
            print(f"✓ 分配记录API正常")
        else:
            print(f"✗ 分配记录API失败: {response.status_code}")

    except requests.exceptions.ConnectionError:
        print("✗ 无法连接到服务器，请确保服务已启动")
    except Exception as e:
        print(f"✗ API测试失败: {str(e)}")

    print()


def run_all_tests():
    """运行所有测试"""
    print("\n")
    print("╔" + "="*58 + "╗")
    print("║" + " "*15 + "级联任务分发系统测试" + " "*19 + "║")
    print("╚" + "="*58 + "╝")
    print()

    try:
        test_dispatcher_initialization()
        test_node_status()
        test_task_dispatch()
        test_allocation_management()
        test_api_endpoints()

        print("="*60)
        print("✓ 所有测试完成")
        print("="*60)

    except Exception as e:
        print(f"✗ 测试失败: {str(e)}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    run_all_tests()

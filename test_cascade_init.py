"""测试级联客户端初始化"""

import sys
import os

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from core.config import cfg

print("="*60)
print("测试级联客户端初始化")
print("="*60)
print()

# 1. 检查配置
print("1. 检查级联配置...")
cascade_config = cfg.get("cascade", {})

print(f"   enabled: {cascade_config.get('enabled', False)}")
print(f"   node_type: {cascade_config.get('node_type', 'parent')}")
print(f"   parent_api_url: {cascade_config.get('parent_api_url', '未配置')}")
print(f"   api_key: {'已配置' if cascade_config.get('api_key') else '未配置'}")
print(f"   api_secret: {'已配置' if cascade_config.get('api_secret') else '未配置'}")
print()

# 2. 测试初始化
print("2. 测试级联同步服务初始化...")
from jobs.cascade_sync import cascade_sync_service

cascade_sync_service.initialize()

if cascade_sync_service.client:
    print("   ✓ 级联客户端初始化成功")
    print(f"     - 父节点地址: {cascade_sync_service.client.parent_api_url}")
    print(f"     - API Key: {cascade_sync_service.client.api_key[:20]}...")
    print(f"     - 同步间隔: {cascade_sync_service.sync_interval}秒")
else:
    print("   ✗ 级联客户端初始化失败")
    print("   可能原因:")
    print("     - 级联模式未启用")
    print("     - 配置不完整（缺少parent_api_url/api_key/api_secret）")
    print()
    print("   请在config.yaml中添加以下配置:")
    print("   cascade:")
    print("     enabled: true")
    print("     node_type: child")
    print("     parent_api_url: http://parent-server:8001")
    print("     api_key: CNxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")
    print("     api_secret: CSxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")

print()
print("="*60)
print("测试完成")
print("="*60)

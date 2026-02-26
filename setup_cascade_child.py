"""
级联子节点配置脚本
在父节点上运行此脚本创建子节点凭证，然后更新子节点配置
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from core.db import DB
from core.models.cascade_node import CascadeNode
from core.cascade import cascade_manager
import hashlib

def main():
    session = DB.get_session()
    
    print("=" * 50)
    print("级联节点配置助手")
    print("=" * 50)
    
    # 检查现有节点
    existing = session.query(CascadeNode).filter(
        CascadeNode.node_type == 1
    ).all()
    
    if existing:
        print("\n现有的子节点:")
        for node in existing:
            print(f"  - ID: {node.id}")
            print(f"    名称: {node.name}")
            print(f"    AK: {node.api_key}")
            print(f"    状态: {'启用' if node.is_active else '禁用'}")
            print()
    
    # 创建新子节点
    print("创建新的子节点...")
    try:
        node = cascade_manager.create_node(
            node_type=1,  # 子节点
            name="子节点-" + hashlib.md5(os.urandom(8)).hexdigest()[:6],
            description="自动创建的子节点"
        )
        
        # 生成凭证
        creds = cascade_manager.generate_node_credentials(node.id)
        
        print("\n" + "=" * 50)
        print("✅ 子节点创建成功！")
        print("=" * 50)
        print(f"\n节点ID: {creds['node_id']}")
        print(f"API Key (AK): {creds['api_key']}")
        print(f"API Secret (SK): {creds['api_secret']}")
        
        print("\n" + "-" * 50)
        print("请将以下配置添加到子节点的 config.yaml 中:")
        print("-" * 50)
        print(f"""
cascade:
  enabled: true
  node_type: child
  parent_api_url: "http://YOUR_PARENT_IP:8001"  # 修改为父节点实际地址
  api_key: {creds['api_key']}
  api_secret: {creds['api_secret']}
  sync_interval: 300
  heartbeat_interval: 60
""")
        
        # 验证 SK 哈希
        stored_hash = session.query(CascadeNode).filter(
            CascadeNode.id == node.id
        ).first().api_secret_hash
        computed_hash = hashlib.sha256(creds['api_secret'].encode()).hexdigest()
        
        print("-" * 50)
        print(f"SK 哈希验证: {'✅ 匹配' if stored_hash == computed_hash else '❌ 不匹配'}")
        print(f"存储哈希: {stored_hash}")
        print(f"计算哈希: {computed_hash}")
        
    except Exception as e:
        print(f"\n❌ 创建失败: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0


if __name__ == "__main__":
    sys.exit(main())

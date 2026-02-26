"""测试父节点级联接口"""

import sys
import os
import asyncio

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import httpx
from core.config import cfg

async def test_parent_endpoints():
    """测试父节点的级联接口"""
    cascade_config = cfg.get("cascade", {})
    parent_url = cascade_config.get("parent_api_url", "http://localhost:8001")
    api_key = cascade_config.get("api_key", "")
    api_secret = cascade_config.get("api_secret", "")

    print("="*60)
    print("测试父节点级联接口")
    print("="*60)
    print()
    print(f"父节点地址: {parent_url}")
    print(f"API Key: {api_key[:20] if api_key else '未配置'}...")
    print()

    # 测试的接口列表
    tests = [
        {
            "name": "心跳接口 (POST)",
            "method": "POST",
            "url": f"{parent_url}/api/v1/cascade/heartbeat",
            "headers": {"Authorization": f"AK-SK {api_key}:{api_secret}"}
        },
        {
            "name": "获取公众号 (GET)",
            "method": "GET",
            "url": f"{parent_url}/api/v1/cascade/feeds",
            "headers": {"Authorization": f"AK-SK {api_key}:{api_secret}"}
        },
        {
            "name": "获取消息任务 (GET)",
            "method": "GET",
            "url": f"{parent_url}/api/v1/cascade/message-tasks",
            "headers": {"Authorization": f"AK-SK {api_key}:{api_secret}"}
        },
        {
            "name": "获取待处理任务 (GET)",
            "method": "GET",
            "url": f"{parent_url}/api/v1/cascade/pending-tasks",
            "headers": {"Authorization": f"AK-SK {api_key}:{api_secret}"}
        },
        {
            "name": "API文档 (GET)",
            "method": "GET",
            "url": f"{parent_url}/api/docs",
            "headers": {}
        }
    ]

    async with httpx.AsyncClient(timeout=10.0) as client:
        for test in tests:
            print(f"测试: {test['name']}")
            print(f"  URL: {test['url']}")
            print(f"  方法: {test['method']}")

            try:
                if test['method'] == "GET":
                    response = await client.get(test['url'], headers=test['headers'])
                elif test['method'] == "POST":
                    response = await client.post(test['url'], headers=test['headers'])
                else:
                    print("  ✗ 不支持的方法")
                    continue

                print(f"  状态码: {response.status_code}")

                if response.status_code == 200:
                    print("  ✓ 接口正常")
                    try:
                        data = response.json()
                        if isinstance(data, dict) and "code" in data:
                            print(f"  响应: {data.get('message', 'OK')}")
                    except:
                        pass
                elif response.status_code == 401:
                    print("  ✗ 认证失败 - 检查 API Key 和 Secret")
                elif response.status_code == 403:
                    print("  ✗ 权限不足")
                elif response.status_code == 404:
                    print("  ✗ 接口不存在 - 检查父节点是否启动")
                elif response.status_code == 405:
                    print("  ✗ 方法不允许 - 检查HTTP方法是否正确")
                    print(f"  允许的方法: {response.headers.get('Allow', '未知')}")
                elif response.status_code == 422:
                    print("  ✗ 参数验证失败")
                    try:
                        error = response.json()
                        print(f"  错误详情: {error}")
                    except:
                        pass
                else:
                    print(f"  ✗ 未知状态码")
                    try:
                        print(f"  响应: {response.text[:200]}")
                    except:
                        pass

            except httpx.ConnectError:
                print("  ✗ 连接失败 - 父节点未运行或地址错误")
            except httpx.TimeoutException:
                print("  ✗ 请求超时")
            except Exception as e:
                print(f"  ✗ 错误: {str(e)}")

            print()

    print("="*60)
    print("常见问题排查")
    print("="*60)
    print()
    print("1. 如果所有接口都返回连接失败:")
    print("   - 确认父节点服务已启动")
    print("   - 检查 parent_api_url 是否正确")
    print("   - 尝试在浏览器访问: http://localhost:8001/api/docs")
    print()
    print("2. 如果返回 401 认证失败:")
    print("   - 检查 api_key 和 api_secret 是否正确")
    print("   - 在父节点重新生成凭证: python jobs/cascade_init.py --child '节点名'")
    print()
    print("3. 如果返回 404 接口不存在:")
    print("   - 确认父节点是最新版本")
    print("   - 检查 apis/cascade.py 是否存在")
    print("   - 重启父节点服务")
    print()
    print("4. 如果返回 405 方法不允许:")
    print("   - 检查接口定义是否正确")
    print("   - 确认HTTP方法与接口定义一致")
    print("   - 查看API文档确认接口路径")


if __name__ == "__main__":
    asyncio.run(test_parent_endpoints())

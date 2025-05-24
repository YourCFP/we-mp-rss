import asyncio
from wx import WX_API
from threading import Thread
def Success(data):
    if data != None:
            print("\n登录结果:")
            print(f"Cookies数量: {len(data['cookies'])}")
            print(f"Token: {data['token']}")
    else:
            print("\n登录失败，请检查上述错误信息")
def task():
    code_url=WX_API.GetCode(Success)
    print(f"code url:{code_url}")
    WX_API.QRcode()

if __name__ == "__main__":
    task()
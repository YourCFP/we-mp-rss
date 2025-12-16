#!/usr/bin/env python
# -*- coding: utf-8 -*-

from driver.playwright_driver import PlaywrightController
import time

def test_browser():
    controller = PlaywrightController()
    try:
        print('正在测试浏览器启动...')
        page = controller.start_browser(headless=True, dis_image=False)
        print('浏览器启动成功！')
        controller.open_url('https://www.baidu.com')
        print('成功打开百度页面！')
        time.sleep(2)
        title = page.title()
        print(f'页面标题: {title}')
        return True
    except Exception as e:
        print(f'测试失败: {e}')
        return False
    finally:
        try:
            controller.Close()
            print('浏览器已关闭')
        except:
            pass

if __name__ == "__main__":
    success = test_browser()
    if success:
        print('✅ 测试通过')
    else:
        print('❌ 测试失败')
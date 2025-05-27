import sys
from .firefox_driver import FirefoxController
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from PIL import Image
from selenium.webdriver.common.action_chains import ActionChains
import time
import os
import re
import json
class Wx:
    HasLogin=False
    SESSION=None
    HasCode=False
    isLOCK=False
    wx_login_url="static/wx_qrcode.png"
    def check_dependencies(self):
        """检查必要的依赖包"""
        try:
            import selenium
            import PIL
        except ImportError as e:
            print("缺少必要的依赖包，请先安装：")
            print("pip install selenium Pillow")
            return False
        return True
    def GetHasCode(self):
        if os.path.exists(self.wx_login_url):
            return True
        return False
    def extract_token_from_requests(self,driver):
        """从页面中提取token"""
        try:
            # 尝试从当前URL获取token
            current_url = driver.current_url
            token_match = re.search(r'token=([^&]+)', current_url)
            if token_match:
                return token_match.group(1)
            
            # 尝试从localStorage获取
            token = driver.execute_script("return localStorage.getItem('token');")
            if token:
                return token
                
            # 尝试从sessionStorage获取
            token = driver.execute_script("return sessionStorage.getItem('token');")
            if token:
                return token
                
            # 尝试从cookie获取
            cookies = driver.get_cookies()
            for cookie in cookies:
                if 'token' in cookie['name'].lower():
                    return cookie['value']
                    
            return None
        except Exception as e:
            print(f"提取token时出错: {str(e)}")
            return None
    def GetCode(self,Callback=None):
        print("子线程执行中")
        from threading import Thread
        thread = Thread(target=self.wxLogin,args=(Callback,))  # 传入函数名
        thread.start()  # 启动线程
        print("微信公众平台登录脚本 v1.3")
        return WX_API.QRcode()
    wait_time=10
    def QRcode(self):
        return {
            "code":self.wx_login_url
        }
    def wxLogin(self,Callback=None):
        """
        微信公众平台登录流程：
        1. 检查依赖和环境
        2. 打开微信公众平台
        3. 全屏截图保存二维码
        4. 等待用户扫码登录
        5. 获取登录后的cookie和token
        """
        # 检查依赖
        if not self.check_dependencies():
            return None
        try:
            if  self.isLOCK:
                raise Exception("微信公众平台登录脚本正在运行，请勿重复运行！")
                return None
            self.HasLogin=False
            self.isLOCK=True
            self.clean()
            # 初始化浏览器控制器
            controller = FirefoxController()
            self.controller=controller
            # 启动浏览器并打开微信公众平台
            print("正在启动浏览器...")
            controller.start_browser()
            controller.open_url("https://mp.weixin.qq.com/")
            
            # 等待页面完全加载
            print("正在加载登录页面...")
            time.sleep(2)
            
            # 滚动到二维码区域
            qrcode = controller.driver.find_element(By.CLASS_NAME, "login__type__container__scan__qrcode")
            ActionChains(controller.driver).move_to_element(qrcode).perform()
            
            # 确保二维码可见
            wait = WebDriverWait(controller.driver, self.wait_time)
            wait.until(EC.visibility_of(qrcode))
            # 全屏截图并裁剪二维码区域
            print("正在生成二维码图片...")
            controller.driver.save_screenshot("temp_screenshot.png")
            img = Image.open("temp_screenshot.png")
            
            # 获取二维码位置和尺寸
            location = qrcode.location
            size = qrcode.size
            left = location['x']
            top = location['y']
            right = location['x'] + size['width']
            bottom = location['y'] + size['height']
            
            # 裁剪并保存二维码
            img = img.crop((left, top, right, bottom))
            img.save(self.wx_login_url)
            os.remove("temp_screenshot.png")
            
            print("二维码已保存为 wx_qrcode.png，请扫码登录...")
            self.HasCode=True
            
            # 等待登录成功（检测页面跳转）
            print("等待扫码登录...")
            wait=WebDriverWait(controller.driver, 120)
            wait.until(EC.url_contains("https://mp.weixin.qq.com/cgi-bin/home"))
            print("登录成功！")
            self.HasLogin=True
            # 获取token
            token = self.extract_token_from_requests(controller.driver)
            
            # 获取cookie
            cookies = controller.driver.get_cookies()
            print("\n获取到的Cookie:")
            cookies_str=""
            for cookie in cookies:
                print(f"{cookie['name']}={cookie['value']}")
                cookies_str+=f"{cookie['name']}={cookie['value']}; "
                
            if token:
                print(f"\n获取到的Token: {token}")
            
            self.SESSION= {
                'cookies': cookies,
                'cookies_str': cookies_str,
                'token': token,
                'wx_login_url': self.wx_login_url,
            }
            if Callback!=None:
                Callback(self.SESSION)
        except Exception as e:
            print(f"\n错误发生: {str(e)}")
            print("可能的原因:")
            print("1. 请确保已安装Firefox浏览器")
            print("2. 请确保geckodriver已下载并配置到PATH中")
            print("3. 检查网络连接是否可以访问微信公众平台")
            self.SESSION=None
        finally:
            self.clean()
            self.isLOCK=False
            if 'controller' in locals():
                controller.close()
        return self.SESSION
    def Close(self):
        rel=False
        try:
                self.controller.close()
                rel=True
        except:
            print("浏览器未启动")
            pass
        return rel
    def clean(self):
        try:
            os.remove(self.wx_login_url)
        except:
            pass
        finally:
           pass
WX_API=Wx()

from core.config import cfg
def sys_notice(text:str="",title:str=""):
    from core.notice import notice
    markdown_text = f"### {title} 通知\n{text}"
    webhook = cfg.get('notice')['dingding']
    if len(webhook)>0:
        notice(webhook, title, markdown_text)
    feishu_webhook = cfg.get('notice')['feishu']
    if len(feishu_webhook)>0:
        notice(feishu_webhook, title, markdown_text)
    wechat_webhook = cfg.get('notice')['wechat']
    if len(wechat_webhook)>0:
        notice(wechat_webhook, title, markdown_text)

from driver.wx import WX_API
def send_wx_code(title:str="",url:str=""):
    WX_API.GetCode(Notice=CallBackNotice)
    pass
def CallBackNotice():
        url=WX_API.QRcode()['code']
        svg="""
        <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 200 200">
        <rect x="10" y="10" width="180" height="180" fill="#ffcc00" stroke="#000" stroke-width="2"/>
        </svg>
        """
        rss_domain=cfg.get("rss.base_url","")
        url=rss_domain+str(url)
        text=f"![二维码]({url})"
        text+=f"<img src='{url}' width=40 height=40 />"
        text+=f"\n\n## 请使用微信扫描二维码进行授权"
        sys_notice(text, "WeRss授权过期")
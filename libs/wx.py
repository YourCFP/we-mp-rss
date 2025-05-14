import requests
import yaml
import json
import re
import datetime
import libs.db as db
from datetime import datetime, timezone

def dateformat(timestamp:int):
    # UTC时间对象
    utc_dt = datetime.fromtimestamp(timestamp, timezone.utc)
    t=(utc_dt.strftime("%Y-%m-%d %H:%M:%S")) 

    # UTC转本地时区
    local_dt = utc_dt.astimezone()
    t=(local_dt.strftime("%Y-%m-%d %H:%M:%S"))
    return t

with open("config.yaml", "r") as f:
     config = yaml.safe_load(f)
def get_Articles(faker_id:str):
    headers = {
        "Cookie": config["cookie"],
        "User-Agent": config["user_agent"]
    }
    params = {
        "sub": "list",
        "sub_action": "list_ex",
        "begin": 0,
        "count": config["count"],
        "fakeid": faker_id,
        "token": config["token"],
        "lang": "zh_CN",
        "f": "json",
        "ajax": 1
    }
    url = "https://mp.weixin.qq.com/cgi-bin/appmsgpublish"
    headers = {
        "Cookie": config["cookie"],
        "User-Agent": config["user_agent"]
    }
    data={}
    try:
        response = requests.get(url, params=params, headers=headers)
        response.raise_for_status  # 检查状态码是否为200
        data = response.text  # 解析JSON数据
        data = json.loads(data)  # 手动解析
        data['publish_page']=json.loads(data['publish_page'])
    except Exception as e:
        print(f"请求失败: {e}")
    return data
def get_id(url:str)->str:
    pattern = r"/([^/]+)$"  # 使用原始字符串避免转义问题
    match = re.search(pattern, url)
    if match:
        article_id = match.group(1)
        print(f"提取结果：{article_id}")  # 输出：5iq10fsH-5ZA9D1Uv4ciqQ
    else:
        print("未匹配到有效内容")
    return article_id
def get_list(faker_id:str=None,mp_id:str=None,is_add:bool=False):
    articles=[]
    data=get_Articles(faker_id)
    try:
        data=data['publish_page']['publish_list']
        wx_db=db.Db()
        wx_db.init(config['db'])
        for i in data:
            art=i['publish_info']
            art=json.loads(art)
            art=art['appmsgex']
            art=art[0]
            #    print(art,type(art),sep='\n\n')
            print(art['title'],art['cover'],art['link'],art['update_time'],art['create_time'],sep='\n',end='\n\n\n')
            article={           
            'id':get_id(art['link']),
            'mp_id':mp_id,
            'title':art['title'],
            'pic_url':art['cover'],
            'publish_time':art['update_time'],
            'created_at':dateformat(art['create_time']),
            'updated_at':dateformat(art['update_time']),
            'is_export':0,
            }
            articles.append(article)
            if is_add:
                wx_db.add_article(article)
                print('添加成功')
    except Exception as e:
        print(e,"出错了")
   
    return articles


# if __name__ == "__main__":
#   data=get_list()
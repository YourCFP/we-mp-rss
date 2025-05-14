from fastapi import FastAPI,Response # 导⼊FastAPI，⽤于定义API

import libs.wx as wx 
import libs.db as db
import job as job
import yaml
from fastapi.responses import PlainTextResponse
app = FastAPI() # 创建FastAPI实例
 
# http请求方式类型：get、post、put、update、delete
# 浏览器默认访问的是get类型，如果使用其他形式访问

# 不带参数的访问形式：
# 访问地址：http://127.0.0.1:8000
@app.get("/")
async def test_one():
    return {"message": "My first fastapi project"}

@app.get("/wx/job")
async def start_job():
    wx_db.init(config["db"])
    mps=wx_db.get_all_mps()
    wx_db.close()
    
    return {"code":"ok","data":mps }

@app.get("/wx/rss/{mp_id}")
async def wx_list(mp_id: str,update:int=0):
    wx_db.init(config["db"])
    faker_id=wx_db.get_faker_id(mp_id)
    data=wx.get_list(faker_id,mp_id,update)
    wx_db.close()
    return {"mp_id": mp_id,"data":data}


wx_db=db.Db()
with open("config.yaml", "r") as f:
        config = yaml.safe_load(f)
#获取所有需要同步的公众号
@app.get("/wx/mps")
async def wx_mps():
    wx_db.init(config["db"])
    data=wx_db.get_all_mps()
    for i in data:
        print(i.mp_name)
    wx_db.close()
    return {"code": "ok","data":data}
import libs.rss as rss
@app.get("/wx/feeds")
async def wx_mps(offset:int=0,limit:int=5):
    wx_db.init(config["db"])
    data=wx_db.get_articles(limit=limit,offset=offset)
    rss_data=[]
    for item in data:
        print(item.id)
        rss_data.append({"title":item.title,"id":item.id,"link":"https://mp.weixin.qq.com/s/"+item.id,"updated":item.created_at.isoformat()})
    wx_db.close()
    _rss_data=rss.generate_rss(rss_data,"rss.xml")
    print(rss_data)
    # return Response(content=_rss_data, media_type="application/xml")
    return Response(content=_rss_data, media_type="text/plan")


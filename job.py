import schedule, time
import libs.wx as wx 
import libs.db as db
import logging
from logging.handlers import RotatingFileHandler
import yaml
# 创建logger对象
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)  # 设置最低日志级别

# 创建文件处理器，每天一个文件，保留7天备份
handler = RotatingFileHandler('app.log', maxBytes=1024*1024, backupCount=7)
handler.setLevel(logging.DEBUG)

# 创建控制台处理器
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)

# 创建格式器并添加到处理器
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
handler.setFormatter(formatter)
console_handler.setFormatter(formatter)

# 将处理器添加到logger
logger.addHandler(handler)
logger.addHandler(console_handler)

with open('config.yaml', 'r') as f:
    config = yaml.safe_load(f)

wx_db=db.Db()    
wx_db.init(config["db"])
mps=wx_db.get_all_mps()
print(mps)
def do_job():
    print("执行任务")
    for item in mps:
        faker_id=item.faker_id
        mp_id=item.id
        print(item,faker_id,mp_id)
        data=wx.get_list(faker_id,mp_id,1)
    logger.info("开始执行函数")
def start():
    schedule.every(10).seconds.do(do_job)  # 每10秒执行
    # schedule.every.day.at("08:00").do(job)  # 每天8点执行
    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == '__main__':
    # start()
    do_job()
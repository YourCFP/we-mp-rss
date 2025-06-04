from core.models.user import User
from core.models.article import Article
from core.models.config_management import ConfigManagement
from core.models.feed import Feed
from core.models.message_task import MessageTask
import core.db as db
from core.config import cfg
from core.auth import pwd_context
from core.data_sync import DataSync
import time
import os
from core.print import print_info, print_error
def init_user(_db: db.Db):
    try:
      username,password=os.getenv("USERNAME", "admin"),os.getenv("PASSWORD", "admin@123")
      _db.create_tables()
     
      _db.session.merge(User(
          id=0,
          username=username,
          password_hash=pwd_context.hash(password),
          ))
      _db.session.commit()
    except Exception as e:
        print_error(f"Init error: {str(e)}")
        pass
def sync_models():
     # 同步模型到表结构
      data_sync = DataSync(bk_db, db.DB)
      models = [User, Article, ConfigManagement, Feed, MessageTask]
      for model in models:
          if not data_sync.sync_model_to_table(model):
              print_error(f"Failed to sync model {model.__name__} to table")
          else:
              print_info(f"Successfully synced model {model.__name__} to table")
import core.db as db
bk_db=db.Db()

def back_init_data():
    os.remove("./init.db")
    # 初始化数据
    bk_db.init("sqlite:///init.db")
    bk_db.create_tables()
    init_user(bk_db)
    #数据结构处理完成
    print_info("数据结构处理完成")
    pass
 
def init():
    back_init_data()
    init_user(db.DB)
    sync_models()

if __name__ == '__main__':
    init()
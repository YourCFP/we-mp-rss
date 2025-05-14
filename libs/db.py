from sqlalchemy import create_engine, Column, Integer, String, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from typing import Optional, List

Base = declarative_base()

class Article(Base):
    __tablename__ = 'articles'
    id = Column(String(255), primary_key=True)
    mp_id = Column(String(255))
    title = Column(String(500))
    pic_url = Column(String(500))
    publish_time = Column(Integer)
    created_at = Column(DateTime)
    updated_at = Column(DateTime)  # Changed from Integer to DateTime for consistency
    is_export = Column(Integer)

class Feeds(Base):
    __tablename__ = 'feeds'
    id = Column(String(255), primary_key=True)
    mp_name = Column(String(255))
    mp_cover = Column(String(255))
    mp_intro = Column(String(255))
    status = Column(Integer)
    sync_time = Column(DateTime)
    update_time = Column(DateTime)
    created_at = Column(DateTime) 
    updated_at = Column(DateTime)
    faker_id = Column(String(255))

class Db:
    def __init__(self):
        self.session: Optional[sessionmaker] = None
        
    def init(self, con_str: str) -> None:
        """Initialize database connection
        
        Args:
            con_str: Database connection string
            
        Raises:
            Exception: If connection fails
        """
        try:
            engine = create_engine(con_str)
            Session = sessionmaker(bind=engine)
            self.session = Session()
        except Exception as e:
            print(f"Error creating database connection: {e}")
            
    def close(self) -> None:
        """Close the database connection"""
        if self.session:
            self.session.close()
            
    def __enter__(self):
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
            
    def add_article(self, article_data: dict) -> None:
        try:
            art = Article(**article_data)
            self.session.add(art) 
            self.session.commit()
        except Exception as e:
            self.session.rollback()
            print(f"Failed to add article: {e}")
            return e
            
    def get_articles(self,id:str=None,limit:int=30,offset:int=0) -> List[Article]:
         try:
            #  data=self.session.query(Feeds).filter_by(id=id).limit(10)
             data=self.session.query(Article).limit(limit).offset(offset)
             return data
         except Exception as e:
             print(f"Failed to fetch feeds: {e}")
             return e    
             
    def get_all_mps(self) -> List[Feeds]:
         """Get all Feeds records"""
         try:
             return self.session.query(Feeds).all()
         except Exception as e:
             print(f"Failed to fetch feeds: {e}")
             return e
    def get_mps(self,mp_id:str)->Optional[Feeds]:
         try:
             data=self.session.query(Feeds).filter_by(id=mp_id).first()
            #  return type(data)
             return  data
         except Exception as e:
             print(f"Failed to fetch feeds: {e}")
             return e

    def get_faker_id(self,mp_id:str):
        data= self.get_mps(mp_id)
        return data.faker_id

from fastapi import APIRouter, Request, Depends, Query, HTTPException
from fastapi.responses import HTMLResponse
from typing import Optional
import os
from datetime import datetime

from core.db import DB
from core.models.article import Article
from core.models.feed import Feed
from core.models.tags import Tags
from apis.base import format_search_kw
from core.lax.template_parser import TemplateParser
from views.config import base
# 创建路由器
router = APIRouter(tags=["文章"])

@router.get("/articles", response_class=HTMLResponse, summary="文章列表页")
async def articles_view(
    request: Request,
    page: int = Query(1, ge=1, description="页码"),
    limit: int = Query(20, ge=1, le=100, description="每页数量"),
    mp_id: Optional[str] = Query(None, description="公众号ID筛选"),
    tag_id: Optional[str] = Query(None, description="标签ID筛选"),
    keyword: Optional[str] = Query(None, description="关键词搜索"),
    sort: str = Query("publish_time", description="排序方式: publish_time, created_at"),
    order: str = Query("desc", description="排序顺序: asc, desc")
):
    """
    文章列表页面，支持筛选、搜索和排序
    """
    session = DB.get_session()
    try:
        # 构建查询
        query = session.query(Article, Feed).join(Feed, Article.mp_id == Feed.id)
        
        # 基础筛选条件
        query = query.filter(Article.status == 1)
        
        # 公众号筛选
        if mp_id:
            query = query.filter(Article.mp_id == mp_id)
        
        # 标签筛选
        mps_ids = []
        if tag_id:
            tag = session.query(Tags).filter(Tags.id == tag_id, Tags.status == 1).first()
            if tag and tag.mps_id:
                import json
                try:
                    mps_data = json.loads(tag.mps_id)
                    mps_ids = [str(mp['id']) for mp in mps_data] if isinstance(mps_data, list) else []
                except (json.JSONDecodeError, TypeError):
                    mps_ids = []
                
                if mps_ids:
                    query = query.filter(Article.mp_id.in_(mps_ids))
        
        # 关键词搜索
        if keyword and keyword.strip():
            search_filter = format_search_kw(keyword.strip())
            if search_filter is not None:
                query = query.filter(search_filter)
        
        # 排序
        if sort == "publish_time":
            order_by = Article.publish_time.desc() if order == "desc" else Article.publish_time.asc()
        elif sort == "created_at":
            order_by = Article.created_at.desc() if order == "desc" else Article.created_at.asc()
        else:
            order_by = Article.publish_time.desc()
        
        query = query.order_by(order_by)
        
        # 查询总数
        total = query.count()
        
        # 分页
        offset = (page - 1) * limit
        articles = query.offset(offset).limit(limit).all()
        
        # 处理文章数据
        article_list = []
        for article, feed in articles:
            article_data = {
                "id": article.id,
                "title": article.title,
                "description": article.description,
                "pic_url": article.pic_url,
                "url": article.url,
                "publish_time": datetime.fromtimestamp(article.publish_time).strftime('%Y-%m-%d %H:%M') if article.publish_time else "",
                "created_at": article.created_at.strftime('%Y-%m-%d %H:%M') if article.created_at else "",
                "mp_name": feed.mp_name if feed else "未知公众号",
                "mp_id": article.mp_id,
                "mp_cover": feed.mp_cover if feed else "",
                "is_read": bool(article.is_read),
                "content": article.content[:200] + "..." if article.content and len(article.content) > 200 else (article.content or "")
            }
            article_list.append(article_data)
        
        # 获取筛选信息
        filter_info = {}
        if mp_id:
            mp = session.query(Feed).filter(Feed.id == mp_id).first()
            if mp:
                filter_info["mp"] = {"id": mp.id, "name": mp.mp_name}
        
        if tag_id:
            tag = session.query(Tags).filter(Tags.id == tag_id).first()
            if tag:
                filter_info["tag"] = {"id": tag.id, "name": tag.name}
        
        # 获取所有标签用于筛选下拉
        tags = session.query(Tags).filter(Tags.status == 1).order_by(Tags.name).all()
        tag_options = [{"id": tag.id, "name": tag.name} for tag in tags]
        
        # 获取热门公众号（文章数量最多的前10个）
        from sqlalchemy import func
        popular_mps = session.query(
            Feed.id, Feed.mp_name, Feed.mp_cover,
            func.count(Article.id).label('article_count')
        ).outerjoin(
            Article, Article.mp_id == Feed.id
        ).filter(
            Article.status == 1
        ).group_by(
            Feed.id, Feed.mp_name, Feed.mp_cover
        ).having(
            func.count(Article.id) > 0
        ).order_by(
            func.count(Article.id).desc()
        ).limit(10).all()
        
        mp_options = [{"id": str(feed_id), "name": mp_name} for feed_id, mp_name, _, _ in popular_mps]
        
        # 计算分页信息
        total_pages = (total + limit - 1) // limit
        has_prev = page > 1
        has_next = page < total_pages
        
        # 构建面包屑
        breadcrumb = [
            {"name": "首页", "url": "/views/home"},
            {"name": "文章列表", "url": None}
        ]
        
        # 读取模板文件
        template_path = base.articles_template
        with open(template_path, 'r', encoding='utf-8') as f:
            template_content = f.read()
        
        parser = TemplateParser(template_content, template_dir=base.public_dir)
        html_content = parser.render({
            "articles": article_list,
            "current_page": page,
            "total_pages": total_pages,
            "total_items": total,
            "limit": limit,
            "has_prev": has_prev,
            "has_next": has_next,
            "filter_info": filter_info,
            "tag_options": tag_options,
            "mp_options": mp_options,
            "current_filters": {
                "mp_id": mp_id,
                "tag_id": tag_id,
                "keyword": keyword,
                "sort": sort,
                "order": order
            },
            "breadcrumb": breadcrumb
        })
        
        return HTMLResponse(content=html_content)
        
    except Exception as e:
        print(f"获取文章列表错误: {str(e)}")
        # 读取模板文件
        template_path=base.articles_template
        with open(template_path, 'r', encoding='utf-8') as f:
            template_content = f.read()
        
        parser = TemplateParser(template_content, template_dir=base.public_dir)
        html_content = parser.render({
            "error": f"加载数据时出现错误: {str(e)}",
            "breadcrumb": [{"name": "首页", "url": "/views/home"}, {"name": "文章列表", "url": None}]
        })
        
        return HTMLResponse(content=html_content)
    finally:
        session.close()

@router.get("/article/{article_id}", response_class=HTMLResponse, summary="文章详情页")
async def article_detail_view(
    request: Request,
    article_id: str
):
    """
    文章详情页面
    """
    session = DB.get_session()
    try:
        # 查询文章信息
        article_query = session.query(Article, Feed).join(
            Feed, Article.mp_id == Feed.id
        ).filter(Article.id == article_id, Article.status == 1).first()
        
        if not article_query:
            raise HTTPException(status_code=404, detail="文章不存在")
        
        article, feed = article_query
        
        # 标记为已读（可选）
        if not article.is_read:
            article.is_read = 1
            session.commit()
        
        # 获取相关文章（同公众号的其他文章）
        related_articles = session.query(Article).filter(
            Article.mp_id == article.mp_id,
            Article.id != article_id,
            Article.status == 1
        ).order_by(Article.publish_time.desc()).limit(5).all()
        
        related_list = []
        for rel_article in related_articles:
            rel_data = {
                "id": rel_article.id,
                "title": rel_article.title,
                "description": rel_article.description,
                "pic_url": rel_article.pic_url,
                "publish_time": datetime.fromtimestamp(rel_article.publish_time).strftime('%Y-%m-%d %H:%M') if rel_article.publish_time else ""
            }
            related_list.append(rel_data)
        
        # 处理文章数据
        article_data = {
            "id": article.id,
            "title": article.title,
            "description": article.description,
            "pic_url": article.pic_url,
            "url": article.url,
            "publish_time": datetime.fromtimestamp(article.publish_time).strftime('%Y-%m-%d %H:%M') if article.publish_time else "",
            "created_at": article.created_at.strftime('%Y-%m-%d %H:%M') if article.created_at else "",
            "content": article.content or "",
            "mp_name": feed.mp_name if feed else "未知公众号",
            "mp_id": article.mp_id,
            "mp_cover": feed.mp_cover if feed else "",
            "mp_intro": feed.mp_intro if feed else ""
        }
        
        # 构建面包屑
        breadcrumb = [
            {"name": "首页", "url": "/views/home"},
            {"name": "文章列表", "url": "/views/articles"},
            {"name": article_data["title"][:50] + "..." if len(article_data["title"]) > 50 else article_data["title"], "url": None}
        ]
        
        # 读取模板文件
        template_path=base.article_detail_template
        with open(template_path, 'r', encoding='utf-8') as f:
            template_content = f.read()
        
        parser = TemplateParser(template_content, template_dir=base.public_dir)
        html_content = parser.render({
            "article": article_data,
            "related_articles": related_list,
            "breadcrumb": breadcrumb
        })
        
        return HTMLResponse(content=html_content)
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"获取文章详情错误: {str(e)}")
        # 读取模板文件
        template_path = base.article_detail_template
        with open(template_path, 'r', encoding='utf-8') as f:
            template_content = f.read()
        
        parser = TemplateParser(template_content, template_dir=base.public_dir)
        html_content = parser.render({
            "error": f"加载文章时出现错误: {str(e)}",
            "breadcrumb": [{"name": "首页", "url": "/views/home"}, {"name": "文章列表", "url": "/views/articles"}]
        })
        
        return HTMLResponse(content=html_content)
    finally:
        session.close()
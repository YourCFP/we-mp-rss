from fastapi import APIRouter, Request, Depends, Query, HTTPException
from fastapi.responses import HTMLResponse
from typing import Optional
import os
from datetime import datetime
import re
import json

from core.db import DB
from core.models.article import Article
from core.models.feed import Feed
from core.models.tags import Tags
from apis.base import format_search_kw
from core.lax.template_parser import TemplateParser
from views.config import base
from driver.wxarticle import Web
from core.cache import cache_view, clear_cache_pattern

def _render_template_with_error(template_path: str, error_msg: str, breadcrumb: list) -> HTMLResponse:
    """渲染错误页面的辅助函数"""
    try:
        with open(template_path, 'r', encoding='utf-8') as f:
            template_content = f.read()
        
        parser = TemplateParser(template_content, template_dir=base.public_dir)
        html_content = parser.render({
            "error": error_msg,
            "breadcrumb": breadcrumb
        })
        return HTMLResponse(content=html_content)
    except Exception:
        return HTMLResponse(content=f"<h1>系统错误</h1><p>{error_msg}</p>")

def process_content_images(content: str) -> str:
    """处理文章内容中的图片链接，添加前缀"""
    if not content:
        return content
    return Web.proxy_images(content)

# 创建路由器
router = APIRouter(tags=["文章"])

@router.get("/articles", response_class=HTMLResponse, summary="文章列表页")
@cache_view("articles_list", ttl=1800)  # 缓存30分钟
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
        # 构建查询 - 先筛选再join以提高性能
        query = session.query(Article)
        
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
        
        # 验证排序参数
        valid_sort_fields = {"publish_time", "created_at"}
        valid_orders = {"asc", "desc"}
        
        if sort not in valid_sort_fields:
            sort = "publish_time"
        if order not in valid_orders:
            order = "desc"
        
        # 排序
        if sort == "publish_time":
            order_by = Article.publish_time.desc() if order == "desc" else Article.publish_time.asc()
        elif sort == "created_at":
            order_by = Article.created_at.desc() if order == "desc" else Article.created_at.asc()
        else:
            order_by = Article.publish_time.desc()
        
        query = query.order_by(order_by)
        
        # 查询总数（在join之前）
        total = query.count()
        
        # 分页
        offset = (page - 1) * limit
        articles_data = query.offset(offset).limit(limit).all()
        
        # 获取关联的Feed信息
        mp_ids = [article.mp_id for article in articles_data]
        feeds_dict = {feed.id: feed for feed in session.query(Feed).filter(Feed.id.in_(mp_ids)).all()} if mp_ids else {}
        # 组装数据
        articles = [(article, feeds_dict.get(article.mp_id)) for article in articles_data]
        
        # 处理文章数据
        article_list = []
        for article, feed in articles:
            article_data = {
                "id": article.id,
                "title": article.title,
                "description": article.description or Web.get_description(article.content),
                "pic_url": Web.get_image_url(article.pic_url),
                "url": article.url,
                "publish_time": datetime.fromtimestamp(article.publish_time).strftime('%Y-%m-%d %H:%M') if article.publish_time else "",
                "created_at": article.created_at.strftime('%Y-%m-%d %H:%M') if article.created_at else "",
                "mp_name": feed.mp_name if feed else "未知公众号",
                "mp_id": article.mp_id,
                "mp_cover": Web.get_image_url(feed.mp_cover) if feed else "",
                "is_read": bool(article.is_read),
            }
            article_list.append(article_data)
        
        # 获取筛选信息 - 合并查询提高性能
        filter_info = {}
        if mp_id or tag_id:
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
        
        # 获取热门公众号（文章数量最多的前10个）- 使用子查询提高性能
        from sqlalchemy import func, text
        
        # 先计算文章数量，再join减少计算量
        article_counts = session.query(
            Article.mp_id,
            func.count(Article.id).label('article_count')
        ).filter(
            Article.status == 1
        ).group_by(
            Article.mp_id
        ).subquery()
        
        popular_mps = session.query(
            Feed.id, Feed.mp_name, Feed.mp_cover,
            article_counts.c.article_count
        ).join(
            article_counts, Feed.id == article_counts.c.mp_id
        ).filter(
            article_counts.c.article_count > 0
        ).order_by(
            article_counts.c.article_count.desc()
        ).limit(10).all()
        
        mp_options = [{"id": str(row[0]), "name": row[1]} for row in popular_mps] if popular_mps else []
        
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
        feed_info=feeds_dict.get(mp_id) if mp_id else None
        info={
            "mp_name": feed_info.mp_name if feed_info else "",
            "mp_cover": Web.get_image_url(feed_info.mp_cover) if feed_info else "",
            "mp_intro": feed_info.mp_intro if feed_info else "",
            "mp_id": mp_id,
        } if feed_info else ""
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
            "info": info,
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
        return _render_template_with_error(
            base.articles_template,
            f"加载数据时出现错误: {str(e)}",
            [{"name": "首页", "url": "/views/home"}, {"name": "文章列表", "url": None}]
        )
    finally:
        session.close()

@router.get("/article/{article_id}", response_class=HTMLResponse, summary="文章详情页")
@cache_view("article_detail", ttl=3600)  # 缓存1小时
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
        ).filter(Article.id == article_id, Article.status == 1, Feed.status == 1).first()
        
        if not article_query:
            raise HTTPException(status_code=404, detail="文章不存在")
        
        if len(article_query) != 2:
            raise HTTPException(status_code=500, detail="数据查询错误")
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
                "description": rel_article.description or Web.get_description(rel_article.content),
                "pic_url": Web.get_image_url(rel_article.pic_url),
                "publish_time": datetime.fromtimestamp(rel_article.publish_time).strftime('%Y-%m-%d %H:%M') if rel_article.publish_time else ""
            }
            related_list.append(rel_data)
        
        # 处理文章数据
        article_data = {
            "id": article.id,
            "title": article.title,
            "description": article.description or Web.get_description(article.content),
            "pic_url": Web.get_image_url(article.pic_url),
            "url": article.url,
            "publish_time": datetime.fromtimestamp(article.publish_time).strftime('%Y-%m-%d %H:%M') if article.publish_time else "",
            "created_at": article.created_at.strftime('%Y-%m-%d %H:%M') if article.created_at else "",
            "content": process_content_images(article.content or ""),
            "mp_name": feed.mp_name if feed else "未知公众号",
            "mp_id": article.mp_id,
            "mp_cover": Web.get_image_url(feed.mp_cover) if feed else "",
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
        return _render_template_with_error(
            base.article_detail_template,
            f"加载文章时出现错误: {str(e)}",
            [{"name": "首页", "url": "/views/home"}, {"name": "文章列表", "url": "/views/articles"}]
        )
    finally:
        session.close()
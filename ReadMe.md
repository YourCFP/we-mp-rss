# WeRSS - 微信公众号订阅助手

[![Python Version](https://img.shields.io/badge/python-3.8+-blue.svg)]()
[![License](https://img.shields.io/badge/license-MIT-green.svg)]()

一个用于订阅和管理微信公众号内容的工具，提供RSS订阅功能。

## 功能特性

- 微信公众号内容抓取和解析
- RSS订阅生成
- 用户友好的Web管理界面
- 定时自动更新内容

## 界面截图

- 登录界面  
  ![登录界面](docs/登录.png)

- 主界面  
  ![主界面](docs/主界面.png)

- 添加订阅  
  ![添加订阅](docs/添加订阅.png)

- 扫码授权  
  ![扫码授权](docs/扫码授权.png)

## 系统架构

项目采用前后端分离架构：
- 后端：Python + FastAPI
- 前端：Vue 3 + Vite
- 数据库：SQLite (默认)/MySQL

## 安装指南

### 后端服务

1. 克隆项目
```bash
git clone https://github.com/rachelos/we-mp-rss.git
cd we-mp-rss
```

2. 安装Python依赖
```bash
pip install -r requirements.txt
```

3. 配置数据库
复制并修改配置文件：
```bash
cp config.example.yaml config.yaml
```

4. 启动API服务
```bash
uvicorn web:app --host 0.0.0.0 --port 8001 --reload
```

### 前端界面

1. 进入web_ui目录
```bash
cd web_ui
```

2. 安装Node.js依赖
```bash
npm install
```

3. 启动开发服务器
```bash
npm run dev
```

## 定时任务

配置定时抓取微信公众号内容：
```bash
python job.py
```

## 配置说明

编辑`config.yaml`文件配置以下参数：
- 数据库连接
- 微信公众号配置
- 抓取间隔时间
- API密钥等

## API文档

API服务启动后，访问以下地址查看文档：
- Swagger UI: http://localhost:8001/docs
- Redoc: http://localhost:8001/redoc

## 快速运行
### Docker Sqlite
```
docker run -d \
  --name we-mp-rss \
  -p 8001:8001 \
  -e DB=sqlite:///db.db \
  -e USERNAME:admin \
  -e PASSWORD:admin@123 \
  -e DINGDING_WEBHOOK=https://oapi.dingtalk.com/robot/send?access_token=xxx \
  -v $(pwd)/data:/app/data \
  ghcr.io/rachelos/we-mp-rss:latest

```
### Docker Mysql
```
docker run -d \
  --name we-mp-rss \
  -p 8001:8001 \
  -e DB=mysql+pymysql://<username>:<password>@<host>/<database> \
  -e USERNAME:admin \
  -e PASSWORD:admin@123 \
  -e DINGDING_WEBHOOK=https://oapi.dingtalk.com/robot/send?access_token=xxx \
  -v $(pwd)/data:/app/data \
  ghcr.io/rachelos/we-mp-rss:latest
```

# Docker构建及运行
```
# 构建
docker build -t we-mp-rss .
# Docker运行
docker run -d --name we-mp-rss -p 8001:8001 we-mp-rss
```

## 开发指南

### 后端开发
1. 安装开发依赖
```bash
pip install -r requirements-dev.txt
```

2. 运行测试
```bash
pytest
```

### 前端开发
1. 修改环境变量
编辑`.env.development`文件

2. 开发模式
```bash
npm run dev
```

3. 构建生产版本
```bash
npm run build
```




## 贡献指南

欢迎提交Pull Request。在提交前请确保：
1. 代码通过所有测试
2. 更新相关文档
3. 遵循代码风格指南

# 友情链接
[WeWe RSS](https://github.com/cooderl/wewe-rss)

## 许可证

MIT License
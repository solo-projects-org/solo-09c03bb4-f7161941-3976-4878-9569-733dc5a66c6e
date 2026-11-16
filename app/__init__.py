"""海洋数仓浮标观测包接收服务。

模块划分：
- config:     环境变量配置
- db:         SQLite 连接、建表与事务
- storage:    内容寻址存储、配额、文件锁、崩溃回收
- validation: Parquet 原包校验
- service:    接收流程编排（声明 / 上传 / 发布 / 中止）
- main:       FastAPI 路由与 Swagger
"""

__version__ = "1.0.0"

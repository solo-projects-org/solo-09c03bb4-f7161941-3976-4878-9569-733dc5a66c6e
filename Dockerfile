FROM python:3.11-slim

# 不写 .pyc、stdout 直出，便于容器日志排查
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /srv/app

# 先装依赖，利用镜像层缓存
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 拷贝应用代码
COPY app ./app

# 数据卷：SQLite、暂存、成品、锁文件全部在此，双实例共享
ENV OBS_DATA_DIR=/data \
    OBS_QUOTA_BYTES=1073741824 \
    OBS_RECONCILE_INTERVAL=30 \
    OBS_CHUNK_SIZE=1048576

EXPOSE 8000

# 容器级健康检查：轻量 HTTP 探活
HEALTHCHECK --interval=15s --timeout=5s --start-period=10s --retries=5 \
    CMD python -c "import urllib.request,sys; \
sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8000/health', timeout=4).status==200 else 1)"

# 单容器单进程；双实例由 compose 编排为两个副本
ENTRYPOINT ["uvicorn"]
CMD ["app.main:app", "--host", "0.0.0.0", "--port", "8000", "--no-access-log"]

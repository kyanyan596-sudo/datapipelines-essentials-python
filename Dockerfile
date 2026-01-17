FROM python:3.10-slim-bookworm


# 安装 Java 17（适配 Debian trixie / bookworm）
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# 设置 JAVA_HOME（这是“容器内”的环境变量）
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"


# 2) 先复制 requirements 以便利用缓存
COPY requirements.txt /app/requirements.txt

# 如果 requirements 里有 hash 或装包很重，建议先只装最小集合跑通
RUN pip install -U pip \
    && pip install -r /app/requirements.txt || true \
    && pip install pyspark py4j

# 3) 复制项目代码
COPY . /app

COPY local_test_data /app/local_test_data


# 4) 默认入口（先用 local_run.py 验证）
CMD ["python", "/app/local_run.py"]


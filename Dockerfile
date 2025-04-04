# 使用 rapidsai/base:25.04a-cuda12.8-py3.11 作为基础镜像
FROM rapidsai/base:25.04a-cuda12.8-py3.11

# 设置工作目录
WORKDIR /workspace

# 安装 pip 包
RUN pip install --no-cache-dir \
    ib_insync \
    redis \
    dill \
    mplfinance

# 安装 conda 包
RUN conda install -c conda-forge ta-lib

FROM jupyter/scipy-notebook:python-3.8.8

USER root

RUN apt-get update --yes && \
    apt-get install --yes --no-install-recommends openjdk-8-jre-headless && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

USER ${NB_UID}

RUN pip install --no-cache-dir -i https://mirrors.aliyun.com/pypi/simple/ \
    'pyalink-flink-1.9==1.5.0' && \
    fix-permissions "${CONDA_DIR}" && \
    fix-permissions "/home/${NB_USER}"

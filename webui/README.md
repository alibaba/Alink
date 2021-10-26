# 快速上手: #
1. 参考 [https://docs.docker.com/get-docker/](https://docs.docker.com/get-docker/) 安装 Docker；
2. 参考 [https://umijs.org/zh-CN/docs/getting-started](https://umijs.org/zh-CN/docs/getting-started) 准备好 node 和 yarn；
3. 克隆 Alink 代码：
``` shell
git clone https://github.com/alibaba/Alink.git alink
cd alink
```

4. 下载 shade 后的 Alink 包到 webui/tools/server/
``` shell
wget https://alink-release.oss-cn-beijing.aliyuncs.com/v1.5.0/alink_core_flink-1.9_2.11-1.5.0.jar -P webui/tools/server/
```
注意：这里也可以使用 flink-1.9 版本的 Alink 代码分支进行编译：
``` shell
mvn -Dmaven.test.skip=true clean package shade:shade
cp core/target/alink_core_flink-1.9_2.11-[xxx].jar webui/tools/server/
```

5. 进入到目录 webui/web/，执行 yarn：
``` shell
cd webui/web/
yarn
```

6. 进入到目录 webui/tools/，执行 sh build-image.sh，等待编译 docker 镜像；
``` shell
cd ../tools
sh build-image.sh
```

7. 编译成功后，进入到目录 webui/tools/docker-compose/alink/nfs/，执行 docker-compose up -d，这将启动一个 NFS 服务；
``` shell
cd docker-compose/alink/nfs/
docker-compose up -d
```

8. 进入到目录 webui/tools/docker-compose/alink，执行 docker-compose up -d，这将启动 Alink 的 server、web、notebook以及数据库等服务；
``` shell
cd ..
docker-compose up -d
```

9. 在浏览器打开 [localhost:9090](localhost:9090)，稍微等待一些时间，就能开始使用了。

# 开发步骤 #
为了简化步骤，可以直接复用“快速上手”中已经启动的数据库、Server、Web 等部件，从而能减少环境的配置，只关注需要开发的部分。

## Web 开发：
1. 请将快速上手中的所有步骤跑通；
2. 将启动的 Web 服务杀掉：
``` shell
WEB_CONTAINER_ID=$(docker ps -a -q --filter="name=alink_web")
docker kill $WEB_CONTAINER_ID && docker rm $WEB_CONTAINER_ID
```

3. 通过代码启动 Web 服务：
``` shell
cd webui/web/
yarn start
```

4. 在浏览器打开 [localhost:8000](localhost:8000)，可以实时预览 Web 代码的修改。

## Server 开发：
1. 请将快速上手中的所有步骤跑通；
2. 将启动的 Server 服务杀掉；
``` shell
SERVER_CONTAINER_ID=$(docker ps -a -q --filter="name=alink_server")
docker kill $SERVER_CONTAINER_ID && docker rm $SERVER_CONTAINER_ID
```

3. 使用 Intellij IDEA 等工具打开 Server 部分的代码 webui/server/，启动 com.alibaba.alink.server.ServerApplication 类。

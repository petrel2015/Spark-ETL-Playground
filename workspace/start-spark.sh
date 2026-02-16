#!/bin/bash

# ==============================================================================
# Spark Client & History Server 启动脚本
# 负责环境配置、启动 History Server 以及自动提交 Spark 任务
# ==============================================================================

# --- 1. 安装必要的系统组件 ---
apt-get install -y bash --no-install-recommends > /dev/null 2>&1

# --- 2. 配置环境变量 ---
echo ">>> [1/6] 注入系统级与用户级环境变量..."

# 2.1 强制设置系统级环境变量 (确保所有 Shell 实例生效)
cat >> /etc/bash.bashrc <<EOF
export SPARK_HOME=/opt/spark
export HADOOP_CONF_DIR=/etc/hadoop
export PATH=\$PATH:\$SPARK_HOME/bin:\$SPARK_HOME/sbin
alias hadoop="java -cp '/opt/spark/jars/*:/etc/hadoop' org.apache.hadoop.fs.FsShell"
EOF

# 2.2 设置用户级 PATH (方便 docker exec 使用)
cat >> ~/.bashrc <<'EOF'
export SPARK_HOME=/opt/spark
export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
EOF

source ~/.bashrc

# --- 3. 准备 Hadoop/Spark 配置文件 ---
echo ">>> [2/6] 配置 Hadoop 客户端与 Spark 默认参数..."

# 3.1 强制建立 Hadoop 客户端配置 (连接 HDFS/YARN 必须)
mkdir -p /etc/hadoop
echo '<configuration><property><name>fs.defaultFS</name><value>hdfs://master:9000</value></property></configuration>' > /etc/hadoop/core-site.xml
echo '<configuration><property><name>yarn.resourcemanager.hostname</name><value>master</value></property></configuration>' > /etc/hadoop/yarn-site.xml
export HADOOP_CONF_DIR=/etc/hadoop

# 3.2 配置 Spark History Server (指向 HDFS 日志路径)
mkdir -p /opt/spark/conf
cat > /opt/spark/conf/spark-defaults.conf <<EOF
spark.eventLog.enabled             true
spark.eventLog.rolling.enabled     true
spark.eventLog.rolling.maxFileSize 128m
spark.eventLog.compress            false
spark.eventLog.dir                 file:///opt/workspace/eventlog
spark.history.fs.logDirectory      file:///opt/workspace/eventlog
spark.yarn.historyServer.address   http://spark:18080
spark.history.ui.port              18080
EOF

# --- 4. 等待 HDFS 服务就绪 ---
# History Server 需要读取 HDFS，必须确保 Master 端口已开放
echo ">>> [3/6] 检查 HDFS 联通性 (master:9000)..."
until (echo > /dev/tcp/master/9000) >/dev/null 2>&1; do
  echo "等待 HDFS Master 启动中..."
  sleep 2
done

# --- 5. 启动 History Server ---
echo ">>> [4/6] 启动 Spark History Server..."
/opt/spark/sbin/start-history-server.sh

# --- 6. 提交测试任务并维持容器运行 ---
echo ">>> [5/6] 正在后台提交 Spark 任务至 YARN..."
echo ">>> 等待60s（等待hdfs数据文件上传就绪）"
sleep 60
echo ">>> 开始提交"
/opt/spark/bin/spark-submit \
--class com.example.spark.AnonymizedWebAnalyticsApp \
--master yarn \
--deploy-mode client \
--num-executors 4 \
--executor-cores 1 \
--executor-memory 512m \
/opt/workspace/sparkapp/spark-etl-playground-1.1.0.jar &

echo ">>> [6/6] ✅ 启动完成，正在追踪 History Server 日志..."
# 等待日志生成
tail -f /opt/spark/logs/*org.apache.spark.deploy.history.HistoryServer*.out
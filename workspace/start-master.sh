#!/bin/bash

# ==============================================================================
# Hadoop Master 启动脚本
# 负责初始化 NameNode, ResourceManager 以及基础 HDFS 目录结构
# ==============================================================================

# --- 1. 配置环境变量 (确保交互式 Shell 可用) ---
echo ">>> [1/7] 配置环境变量..."
if ! grep -q "hadoop/bin" ~/.bashrc; then
  echo 'export HADOOP_HOME=/opt/hadoop' >> ~/.bashrc
  echo 'export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin' >> ~/.bashrc
fi
source ~/.bashrc

export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop

# --- 2. 强制初始化配置文件 ---
# 确保集群内各节点配置一致，指向 master:9000
echo ">>> [2/7] 生成 Hadoop 核心配置文件 (core-site.xml & yarn-site.xml)..."

cat > $HADOOP_CONF_DIR/core-site.xml <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <property><name>fs.defaultFS</name><value>hdfs://master:9000</value></property>
    <property><name>hadoop.security.authorization</name><value>false</value></property>
    <property><name>hadoop.security.authentication</name><value>simple</value></property>
    <property><name>hadoop.proxyuser.root.hosts</name><value>*</value></property>
    <property><name>hadoop.proxyuser.root.groups</name><value>*</value></property>
</configuration>
EOF

cat > $HADOOP_CONF_DIR/yarn-site.xml <<EOF
<?xml version="1.0"?>
<configuration>
    <property><name>yarn.resourcemanager.hostname</name><value>master</value></property>
    <property><name>yarn.nodemanager.vmem-check-enabled</name><value>false</value></property>
    <property><name>yarn.nodemanager.pmem-check-enabled</name><value>false</value></property>
    <property><name>yarn.scheduler.minimum-allocation-mb</name><value>128</value></property>
    <property><name>yarn.nodemanager.resource.memory-mb</name><value>8192</value></property>
    <property><name>yarn.log-aggregation-enable</name><value>true</value></property>
    <property><name>yarn.nodemanager.remote-app-log-dir</name><value>/tmp/logs</value></property>
    <property><name>yarn.resourcemanager.webapp.cross-origin.enabled</name><value>true</value></property>
</configuration>
EOF

# --- 3. 检查并格式化 NameNode ---
# 仅在第一次启动时进行格式化，防止数据丢失
echo ">>> [3/7] 检查 NameNode 状态..."
if [ ! -d "/tmp/hadoop-root/dfs/name/current" ]; then
    echo "首次运行，正在执行 NameNode 格式化..."
    /opt/hadoop/bin/hdfs --config $HADOOP_CONF_DIR namenode -format -force
fi

# --- 4. 启动 Hadoop Master 服务 ---
echo ">>> [4/7] 启动 NameNode 与 ResourceManager..."
/opt/hadoop/bin/hdfs --config $HADOOP_CONF_DIR --daemon start namenode
/opt/hadoop/bin/yarn --config $HADOOP_CONF_DIR --daemon start resourcemanager

# --- 5. 等待 HDFS 进入可用状态 ---
# 防止后续 HDFS 操作因处于安全模式 (Safemode) 而失败
echo ">>> [5/7] 等待 HDFS 退出安全模式..."
until /opt/hadoop/bin/hdfs --config $HADOOP_CONF_DIR dfsadmin -safemode wait; do
    echo "HDFS 启动中，等待安全模式释放..."
    sleep 3
done

# --- 6. 初始化 HDFS 目录结构 ---
echo ">>> [6/7] 配置 HDFS 基础目录 (Spark EventLog & Data)..."
# 创建 Spark 历史日志目录
# /opt/hadoop/bin/hdfs --config $HADOOP_CONF_DIR dfs -mkdir -p /user/spark/eventlog
# /opt/hadoop/bin/hdfs --config $HADOOP_CONF_DIR dfs -chmod 1777 /user/spark/eventlog

# 上传测试数据至 HDFS
/opt/hadoop/bin/hdfs --config $HADOOP_CONF_DIR dfs -mkdir /user
/opt/hadoop/bin/hdfs --config $HADOOP_CONF_DIR dfs -put -f /opt/workspace/data /user/
/opt/hadoop/bin/hdfs --config $HADOOP_CONF_DIR dfs -chmod -R 1777 /user/data

# --- 7. 维持容器运行 ---
echo ">>> [7/7] ✅ Hadoop Master 已就绪，开始追踪服务日志..."
mkdir -p /opt/hadoop/logs
touch /opt/hadoop/logs/hadoop-master-namenode-master.log
tail -f /opt/hadoop/logs/*.log

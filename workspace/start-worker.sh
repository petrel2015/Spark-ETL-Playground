#!/bin/bash

# ==============================================================================
# Hadoop Worker 启动脚本
# 负责启动 DataNode 与 NodeManager 以供集群计算和存储
# ==============================================================================

# --- 1. 配置环境变量 (确保交互式 Shell 可用) ---
echo ">>> [1/4] 配置环境变量..."
if ! grep -q "hadoop/bin" ~/.bashrc; then
  echo 'export HADOOP_HOME=/opt/hadoop' >> ~/.bashrc
  echo 'export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin' >> ~/.bashrc
fi
source ~/.bashrc

export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop

# --- 2. 强制初始化配置文件 ---
# 同步集群配置，确保 Worker 指向正确的 Master 节点
echo ">>> [2/4] 同步 Hadoop 核心配置文件..."

cat > $HADOOP_CONF_DIR/core-site.xml <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
    <property><name>fs.defaultFS</name><value>hdfs://master:9000</value></property>
    <property><name>hadoop.security.authorization</name><value>false</value></property>
    <property><name>hadoop.security.authentication</name><value>simple</value></property>
</configuration>
EOF

cat > $HADOOP_CONF_DIR/yarn-site.xml <<EOF
<?xml version="1.0"?>
<configuration>
    <property><name>yarn.resourcemanager.hostname</name><value>master</value></property>
    <property><name>yarn.nodemanager.vmem-check-enabled</name><value>false</value></property>
    <property><name>yarn.nodemanager.pmem-check-enabled</name><value>false</value></property>
    <property><name>yarn.nodemanager.resource.memory-mb</name><value>8192</value></property>
    <property><name>yarn.log-aggregation-enable</name><value>true</value></property>
    <property><name>yarn.nodemanager.remote-app-log-dir</name><value>/tmp/logs</value></property>
</configuration>
EOF

# --- 3. 启动 Worker 服务 ---
echo ">>> [3/4] 启动 DataNode 与 NodeManager..."
/opt/hadoop/bin/hdfs --config $HADOOP_CONF_DIR --daemon start datanode
/opt/hadoop/bin/yarn --config $HADOOP_CONF_DIR --daemon start nodemanager

# --- 4. 维持容器运行 ---
echo ">>> [4/4] ✅ Worker 已就绪，开始追踪服务日志..."
mkdir -p /opt/hadoop/logs
touch /opt/hadoop/logs/hadoop-worker-datanode-worker.log
tail -f /opt/hadoop/logs/*.log

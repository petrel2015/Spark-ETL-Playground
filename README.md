# Spark ETL 性能分析训练场 (NYC Taxi Edition)

本项目是一个专门用于学习 **Spark SQL ETL 开发**及 **Spark UI 性能调优**的实验环境。通过在真实场景中构造“坏代码”，复现并分析数据倾斜、数据膨胀、复杂 Shuffle 等常见的生产瓶颈。

## 🚀 技术栈
*   **Spark:** 3.5.8
*   **Scala:** 2.12.18
*   **Hadoop:** 3.3.6 (YARN Mode)
*   **Dataset:** NYC Yellow Taxi Trip Records & Zone Lookup

---

## 🛠️ 快速开始

### 1. 编译打包与启动
你可以选择分步执行，或者使用预设的 Maven Profile 一键完成编译、拷贝 Jar 包及重启 Docker 环境。

**一键完成（推荐）:**
```bash
mvn clean package -DskipTests -Prun
```

**分步执行:**
```bash
# 仅编译打包 (自动拷贝 Jar 到 workspace)
mvn clean package -DskipTests

# 手动启动环境
docker-compose down -v && docker-compose up -d
```

### 3. 查看运行情况
环境启动后，`master` 容器会自动将 `workspace/data` 下的数据上传至 HDFS，同时 `spark` 容器会自动触发 `spark-submit`。

你可以通过查看容器日志来观察运行情况：
```bash
docker logs -f spark
```

---

## 📊 监控与分析

项目集成了 Spark UI 和 History Server，方便进行全方位的性能诊断。

*   **Spark UI (运行中)**: [http://localhost:4040](http://localhost:4040)
*   **YARN ResourceManager**: [http://localhost:8088](http://localhost:8088)
*   **Spark History Server**: [http://localhost:18080](http://localhost:18080) *(任务完成后查看日志)*

---

## 🔍 Spark UI 学习重点

### Scenario 1: 数据膨胀与宽表 (Data Inflation & Wide Table)
*   **现象**: 将原始数据翻 10 倍，并衍生 100 列宽表。
*   **UI 观察点**:
    *   **Input Size / Records**: 观察 Stage 1 的数据读取总量。
    *   **Storage Tab**: 观察 `wideDF` 在序列化缓存后的内存与磁盘占用情况。

### Scenario 2: 剧烈 Shuffle 与数据倾斜 (Ultra Shuffle & Skew)
*   **现象**: 在倾斜字段 `PULocationID` 上进行海量数据的 Self-Join。
*   **UI 观察点**:
    *   **Shuffle Write/Read**: 观察高达数 GB 甚至更多的 Shuffle 数据传输。
    *   **Task Skew**: 观察特定 Task 处理数据量远超其他 Task 的现象。

### Scenario 3: 复杂 DAG 与 多级聚合 (Complex DAG & Multi-Agg)
*   **现象**: 先写出 Parquet 再读回进行多维度聚合排序。
*   **UI 观察点**:
    *   **DAG Visualization**: 查看完整的血缘关系和 Stage 划分。
    *   **Job Grouping**: 在 UI 中通过自定义的 Job Group Name（如 `Step_5_Final_Agg`）快速定位逻辑。

---

## 💡 实验建议
你可以尝试修改 `src/main/scala/com/example/spark/BadTaxiApp.scala` 中的配置并重新打包，观察 UI 变化：
*   **优化 Join**: 启用/禁用 `spark.sql.autoBroadcastJoinThreshold`。
*   **调整并行度**: 修改 `spark.sql.shuffle.partitions`（当前设为 10，观察改为 200 后的变化）。
*   **内存管理**: 在 `workspace/start-spark.sh` 中调整 `--executor-memory`，观察对 Spill 的影响。

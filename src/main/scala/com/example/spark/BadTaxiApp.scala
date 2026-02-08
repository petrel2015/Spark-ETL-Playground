package com.example.spark

import org.apache.spark.sql.{SparkSession, SaveMode, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

object BadTaxiApp {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("BadTaxi_Ultra_IO_Shuffle")
      // 故意设置较多的 Shuffle Partitions 以产生大量的小 Task
      .config("spark.sql.shuffle.partitions", "400")
      .getOrCreate()

    import spark.implicits._
    val sc = spark.sparkContext

    try {
      // ==========================================
      // Step 1: Input & Inflation
      // ==========================================
      sc.setJobGroup("Step_1_Input", "Reading and Inflating Data (10x)")
      
      val rawDf = spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv("hdfs:///user/data/yellow_tripdata_2021-01.csv.gz")

      // 将数据翻 10 倍，构造海量输入 IO
      var taxiDF = rawDf
      for (_ <- 1 to 3) {
        taxiDF = taxiDF.union(taxiDF) // 1 -> 2 -> 4 -> 8
      }
      taxiDF = taxiDF.union(rawDf).union(rawDf) // ~10x
      
      // 触发一次 Action 观察 Input 大小
      println(s">>> Total Records: ${taxiDF.count()}")

      // ==========================================
      // Step 2: Wide Mapping (Column Expansion)
      // ==========================================
      sc.setJobGroup("Step_2_Mapping", "Creating Wide Table (100 Columns)")
      
      var wideDF = taxiDF
      for (i <- 1 to 100) {
        wideDF = wideDF.withColumn(s"feat_$i", $"fare_amount" * 0.1 + $"trip_distance" * i)
      }

      // ==========================================
      // Step 2.5: Storage Visibility (Persist)
      // ==========================================
      sc.setJobGroup("Step_2_5_Cache", "Persisting Wide DataFrame to Storage")
      println(">>> Persisting wideDF to Storage (MEMORY_AND_DISK_SER)...")
      
      // 使用 MEMORY_AND_DISK_SER 策略：优先存内存，存不下溢写磁盘，且进行序列化以节省空间
      wideDF.persist(StorageLevel.MEMORY_AND_DISK_SER)
      
      // 必须执行 Action 才能触发 Cache 填充
      val cachedCount = wideDF.count()
      println(s">>> Wide DF Cached. Count: $cachedCount")

      // ==========================================
      // Step 3: Heavy Join (The Shuffle Monster)
      // ==========================================
      sc.setJobGroup("Step_3_Join", "Self-Join on PULocationID (Data Explosion & Shuffle)")
      
      // 这里的 PULocationID 存在严重倾斜，Self-Join 会导致数据量指数级增长和剧烈的 Shuffle
      // 我们取一个小子集进行 Join 以免直接撑爆 HDFS，但依然保持 Shuffle 的压力
      val leftSide = wideDF.filter($"PULocationID" % 10 === 0).repartition(400)
      val rightSide = wideDF.filter($"PULocationID" % 10 === 0).select("PULocationID", "feat_1", "feat_50").repartition(400)

      val joinedDF = leftSide.as("l")
        .join(rightSide.as("r"), "PULocationID")
        .withColumn("combined_val", $"l.feat_100" + $"r.feat_50")

      // ==========================================
      // Step 4: Write Intermediate (IO Output)
      // ==========================================
      sc.setJobGroup("Step_4_Write", "Saving Explosive Join Result to Parquet")
      val outputPath = "hdfs:///user/spark/io_intensive_output"
      
      joinedDF.write
        .mode(SaveMode.Overwrite)
        .parquet(outputPath)

      // ==========================================
      // Step 5: Read Back & Multi-Agg (Shuffle Read & Complex DAG)
      // ==========================================
      sc.setJobGroup("Step_5_Final_Agg", "Complex Multi-Stage Aggregation")
      
      val loadedDF = spark.read.parquet(outputPath)
      
      val finalResult = loadedDF
        .groupBy("PULocationID", "VendorID")
        .agg(
          sum("combined_val").as("total_value"),
          avg("l.feat_1").as("avg_feat_1"),
          count("*").as("pair_count"),
          max("l.feat_99").as("max_feat_99")
        )
        .filter($"pair_count" > 100)
        .sort($"total_value".desc)

      finalResult.show(10, truncate = false)
      
      // 任务结束前清理 Cache，虽然 Spark 关闭时会自动清理，但显式调用是个好习惯，也能在 Event Log 中留下记录
      wideDF.unpersist()

    } finally {
      println(">>> Ultra IO/Shuffle Job Finished.")
      spark.stop()
    }
  }
}

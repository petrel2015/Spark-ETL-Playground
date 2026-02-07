package com.example.spark

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types._

object BadTaxiApp {

  // --- 模拟一个极其低效的业务逻辑 UDF ---
  // 这种正则匹配在百万级数据上每一行跑一次，非常消耗 CPU
  val complexBusinessLogicUDF = udf((s: String) => {
    "UNKNOWN"
  })

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("BadTaxiETLPlayground")
      .config("spark.eventLog.enabled", "true")
      .config("spark.sql.autoBroadcastJoinThreshold", "-1") // 禁用广播，强制 Shuffle
      .config("spark.sql.shuffle.partitions", "20") // 增加一点并行度
      .config("spark.memory.fraction", "0.6") // 默认内存设置
      .getOrCreate()

    import spark.implicits._

    println("=== Loading Data from HDFS ===")
    
    val zonesDF = spark.read.option("header", "true").option("inferSchema", "true")
      .csv("hdfs:///user/data/taxi_zone_lookup.csv")
    
    // 强制缓存小表，避免重复读取干扰分析
    zonesDF.cache()
    zonesDF.count()

    val taxiDF = spark.read.option("header", "true").option("inferSchema", "true")
      .csv("hdfs:///user/data/yellow_tripdata_2021-01.csv.gz")
      .withColumn("tpep_pickup_datetime", to_timestamp($"tpep_pickup_datetime"))
      .withColumn("tpep_dropoff_datetime", to_timestamp($"tpep_dropoff_datetime"))

    println(s"Fact table count: ${taxiDF.count()}")

    // ==================================================================================
    // Scenario 1: CPU Burner (CPU 密集型)
    // ==================================================================================
    println("\n>>> Running Scenario 1: The CPU Burner (Heavy UDF)")
    // 目的：让 Task 变慢，不是因为 Shuffle，而是因为计算逻辑重
    val cpuIntensiveDF = taxiDF
      .filter($"total_amount" > 0)
      .withColumn("complex_category", complexBusinessLogicUDF($"VendorID".cast("string")))
      .groupBy("complex_category")
      .count()
    
    cpuIntensiveDF.show()


    // ==================================================================================
    // Scenario 2: The Interval Join Explosion (区间关联 - 内存/Shuffle 杀手)
    // ==================================================================================
    println("\n>>> Running Scenario 2: Interval Join (Find trips within 15 mins in same zone)")
    // 这是一个非常昂贵的操作。我们只取前 5万行做实验，否则可能直接跑不完
    val smallTaxi = taxiDF
      .limit(50000)
      .cache()
    
    // 逻辑：找出 A 车出发后 15 分钟内，同样从该地点出发的 B 车
    // 这种非等值的 Range Join 会产生巨大的中间数据集
    val intervalJoinDF = smallTaxi.as("a")
      .join(smallTaxi.as("b"),
        $"a.PULocationID" === $"b.PULocationID" &&
        $"b.tpep_pickup_datetime" > $"a.tpep_pickup_datetime" &&
        $"b.tpep_pickup_datetime" < ($"a.tpep_pickup_datetime" + expr("INTERVAL 15 MINUTES"))
      )
      .agg(count("*").as("pair_count"))

    intervalJoinDF.show()


    // ==================================================================================
    // Scenario 3: The OOM Trap (Skewed Collect List)
    // ==================================================================================
    println("\n>>> Running Scenario 3: Memory Pressure (collect_list on Skewed Keys)")
    // 这是一个典型的反模式。PULocationID 为 237, 236, 161 的数据非常多。
    // collect_list 会尝试把这个 Key 下的所有 VendorID (几十万个字符串) 放入一个 Array。
    // 这将给单个 Executor 造成极大的堆内存压力 (Heap Pressure)。
    
    try {
      val oomRiskDF = taxiDF
        .join(zonesDF, taxiDF("PULocationID") === zonesDF("LocationID"))
        .groupBy("Borough") // Borough 的粒度比 Zone 更粗，倾斜更严重 (Manhattan 极其大)
        .agg(
          count("*").as("total_trips"),
          // 危险操作：
          collect_list("VendorID").as("all_vendors_list") 
        )
      
      // 我们只打印 Schema 和大小，不敢直接 Show 全量，怕 Driver 挂掉，
      // 但执行 count 会触发 Shuffle 和 Reduce 阶段的 OOM 风险
      println(s"Result Row Count: ${oomRiskDF.count()}") 
    } catch {
      case e: Exception => 
        println("!!! Expected Exception might have occurred (OOM or Timeout) !!!")
        e.printStackTrace()
    }

    println("\n=== ETL Job Finished. Keeping Spark UI alive... ===")
    println("Go to http://localhost:4040 to see the Spark UI")
//    Thread.sleep(30000)
    
    spark.stop()
  }
}

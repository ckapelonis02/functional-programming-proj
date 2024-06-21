import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.hadoop.fs.FileSystem


object Aegean extends App {
  // LOCAL CONFIGS
  val spark = SparkSession.builder()
    .master("local[*]") // local[*] means "use as many threads as the number of processors available to the Java virtual machine"
    .appName("Aegean")
    .getOrCreate()
  val dataPath = "hdfs://localhost:9000/Aegean/nmea_aegean.logs"
  val out1 = "hdfs://localhost:9000/Aegean/output2/out1"
  val out2 = "hdfs://localhost:9000/Aegean/output2/out2"
  val out3 = "hdfs://localhost:9000/Aegean/output2/out3"
  val out4 = "hdfs://localhost:9000/Aegean/output2/out4"
  val out5 = "hdfs://localhost:9000/Aegean/output2/out5"

  // CLUSTER CONFIGS
//  val spark = SparkSession.builder
//    .appName("Aegean")
//    .master("yarn")
//    .config("spark.hadoop.fs.defaultFS", "hdfs://clu01.softnet.tuc.gr:8020")
//    .config("spark.hadoop.yarn.resourcemanager.address", "http://clu01.softnet.tuc.gr:8189")
//    .config("spark.hadoop.yarn.application.classpath", "$HADOOP_CONF_DIR,$HADOOP_COMMON_HOME/*,$HADOOP_COMMON_HOME/lib/*,$HADOOP_HDFS_HOME/*,$HADOOP_HDFS_HOME/lib/*,$HADOOP_MAPRED_HOME/*,$HADOOP_MAPRED_HOME/lib/*,$HADOOP_YARN_HOME/*,$HADOOP_YARN_HOME/lib/*")
//    .getOrCreate()
//  val sc = spark.sparkContext
//  val hdfsURI = "hdfs://clu01.softnet.tuc.gr:8020"
//  FileSystem.setDefaultUri(sc.hadoopConfiguration, hdfsURI)
//  val dataPath = "/user/chrisa/nmea_aegean/nmea_aegean.logs"
//  val out1 = "/user/fp19/output2/out1"
//  val out2 = "/user/fp19/output2/out2"
//  val out3 = "/user/fp19/output2/out3"
//  val out4 = "/user/fp19/output2/out4"
//  val out5 = "/user/fp19/output2/out5"

  // Reading the Aegean logs from hdfs
  val dataset = spark.read
    .option("header", "true") // first row is column names
    .option("inferSchema", "true") // automatically infer the schema of each column
    .csv(dataPath)
    .withColumn("date", to_date(col("timestamp")))
    .withColumn("abs_diff", abs(col("heading") - col("courseoverground")))
    .select("station", "date", "mmsi", "abs_diff", "speedoverground", "status") // select only necessary columns

  // Question 1: grouping by station and date since we want per station per day
  val q1 = dataset
    .groupBy("station", "date")
    .count()
    .rdd
  q1.collect().take(30).foreach(println)
  q1.saveAsTextFile(out1)

  // Question 2: grouping and counting based on #mmsi, then taking the max
  val q2 = dataset
    .groupBy("mmsi")
    .count()
    .orderBy(desc("count"))
    .limit(1)
    .rdd
  q2.collect().foreach(println)
  q2.saveAsTextFile(out2)

  // Question 3:
  val q3 = dataset
    .filter(col("station") === 8006 || col("station") === 10003) // get rid of the rest of the stations
    .groupBy("mmsi", "date") // grouping by mmsi and date to find...
    .agg(
      collect_set("station").alias("stations"),
      avg("speedoverground").alias("avg_SOG")
    ) // ...the stations foreach vessel on the same date and average their SOG's
    .filter(size(col("stations")) === 2) // take only those both at 8006 and 10003
    .select(avg("avg_SOG").alias("average_SOG")) // average the avg's
    .rdd
  q3.collect().take(30).foreach(println)
  q3.saveAsTextFile(out3)

  // Question 4: per station -> group by station
  val q4 = dataset
    .groupBy("station")
    .agg(avg("abs_diff").alias("avg_diff")) // difference already calculated
    .rdd
  q4.collect().take(30).foreach(println)
  q4.saveAsTextFile(out4)

  // Question 5: grouping by status and counting occurences of status_i, then sorting and taking top 3
  val q5 = dataset
    .groupBy("status")
    .count()
    .orderBy(desc("count"))
    .limit(3)
    .rdd
  q5.collect().foreach(println)
  q5.saveAsTextFile(out5)

  // DO NOT FORGET!
  spark.stop()
}

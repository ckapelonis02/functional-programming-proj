import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.{FileSystem, Path}

object Reuters extends App {
  // LOCAL CONFIGS
  val spark = SparkSession.builder
   .master("local[*]") // local[*] means "use as many threads as the number of processors available to the Java virtual machine"
   .appName("Reuters")
   .getOrCreate()
  val sc = spark.sparkContext
  val categoriesPath = "hdfs://localhost:9000/Reuters/rcv1-v2.topics.qrels"
  val termsPath = "hdfs://localhost:9000/Reuters/lyrl2004_vectors_*.dat" // '*' means match anything
  val stemsPath = "hdfs://localhost:9000/Reuters/stem.termid.idf.map.txt"
  val outputPath = "hdfs://localhost:9000/Reuters/output"

  // CLUSTER CONFIGS
//  val spark = SparkSession.builder
//    .appName("Reuters")
//    .master("yarn")
//    .config("spark.hadoop.fs.defaultFS", "hdfs://clu01.softnet.tuc.gr:8020")
//    .config("spark.hadoop.yarn.resourcemanager.address", "http://clu01.softnet.tuc.gr:8189")
//    .config("spark.hadoop.yarn.application.classpath", "$HADOOP_CONF_DIR,$HADOOP_COMMON_HOME/*,$HADOOP_COMMON_HOME/lib/*,$HADOOP_HDFS_HOME/*,$HADOOP_HDFS_HOME/lib/*,$HADOOP_MAPRED_HOME/*,$HADOOP_MAPRED_HOME/lib/*,$HADOOP_YARN_HOME/*,$HADOOP_YARN_HOME/lib/*")
//    .getOrCreate()
//  val sc = spark.sparkContext
//  val hdfsURI = "hdfs://clu01.softnet.tuc.gr:8020"
//  FileSystem.setDefaultUri(sc.hadoopConfiguration, hdfsURI)
//  val hdfs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
//  val categoriesPath = "/user/chrisa/Reuters/rcv1-v2.topics.qrels"
//  val termsPath = "/user/chrisa/Reuters/lyrl2004_vectors_*.dat" // '*' means match anything
//  val stemsPath = "/user/chrisa/Reuters/stem.termid.idf.map.txt"
//  val outputPath = "/user/fp19/output"


  // loading the categories in a val
  // categories = [(docId1, category1), ..., (docIdN, categoryN)]
  val categories = sc.textFile(categoriesPath).map { line =>
    val parts = line.split(" ")
    (parts(1).toInt, parts(0))
  }
  val catDocs = sc.parallelize((categories.map(_._2).countByValue()).toSeq)

  // loading the terms in a val
  // terms = [(docId1, term11), ..., (docId1, term1Μ1),
  //          (docId2, term21), ..., (docId2, term2Μ2),
  //                .            .           .
  //                .            .           .
  //                .            .           .
  //          (docIdΝ, termΝ1), ..., (docIdΝ, termΝΜΝ)]
  val terms = sc.textFile(termsPath).flatMap { line =>
    val parts = line.split(" ")
    // BE CAREFUL AND DROP 2 NOT 1 !!!
    parts.drop(2).map( term => (parts(0).toInt, term.split(":")(0).toInt) )
  }
  val termDocs = sc.parallelize((terms.map(_._2).countByValue()).toSeq)

  // loading the stems in a val
  val stems = sc.textFile(stemsPath).map { line =>
    val parts = line.split(" ")
    (parts(1).toInt, parts(0))
  }.collectAsMap() // collected as Map [O(1) access]
  val stemsTerms = sc.parallelize(stems.toSeq)

  // resulting RDD[String] contains jaccard index for every category
  // term pair using joins to calculate it
  val result = categories
    .join(terms)
    .map { case (_, (c, t)) => ((c, t), 1) }
    .reduceByKey(_ + _)
    .map { case ((c, t), i) => (c, (t, i)) }
    .join(catDocs)
    .map { case (cat, ((term, count), catCount)) => (term, (cat, count, catCount)) }
    .join(termDocs)
    .join(stemsTerms)
    .map { case (_, (((cat, count, catCount), termCount), stem)) =>
      s"$cat;$stem;${count.toDouble / (catCount + termCount - count).toDouble}"
    }

  //  if (hdfs.exists(new Path(outputPath))) {
  //    hdfs.delete(new Path(outputPath), true)
  //  }
  result.saveAsTextFile(outputPath)

  // do not forget to stop the context
  sc.stop()
}

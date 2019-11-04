package com.bigdata.spark.sparkStreaming
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object nifitokafkathenoracledb {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("sparkKafkaConsumer").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
    import spark.implicits._
    import spark.sql
    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    import scala.util.Try
    val topics = "mynifi"
    val brokers = "localhost:9092"
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers,
      "bootstrap.servers"->"localhost:9092",
      "zookeeper.connect" -> "localhost:2181",
      "group.id" -> "kaf",
      "key.serializer"->"org.apache.kafka.common.serialization.StringSerializer",
      "value.serializer"->"org.apache.kafka.common.serialization.StringSerializer",
      "key.deserializer"->"org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer"->"org.apache.kafka.common.serialization.StringDeserializer")

    val messages = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,         // this is to run the
      ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams))
    val lines = messages.map(_.value)   // lines is a dStream  map(x=>x.value) or map(x=>x.key)
    // val data = args(0)

    lines.foreachRDD { x =>

      println(s"processing first line : $x")
      val spark = SparkSession.builder.config(x.sparkContext.getConf).getOrCreate()
      import spark.implicits._
      //val df1=spark.read.format("csv").option("delimiter"," ").load(data)
      //val dfschema=df1.schema()

      val df = spark.read.option("inferSchema","true").json(x)
      // val df = spark.read.format("csv").schema(dfschema).load(data)
      df.show()
     /* val oUrl = "jdbc:oracle:thin://@oracledb.cotidg8mtnrt.ap-south-1.rds.amazonaws.com:1521/ORCL"
      val oProp = new java.util.Properties()
      oProp.setProperty("user","ousername")
      oProp.setProperty("password","opassword")
      oProp.setProperty("driver","oracle.jdbc.OracleDriver")
      //df.createOrReplaceTempView("personal")
      df.write.mode("append").jdbc(oUrl,"kafkalogssunil",oProp)*/
    }
    ssc.start()             // Start the computation
    ssc.awaitTermination()  // Wait for the computation to terminate

  }
}

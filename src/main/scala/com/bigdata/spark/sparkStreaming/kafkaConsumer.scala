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

object kafkaConsumer {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    import scala.util.Try
    val topics = "indeng"
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
    val pro = lines.foreachRDD { x=>x
      val spark = SparkSession.builder.config(x.sparkContext.getConf).getOrCreate()
      import spark.implicits._

      val df = x.filter(x=>x!="" || x!=null).map(x=>x.split(",")).map(x=>(x(0),x(1),x(2))).toDF("name","age","city")
      df.show()

      df.createOrReplaceTempView("san")
      val res = spark.sql("select * from san where city='mas'")
      val del = spark.sql("select * from san where city='del'")
      val myurl = "jdbc:oracle:thin:@//myoracledb.cdpobvgs3kxr.us-east-2.rds.amazonaws.com:1521/orcl"
      val oprop = new java.util.Properties()
      oprop.setProperty("user","ousername")
      oprop.setProperty("password","opassword")
      oprop.setProperty("driver","oracle.jdbc.OracleDriver")
      res.write.mode(SaveMode.Append).jdbc(myurl,"masinfo",oprop)
      res.write.mode(SaveMode.Append).jdbc(myurl,"delinfo",oprop)



    }
    ssc.start()        //start compailing
    ssc.awaitTermination()
  }
}

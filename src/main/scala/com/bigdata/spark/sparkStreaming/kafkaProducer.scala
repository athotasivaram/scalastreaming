package com.bigdata.spark.sparkStreaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

import org.apache.kafka.clients.producer._
import org.apache.spark.sql.SparkSession

import org.apache.spark.streaming._
import org.apache.spark.streaming._

object kafkaProducer {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
    //  val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    val ssc = new StreamingContext(sc, Seconds(10))
    import spark.implicits._
    import spark.sql
/*    val url = "jdbc:oracle:thin:@//myoracledb.cdpobvgs3kxr.us-east-2.rds.amazonaws.com:1521/orcl"
    val oprop = new java.util.Properties()
    oprop.setProperty("user","ousername")
    oprop.setProperty("password","opassword")
    oprop.setProperty("driver","oracle.jdbc.OracleDriver")
    val df = spark.read.jdbc(url,"emp",oprop)
val data = df.rdd*/
    val topic = "indeng"
    val path = "C:\\work\\datasets\\cricscore"
    val data = spark.sparkContext.textFile(path)

    data.foreachPartition(x => {
      import java.util._

      val props = new java.util.Properties()
      //  props.put("metadata.broker.list", "localhost:9092")
      //      props.put("serializer.class", "kafka.serializer.StringEncoder")
      props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
      props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
      props.put("bootstrap.servers", "localhost:9092")

      // import kafka.producer._
      // val config = new ProducerConfig(props)
      val producer = new KafkaProducer[String, String](props)

      x.foreach(x => {
        println(x)
        producer.send(new ProducerRecord[String, String](topic.toString(), x.toString)) //sending to kafka broker
        //(topic, "venu,32,hyd")
        //(indpak,"anu,56,mas")
        Thread.sleep(5000)

      })

    })

    ssc.start() // Start the computation
    ssc.awaitTermination()
  }
}

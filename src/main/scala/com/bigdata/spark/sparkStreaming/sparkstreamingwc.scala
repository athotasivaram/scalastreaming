package com.bigdata.spark.sparkStreaming

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark._
import org.apache.spark.streaming._

object sparkstreamingwc {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[2]").appName("sparkstreamingwc").getOrCreate()
    //  val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    val ssc = new StreamingContext(sc, Seconds(10))
    //sc ....to create RDD
    //sql context....data frame...
    //streaming data...dstream...ssc(spark streaming conteext)
    val lines = ssc.socketTextStream("ec2-13-233-68-21.ap-south-1.compute.amazonaws.com",1234)
   // val process = lines.flatMap(x=>x.split(" ")).map(x=>(x,1)).groupByKey().map(x=>(x._1,x._2.sum))
     // val process = lines.flatMap(x=>x.split(" ")).map(x=>(x,1)).reduceByKeyAndWindow((a:Int,b:Int) => (a + b), Minutes(2), Seconds(30))
    //by default ull get 10 sec , but process after 30 seconds


    //process.print()
    val pro = lines.foreachRDD { x=>x
      val spark = SparkSession.builder.config(x.sparkContext.getConf).getOrCreate()
      import spark.implicits._

      val df = x.filter(x=>x!="" || x!=null).map(x=>x.split(",")).map(x=>(x(0),x(2),x(3))).toDF("name","age","city")
      df.createOrReplaceTempView("san")
      val res = spark.sql("select * from san where city='mas'")
      val del = spark.sql("select * from tab where city='del'")
      val myurl = "jdbc:oracle:thin:@//myoracledb.cdpobvgs3kxr.us-east-2.rds.amazonaws.com:1521/orcl"
      val oprop = new java.util.Properties()
      oprop.setProperty("user","ousername")
      oprop.setProperty("password","opassword")
      oprop.setProperty("driver","oracle.jdbc.OracleDriver")
      res.write.mode(SaveMode.Append).jdbc(myurl,"masinfo",oprop)
      res.write.mode(SaveMode.Append).jdbc(myurl,"delinfo",oprop)


    }
    import spark.implicits._
    import spark.sql

    ssc.start()        //start compailing
    ssc.awaitTermination()
  }
}

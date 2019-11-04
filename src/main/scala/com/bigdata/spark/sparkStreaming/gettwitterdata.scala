package com.bigdata.spark.sparkStreaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._

object gettwitterdata {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
    //  val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    import spark.implicits._
    import spark.sql
    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))

    val APIkey = "qPgZO22ns0wUkXKIOXLRbVVtb"//APIKey
    val APIsecretkey = "Q1ls9fLs7UDXqikmNmtt4cfNSJY6ZJe40iKHxnV8Tj5v6sQ6aR"// (API secret key)
    val Accesstoken = "181460431-ZxIP5rZYAKDdKe9Kbl4Wp1VKJrkP0sV5hC4Ye9uR" //Access token
    val Accesstokensecret = "1QxS8ZcVlGQJtQbaIVTc18Oz8MA5ODuFUAyA54kldbWNQ" //Access token secret

    val searchFilter = "tensorflow,Artificial Intelligence"
    //  val pipelineFile = ""
    //val searchFilter = "BJP, Jammu and Kashmir, Jammu, 370,ladakh"

    val interval = 10
    //  import spark.sqlContext.implicits._
    System.setProperty("twitter4j.oauth.consumerKey", APIkey)
    System.setProperty("twitter4j.oauth.consumerSecret", APIsecretkey)
    System.setProperty("twitter4j.oauth.accessToken", Accesstoken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", Accesstokensecret)

    val tweetStream = TwitterUtils.createStream(ssc, None, Seq(searchFilter.toString))
    val pro = tweetStream.foreachRDD{ x=>x
      val spark = SparkSession.builder.config(x.sparkContext.getConf).getOrCreate()
      import spark.implicits._
      val df = x.map(x=>(x.getSource,x.getCreatedAt.toString,x.getUser().getScreenName,x.getText)).toDF("source","crdate","user","text")
      df.show(false)
      df.createOrReplaceTempView("tab")
      val res = spark.sql("select * from tab where text like '%university%'")

    }

    ssc.start()             // Start the computation
    ssc.awaitTermination()  // Wait for the computation to terminate

  }
  //if you get like this ending with V its version problem
  //Caused by: java.lang.NoSuchMethodError: twitter4j.TwitterStream.addListener(Ltwitter4j/StreamListener;)V

}
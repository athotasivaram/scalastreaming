package com.bigdata.spark.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object datasetApi {
  case class bankcc (age:Int, job:String, marital:String, education:String, default1:String, balance:Double, housing:String, loan:String, contact:String, day:Int, month:String, duration:Int, campaign:Int, pdays:Int, previous:Int, poutcome:String, y:String)
 case class selcol(age:Int,job:String,balance:Int)
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("${name}").getOrCreate()
    //  val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    import spark.implicits._
    import spark.sql
    val dataapi = "file:///C:\\work\\datasets\\bank.csv"
    val df = spark.read.format("csv").option("header","true").option("inferSchema","true").option("delimiter",";").load(dataapi).withColumnRenamed("default","default1")
   val ds = df.as[bankcc]
    df.show()
    ds.createOrReplaceTempView("tab")
    //val res = spark.sql("select * from tab where age >30").as[bankcc]
    //res.show()
    val dsa = df.as[selcol]
    val res1 = df.select($"age",$"job",$"balance").where($"age".gt(40)).as[selcol]
    res1.show()

    spark.stop()
  }
}

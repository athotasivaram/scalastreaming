package com.bigdata.spark.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object ooziepoc {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("${name}").enableHiveSupport().getOrCreate()
    //  val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    import spark.implicits._
    import spark.sql
    val url ="jdbc:oracle:thin:@//myoracledb.cdpobvgs3kxr.us-east-2.rds.amazonaws.com:1521/orcl"
    val oprop=new java.util.Properties()
    oprop.setProperty("user","ousername")
    oprop.setProperty("password","opassword")
    oprop.setProperty("driver","oracle.jdbc.OracleDriver")
    val df =spark.read.jdbc(url, "emp" ,oprop)
    val tab = args(0)
    df.write.mode(SaveMode.Overwrite).format("hive").saveAsTable(tab)
    df.show()
    spark.stop()
  }
}

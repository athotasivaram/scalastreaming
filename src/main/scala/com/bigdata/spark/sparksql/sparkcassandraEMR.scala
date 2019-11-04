package com.bigdata.spark.sparksql

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object sparkcassandraEMR {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("${name}").getOrCreate()
    //  val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    import spark.implicits._
    import spark.sql
    val url ="jdbc:oracle:thin:@//myoracledb.cdpobvgs3kxr.us-east-2.rds.amazonaws.com:1521/orcl"
    val oprop=new java.util.Properties()
    oprop.setProperty("user","ousername")
    oprop.setProperty("password","opassword")
    oprop.setProperty("driver","oracle.jdbc.OracleDriver")
    val df =spark.read.jdbc(url, "DEPT" ,oprop)
    df.printSchema()
    df.write.mode(SaveMode.Append).format("org.apache.spark.sql.cassandra").option("table","dept").option("keyspace","sivaramdb").save()
    df.show()

    spark.stop()
  }
}

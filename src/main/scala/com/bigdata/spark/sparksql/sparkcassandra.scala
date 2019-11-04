package com.bigdata.spark.sparksql
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object sparkcassandra {
  case class aslcc (id:Int,city:String,name:String)
  case class nepcc (myname:String,email:String,phone:Int)
  case class rescc (id:Int,city:String,name:String,email:String,phone:String)
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("${name}").getOrCreate()
    //  val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    import spark.implicits._
    import spark.sql
val df = spark.read.format("org.apache.spark.sql.cassandra").option("table","asl").option("keyspace","sivaramdb").load().as[aslcc]
    val df1 = spark.read.format("org.apache.spark.sql.cassandra").option("table","nep").option("keyspace","sivaramdb").load().withColumnRenamed("name","myname").as[nepcc]
   // df.show()
    //df1.show()
    df.createOrReplaceTempView("t1")
    df1.createOrReplaceTempView("t2")
    //val ss =spark.sql("select t2.name, t2.phone, t1.city from t1 join t2 on t1.name=t2.name ")

    val ss = df.join(df1,$"name"===$"myname","left_outer").drop($"myname").as[rescc]
    //to export /write cassandra first create table in cassandra

    ss.write.mode(SaveMode.Append).format("org.apache.spark.sql.cassandra").option("table","aslnep").option("keyspace","sivaramdb").save()
    ss.show()
    spark.stop()
  }
}

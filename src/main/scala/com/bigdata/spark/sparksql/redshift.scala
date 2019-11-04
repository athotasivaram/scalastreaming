package com.bigdata.spark.sparksql

import com.databricks.spark.redshift._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
// com.bigdata.spark.sparksql.redshift
object redshift {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("${name}").getOrCreate()
    //  val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    import spark.implicits._
    import spark.sql
    val url ="jdbc:redshift://redshiftcluster.c6caavont6yh.us-east-2.redshift.amazonaws.com:5439/reddb"
    val prop=new java.util.Properties()
    prop.setProperty("user","rusername")
    prop.setProperty("password","Rpassword.1")
    prop.setProperty("driver","com.amazon.redshift.jdbc.Driver")
    val df=spark.read.jdbc(url,"Babynames1",prop).withColumnRenamed("year of birth","yearofbirth")
    df.show(5)
    df.printSchema()
    val dff=df.createOrReplaceTempView("tab1")
    val ss=spark.sql("select * from tab1 where length(childname)>10")
    val dd = spark.sql("select * from tab1 where count=(select max(count) from tab1)")
    dd.write.jdbc(url,"topten",prop)
    ss.show(50)
    dd.show()



    spark.stop()
  }
}

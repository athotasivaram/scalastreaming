package com.bigdata.spark.sparksql


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object sparkdatefun {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("${name}").getOrCreate()
    //  val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    import spark.implicits._
    import spark.sql

  /* val url = "jdbc:oracle:thin:@//myoracledb.cdpobvgs3kxr.us-east-2.rds.amazonaws.com:1521/orcl"
    val oprop=new java.util.Properties()
    oprop.setProperty("user","ousername")
    oprop.setProperty("password","opassword")
    oprop.setProperty("driver","oracle.jdbc.OracleDriver")
    val df =spark.read.jdbc(url, "emp" ,oprop)
    //df.show()
    //df.printSchema()
    //df.createOrReplaceTempView("emp")
    //val result = spark.sql("select * from emp where sal >3000")
    //result.show()
    //val query = "select * from emp"
    //val res = spark.sql(query)*/
    val df = spark.createDataFrame(Seq(
      (7369, "SMITH", "CLERK", 7902, "17-Dec-80", 800, 20, 10),
      (7499, "ALLEN", "SALESMAN", 7698, "20-Feb-81", 1600, 300, 30),
      (7521, "WARD", "SALESMAN", 7698, "22-Feb-81", 1250, 500, 30),
      (7566, "JONES", "MANAGER", 7839, "2-Apr-81", 2975, 0, 20),
      (7654, "MARTIN", "SALESMAN", 7698, "28-Sep-81", 1250, 1400, 30),
      (7698, "BLAKE", "MANAGER", 7839, "1-May-81", 2850, 0, 30),
      (7782, "CLARK", "MANAGER", 7839, "9-Jun-81", 2450, 0, 10),
      (7788, "SCOTT", "ANALYST", 7566, "19-Apr-87", 3000, 0, 20),
      (7839, "KING", "PRESIDENT", 0, "17-Nov-81", 5000, 0, 10),
      (7844, "TURNER", "SALESMAN", 7698, "8-Sep-81", 1500, 0, 30),
      (7876, "ADAMS", "CLERK", 7788, "23-May-87", 1100, 0, 20)
    )).toDF("empno", "ename", "job", "mgr", "hiredate", "sal", "comm", "deptno")
    //df.printSchema()
   // df.show(3)
    //val res = df.withColumn("today",current_date()) //return date/current date
    val df1 = df.withColumn("hiredate", to_date (unix_timestamp($"hiredate", "dd-MMM-yy").cast("timestamp")))
    //convert date format..."to date
   //val res = df1.withColumn("after6month",add_months($"hiredate",1)).withColumn("currdate",to_date(current_date()))
    val res = df1.withColumn("after10days", date_add($"hiredate",10)).withColumn("datediff",datediff(current_date(),$"hiredate"))
       .withColumn("dayoffweek",dayofweek(current_date()))
       .withColumn("unixts", unix_timestamp($"hiredate"))
       .withColumn("lastdate",last_day($"hiredate"))
       .withColumn("waitgetsal",datediff(last_day(current_date()),current_date()))
       .withColumn("monthsbeetween",months_between(current_date(),$"hiredate"))
       .withColumn("nextday",next_day(current_date(),"SUN"))
       .withColumn("weeks",weekofyear(current_date()))

    df1.createOrReplaceTempView("tab")
    val result = spark.sql("select *,weekofyear(current_date()) weeks, next_day(current_date(),'SUN') nextday from tab")
    result.show()
    //res.printSchema()
    //when do we need to use sparkcon

    spark.stop()
  }
}

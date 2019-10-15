package com.bigdata.spark.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object datasetjson {
  case class idcol(
                    `$oid`: String
                  )
  case class Majorsector_percent(
                                  Name: String,
                                  Percent: Double
                                )
  case class Mjsector_namecode(
                                name: String,
                                code: String
                              )
  case class Project_abstract(
                               cdata: String
                             )
  case class Projectdocs(
                          DocTypeDesc: String,
                          DocType: String,
                          EntityID: String,
                          DocURL: String,
                          DocDate: String
                        )
  case class Sectorcc(
                       Name: String
                     )
  case class sivacc(
                      _id: idcol,
                      approvalfy: String,
                      board_approval_month: String,
                      boardapprovaldate: String,
                      borrower: String,
                      closingdate: String,
                      country_namecode: String,
                      countrycode: String,
                      countryname: String,
                      countryshortname: String,
                      docty: String,
                      envassesmentcategorycode: String,
                      grantamt: Double,
                      ibrdcommamt: Double,
                      id: String,
                      idacommamt: Double,
                      impagency: String,
                      lendinginstr: String,
                      lendinginstrtype: String,
                      lendprojectcost: Double,
                      majorsector_percent: List[Majorsector_percent],
                      mjsector_namecode: List[Mjsector_namecode],
                      mjtheme: List[String],
                      mjtheme_namecode: List[Mjsector_namecode],
                      mjthemecode: String,
                      prodline: String,
                      prodlinetext: String,
                      productlinetype: String,
                      project_abstract: Project_abstract,
                      project_name: String,
                      projectdocs: List[Projectdocs],
                      projectfinancialtype: String,
                      projectstatusdisplay: String,
                      regionname: String,
                      sector: List[Sectorcc],
                      sector1: Majorsector_percent,
                      sector2: Majorsector_percent,
                      sector3: Majorsector_percent,
                      sector4: Majorsector_percent,
                      sector_namecode: List[Mjsector_namecode],
                      sectorcode: String,
                      source: String,
                      status: String,
                      supplementprojectflg: String,
                      theme1: Majorsector_percent,
                      theme_namecode: List[Mjsector_namecode],
                      themecode: String,
                      totalamt: Double,
                      totalcommamt: Double,
                      url: String
                    )
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("${name}").getOrCreate()
    //  val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    import spark.implicits._
    import spark.sql
    val datajson = "file:///C:\\work\\datasets\\jsondataset\\stocks.json"
    val df = spark.read.format("json").option("inferSchema","true").load(datajson)
    val ds = df.as[sivacc]
    ds.createOrReplaceTempView("tab")
    val res = spark.sql("select * from tab ")
    res.show(10)

    spark.stop()
  }
}

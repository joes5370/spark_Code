package com.haiteam

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object 문제원형 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().
      setAppName("DataLoading").
      setMaster("local[*]")
    var sc = new SparkContext(conf)
    val spark = new SQLContext(sc)

    import spark.implicits._
    import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode}
    import org.apache.spark.sql.types.{StringType, StructField, StructType}

    var salesFile = "pro_actual_sales.csv"
    // 절대경로 입력
    var salesDf=
      spark.read.format("csv").
        option("header","true").
        option("Delimiter",",").
        load("C:/spark_orgin_2.2.0/bin/data/"+salesFile)

    // 데이터 확인 (3)
    print(salesDf.show(2))

    var salesColums = salesDf.columns.map(x=>{x.toLowerCase()})
    var regionidno1 = salesColums.indexOf("regionseg1")
    var productno1 = salesColums.indexOf("productseg1")
    var productno2 = salesColums.indexOf("productseg2")
    var regionidno2 = salesColums.indexOf("regionseg2")
    var regionidno3 = salesColums.indexOf("regionseg3")
    var productno3 = salesColums.indexOf("productseg3")
    var yearweekno = salesColums.indexOf("yearweek")
    var yearno = salesColums.indexOf("year")
    var weekno = salesColums.indexOf("week")
    var qtyno = salesColums.indexOf("qty")

    var promotionFile = "pro_promotion.csv"

    var promotionDf=
      spark.read.format("csv").
        option("header","true").
        option("Delimiter",",").
        load("C:/spark_orgin_2.2.0/bin/data/"+promotionFile)

    print(promotionDf.show(2))

    var promotionColums = promotionDf.columns.map(x=>{x.toLowerCase()})
    var regionidno = promotionColums.indexOf("regionseg")
    var salesidno = promotionColums.indexOf("salesid")
    var productgroup2 = promotionColums.indexOf("productgroup")
    var itemno = promotionColums.indexOf("item")
    var targetweekno3 = promotionColums.indexOf("targetweek")
    var planweekno = promotionColums.indexOf("planwee")
    var map_priceno = promotionColums.indexOf("map_price")
    var irno = promotionColums.indexOf("ir")
    var pmapno = promotionColums.indexOf("pmap")
    var pmap10no = promotionColums.indexOf("pmap10")
    var pro_percentno = promotionColums.indexOf("pro_percent")


    //두개 RDD
    var salesRdd = salesDf.rdd
    var promotionRdd = promotionDf.rdd


  }
}

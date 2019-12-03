package com.haiteam

import org.apache.spark
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object 실습 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().
      setAppName("DataLoading").
      setMaster("local[*]")
    var sc = new SparkContext(conf)
    val spark = new SQLContext(sc)

    import spark.implicits._
    var salesDataFile = "pro_actual_sales.csv"
    var salesData = spark.read.format("csv").
      option("header","true").
      option("Delimiter",",").
      load("C:/spark_orgin_2.2.0/bin/data/"+salesDataFile)

    var salesColume = salesData.columns.map(x=>{x.toLowerCase()})

    var regionseg1 = salesColume.indexOf("regionseg1")
    var productseg1 = salesColume.indexOf("productseg1")
    var productseg2 = salesColume.indexOf("productseg2")
    var regionseg2 = salesColume.indexOf("regionseg2")
    var regionseg3 = salesColume.indexOf("regionseg3")
    var productseg3 = salesColume.indexOf("productseg3")
    var yearweek = salesColume.indexOf("yearweek")
    var year = salesColume.indexOf("year")
    var week = salesColume.indexOf("week")
    var qty = salesColume.indexOf("qty")

    var salesRdd = salesData.rdd

    var groupData = salesRdd.groupBy(x=>{
      (x.getString(productseg2),x.getString(regionseg3),x.getString(yearweek))
    }).flatMap(x=>{
      var key = x._1
      var data = x._2
      var sumation = data.map(x=>{x.getString(qty).toInt}).sum
      var size = data.size
      var average = 0.0d
      if(size != 0){
        average = sumation / size
      }

      var finalData = data.map(x=>{
        var ratio = 0.0d
        var each_qty = x.getString(qty).toDouble
        if(average != 0){
          ratio = each_qty / average
        }

        (x.getString(regionseg1),
          x.getString(productseg2),
          x.getString(regionseg2),
          x.getString(regionseg3),
          x.getString(productseg3),
          x.getString(yearweek),
          x.getString(year),
          x.getString(week),
          x.getString(qty),
          average.toString,
          ratio.toString)
      })
      finalData
    })
  }
}

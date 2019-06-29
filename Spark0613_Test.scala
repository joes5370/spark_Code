package com.haiteam

import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object Spark0613_Test {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().
      setAppName("DataLoading").
      setMaster("local[*]")
    var sc = new SparkContext(conf)
    val spark = new SQLContext(sc)


    // 데이터 파일 로딩
    // 파일명 설정 및 파일 읽기 (2)
    var paramFile = "kopo_product_volume.csv"

    // 절대경로 입력
    var paramData=
      spark.read.format("csv").
        option("header","true").
        option("Delimiter",","). // csv파일 불러올때는 꼭 확인해주자
        load("C:/spark_orgin_2.2.0/bin/data/"+paramFile)

    // 데이터 확인 (3)
    print(paramData.show)

    var rawDataColumns = paramData.columns
    var regionidNo = rawDataColumns.indexOf("REGIONID")
    var productNo = rawDataColumns.indexOf("PRODUCTGROUP")
    var yearweekNo = rawDataColumns.indexOf("YEARWEEK")
    var qtyNo = rawDataColumns.indexOf("VOLUME")

    var rawRdd = paramData.rdd

    var filterRdd = rawRdd.filter(x=>{
      var check = true
      var yearweek = x.getString(yearweekNo)
      if(yearweek.length != 6){
        check = false
      }
      check
    })

    var removeRdd = rawRdd.filter(x=>{
      var check = true
      var yearweek = x.getString(yearweekNo)
      if(yearweek.substring(4,6).toInt > 53){
        check = false
      }
      check
    })

    var containRdd = rawRdd.filter(x=>{
      var product = x.getString(productNo)
      var check = true
      if(product.contains("PRODUCT1","PRODUCT2")){
        check = false
      }
     check
    })

    var maxRdd = removeRdd.map(x=>{
      var qty = x.getString(qtyNo).toDouble
      var maxValue = 700000
      if(qty > maxValue){qty = maxValue}
      Row( x.getString(regionidNo),
        x.getString(productNo),qty)
    })

    var minRdd = removeRdd.map(x=>{
      var qty = x.getString(qtyNo).toDouble
      var minValue = 150000
      if(qty < minValue){qty = minValue}
      Row( x.getString(regionidNo),
        x.getString(productNo),qty)
//      x.getString(regionidNo),
//      x.getString(productNo),
//      x.getString(yearweekNo),qty,qty*1.2)
    })



    var createRdd = removeRdd.map(x=>{
      var MIN_VALUE = 150000
      var qty = x.getString(qtyNo).toDouble
      var new_qty = qty

      if(qty < MIN_VALUE){
        qty = MIN_VALUE
      }
      Row( x.getString(regionidNo),qty,new_qty*1.2)
    })


    var groupRdd2 = removeRdd.groupBy(x=>{
      (x.getString(regionidNo),x.getString(productNo))
    }).map(x=>{
      var key = x._1
      var data = x._2
      var sumation = data.map(x=>{
        x.getString(qtyNo).toDouble
      }).sum
      var size = data.size
      var average = 0.0d

      if(size != 0){
        average = sumation/size
      }else{
        average = 0.0d
      }
      (key,average)
    })

    var groupMap = groupRdd2.collectAsMap()


  }

}

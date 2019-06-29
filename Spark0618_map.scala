package com.haiteam

import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object Spark0618_map {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().
      setAppName("DataLoading").
      setMaster("local[*]")
    var sc = new SparkContext(conf)
    val spark = new SQLContext(sc)


    // 데이터 파일 로딩
    // 파일명 설정 및 파일 읽기 (2)

    // 절대경로 입력
    var staticUrl = "jdbc:oracle:thin:@192.168.110.112:1521/orcl"
    var staticUser = "kopo"
    var staticPw = "kopo"
    var selloutDb = "kopo_product_volume"

    // jdbc (java database connectivity) 연결
    var selloutDataFromOracle = spark.read.format("jdbc").
      option("url",staticUrl).
      option("dbtable",selloutDb).
      option("user",staticUser).
      option("password",staticPw).load
    // 메모리 테이블 생성
    selloutDataFromOracle.createOrReplaceTempView("selloutTable")
    selloutDataFromOracle.show()

    var rawRdd = selloutDataFromOracle.rdd
    var rawDataColumns = selloutDataFromOracle.columns

    var regionNo = rawDataColumns.indexOf("REGIONID")
    var productGroupNo = rawDataColumns.indexOf("PRODUCTGROUP")
    var yearweekNo = rawDataColumns.indexOf("YEARWEEK")
    var qtyNo = rawDataColumns.indexOf("VOLUME")



    var filterRdd = rawRdd.filter(x=>{
      var checkValid = true
      var yearweek = x.getString(yearweekNo)
      if(yearweek.substring(4,6).toInt > 52){
        checkValid = false}
      checkValid
    })

    var tempMap = filterRdd.map(x=>{
      var qty = x.get(qtyNo).toString.toDouble
      var new_qty = qty
      Row(
        x.getString(regionNo),
        x.getString(yearweekNo),
        x.getString(productGroupNo),new_qty
      )
    })

    var resultFlat = tempMap.groupBy(x=>{
      (x.getString(regionNo), x.getString(2)
      )
    }).flatMap(x=>{
      var key = x._1
      var data = x._2
      //사이즈
      var size = data.size

      var qtyList = data.map(x=>{
        x.getDouble(qtyNo)})
      var qtyArray = qtyList.toArray
      //합
      var qtySum = qtyArray.sum
      //평균
      var qtyMean = if(size != 0){
        qtySum / size
      }else{
        0.0
      }
      //표준편차
      var stdev = if(size!=0){
        Math.sqrt(qtyList.map(x=>{Math.pow(x-qtyMean,2)}).sum/size)
      }else{
        0.0
      }


//      var sumation = data.map(x=>{
//       x.getDouble(qtyNo)}).sum
//      var average = 0.0d
//      if(size != 0 ){
//        average = sumation / size
//      }

      //계절성 지수 = getString(qtyNo)/qtyMean

      var outputData = data.map(x=>{(

        x.getString(regionNo),
        x.getString(productGroupNo),
        size,
        qtySum,
        qtyMean,
        stdev
            )
      })
      outputData
    })
  }
}

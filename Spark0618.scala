package com.haiteam

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext}

object Spark0618 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().
      setAppName("DataLoading").
      setMaster("local[*]")
    var sc = new SparkContext(conf)
    val spark = new SQLContext(sc)


    // 데이터 파일 로딩
    // 파일명 설정 및 파일 읽기 (2)
    var paramFile = "kopo_channel_seasonality.csv"

    // 절대경로 입력
    var paramData=
      spark.read.format("csv").
        option("header","true").
        option("Delimiter",","). // csv파일 불러올때는 꼭 확인해주자
        load("C:/spark_orgin_2.2.0/bin/data/"+paramFile)

    // 데이터 확인 (3)
    print(paramData.show)

    var rawRdd = paramData.rdd

    rawRdd.collect.foreach(println)

    //데이터 프레임 컬럼 인덱스 = 하드코딩을 빼기 위해서
    var rawDataColumns = paramData.columns

    var regionNo = rawDataColumns.indexOf("REGIONID")
    var productGroupNo = rawDataColumns.indexOf("PRODUCTGROUP")
    var productNo = rawDataColumns.indexOf("PRODUCT")
    var yearweekNo = rawDataColumns.indexOf("YEARWEEK")
    var yearNo = rawDataColumns.indexOf("YEAR")
    var weekNo = rawDataColumns.indexOf("WEEK")
    var qtyNo = rawDataColumns.indexOf("QTY")

    var filterRdd = rawRdd.filter(x=>{
      var checkValid = true
      var yearweek = x.getString(yearweekNo)
      if(yearweek.substring(4,6).toInt > 52){
        checkValid = false}
      checkValid
    })

    var minIfQuiz = filterRdd.map(x=>{
      var MIN_VALUE = 150000
      var qty = x.getString(qtyNo).toDouble

      var new_qty = qty
      if (qty < MIN_VALUE) {
        new_qty = MIN_VALUE
      }
      Row(
        x.getString(regionNo),
        x.getString(productNo),
        x.getString(yearweekNo),new_qty)
    });


    //만약 36line의 인덱스와 다를 경우 인덱스 계속 바꿔주어야 한다.
    var testMap2 = filterRdd.map(x=>{
      var maxValue = 5000
      var qty = x.getString(qtyNo).toDouble
      var new_qty = qty
      if(qty > maxValue){
        new_qty = maxValue
      }
      (
        x.getString(regionNo),
        x.getString(yearweekNo),
        x.getString(productGroupNo),new_qty)

    })


    // regionid(0), yearweek(1), product(2), qty(3)

    var groupByRdd = testMap2.groupBy(x=>{
      (x._1, x._3)
  }).map(x=>{
    var key = x._1
    var data = x._2
    var sumation = data.map(x=>{
      x._4}).sum
    var size = data.size
    var average = 0.0d
    if(size !=0){
      average = sumation / size
    }

    (key,average)
      //(key,(size,qty,...)) 이런식으로 쓸 수 있다.
      //key:value임으로 res에 저장이 되면 res(key)값을 부르면 value가 나온다 또 res(key)._1, res(key)._2 도 나온다.
  })

    var groupMap = groupByRdd.collectAsMap()


  }
}
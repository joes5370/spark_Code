package com.haiteam

import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object Spark0613 {
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

    //Rdd 변환
    var rawRdd = paramData.rdd

    rawRdd.collect.foreach(println)

    //데이터 프레임 컬럼 인덱스 = 하드코딩을 빼기 위해서
    var rawDataColumns = paramData.columns

    var regionNo = rawDataColumns.indexOf("REGIONID")
    var productNo = rawDataColumns.indexOf("PRODUCTGROUP")
    var yearweekNo = rawDataColumns.indexOf("YEARWEEK")
    var qtyNo = rawDataColumns.indexOf("VOLUME")

    //-1이 나오면 컬럼이 없는 것이다.

    //데이터 정제
    var filterRdd = rawRdd.filter(x=>{
    //디버깅 코드
    // var x = rawRdd.first 무잒위로 하나의 x를 가져온다.
    var checkValid = true

      //한행 내 특정 컬럼 추출
    var yearweek = x.getString(yearweekNo)

      //조건 수행
    if(yearweek.length > 6) {
      checkValid = false}
      checkValid
    })

    //52주차 초과 제거

    var filterRddQuiz = rawRdd.filter(x=>{
      var checkValid = true
      var yearweek = x.getString(yearweekNo)
      if(yearweek.substring(4,6).toInt > 52){
        checkValid = false}
      checkValid
    })

    var maxRdd = filterRddQuiz.map(x=>{
      var qty = x.getString(qtyNo).toDouble
      var maxValue = 700000
      if(qty > maxValue){qty = maxValue}
      Row( x.getString(yearweekNo),
        qty)
    })

    var minQuiz = filterRddQuiz.map(x=>{
      var qty = x.getString(qtyNo).toDouble
      var minValue = 150000
      if(qty < minValue){ qty = minValue}
      Row(
        x.getString(regionNo),
        x.getString(productNo),
        x.getString(yearweekNo),qty,qty*1.2)
    })

    //if를 통한 개선
    var minIfQuiz = filterRddQuiz.map(x=>{
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



    //group-by
    var groupRdd = minQuiz.groupBy(x=>{
      (x.getString(regionNo), x.getString(productNo))
    }).map(x=>{

      //debug
      // var x = .first해서 하기
      var key = x._1
      var data = x._2
      var sumation = data.map(x=>{
        x.getDouble(qtyNo)
      }).sum
      var size = data.size
      var average = 0.0d
      if(size != 0){
        average = sumation/size
      }else{
        average = 0.0d
      }
      (key,average)
      //(key,size)
      //(key,sumation)
      //(key,average)
    })

    var groupMap = groupRdd.collectAsMap()
  }

}

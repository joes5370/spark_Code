package com.haiteam

import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object Spark0620 {

  val conf = new SparkConf().
    setAppName("DataLoading").
    setMaster("local[*]")
  var sc = new SparkContext(conf)
  val spark = new SQLContext(sc)


  // 데이터 파일 로딩
  // 파일명 설정 및 파일 읽기 (2)
  var join = "joindata1.csv"
  var join2 = "joindata2.csv"
  // 절대경로 입력
  var joinData=
    spark.read.format("csv").
      option("header","true").
      option("Delimiter",","). // csv파일 불러올때는 꼭 확인해주자
      load("C:/spark_orgin_2.2.0/bin/data/"+join)

  var joinData2=
    spark.read.format("csv").
      option("header","true").
      option("Delimiter",","). // csv파일 불러올때는 꼭 확인해주자
      load("C:/spark_orgin_2.2.0/bin/data/"+join2)

  // 데이터 확인 (3)
  var joinDataColumns = joinData.columns.map(x=>{x.toLowerCase()})

  var mataTinNo = joinDataColumns.indexOf("mata_tin")
//  var mataSexCdNo = joinDataColumns.indexOf("mata_txpr_sex_cd")
//  var mateMdfYnNo = joinDataColumns.indexOf("mate_mdf_yn")
//  var qtyNo = joinDataColumns.indexOf("mate_srcs_cntn")
//  var memRitNo = joinDataColumns.indexOf("mem_rit_cd")
//  var ritEndDtNo = joinDataColumns.indexOf("rit_end_dt")
//  var rltStrtDtNo = joinDataColumns.indexOf("rit_strt_dt")
  var sutaTinNo = joinDataColumns.indexOf("suta_tin")
//  var sutaTxprSexCdNo = joinDataColumns.indexOf("suta_txpr_sex_cd")


  //컬럼 소문자
  var joinData2Columns = joinData2.columns.map(x=>{x.toLowerCase()})
  var sutaNo = joinData2Columns.indexOf("suta_tin")
  var mataNo = joinData2Columns.indexOf("mata_tin")


  var joinRdd = joinData.rdd
  var joinRdd2 = joinData2.rdd


  //(key,(size,qty,...)) 이런식으로 쓸 수 있다.
  //key:value임으로 res에 저장이 되면 res(key)값을 부르면 value가 나온다 또 res(key)._1, res(key)._2 도 나온다.
  var groupByRdd = joinRdd2.groupBy(x=>{
    x.getString(sutaNo)
  }).map(x=>{
    var key = x._1.trim()
    var data = x._2
    var matatinValue = data.map( x=>{x.getString(mataNo).trim()}).toArray
    (key,matatinValue(0))
  })
  var collectAs = groupByRdd.collectAsMap()



  var answer = joinRdd.map(x=>{
    var org_sutatim = x.getString(sutaTinNo).trim()
    var new_sutatin = ""

    //기존테이블 정보가 수정테이블 정보에 있으면 수정테이블의 값으로 없데이트
    if(collectAs.contains(org_sutatim)){
      new_sutatin = collectAs(org_sutatim)
    }else{
      //없으면 기존 sutatin 유지
      new_sutatin = org_sutatim
    }
    //Define outputLayout
    (org_sutatim,new_sutatin)
  })


  //공백제거 String에서는 replace = x._1.replace(" ","")

//  var tempMap = joinRdd.map(x=>{
//    var suta = x.get(sutaTinNo).toString.toDouble
//    var suta2 = x.get(sutaNo).toString.toDouble
//    var new_sutatim = x.get(mataTinNo).toString.toDouble
//
//    if(suta == suta2){
//      suta = mataTinNo
//      new_sutatim = suta
//    }
//
//    Row(
//      x.getString(mataTinNo),
//      x.getString(mataSexCdNo),
//      x.getString(mateMdfYnNo),
//      x.getString(qtyNo),
//      x.getString(memRitNo),
//      x.getString(ritEndDtNo),
//      x.getString(rltStrtDtNo),
//      x.getString(sutaTinNo),
//      x.getString(sutaTxprSexCdNo),
//      new_sutatim
//    )
//  })
//
//  Map으로 데이터들을 정제하고, 분석한 후 결과값을 컬럼에 추가하려면 이전 컬럼들을 다 써줘야 했던 문제
//  때문에 코드가 길어진다.
//    그문제를 해결하기 위해 교수님께서 하신 방법은
//
//  Var test2 = join1Rdd.map(x=>{
//    Var originalColums = x.toSeq,toList
//    Var append = 0.0d
//    Var append1 = 0.0d
//    Var append2= 0.0d
//    Row.fromSeq(originalColumns ++ List(append,append1,append2) ) })

  var paramFile = "kopo_product_volume.csv"

  // 절대경로 입력
  var paramData=
    spark.read.format("csv").
      option("header","true").
      option("Delimiter",","). // csv파일 불러올때는 꼭 확인해주자
      load("C:/spark_orgin_2.2.0/bin/data/"+paramFile)

  // 데이터 확인 (3)

  var rawDataColumns = paramData.columns
  var regionidNo = rawDataColumns.indexOf("REGIONID")
  var productNo = rawDataColumns.indexOf("PRODUCTGROUP")
  var yearweekNo = rawDataColumns.indexOf("YEARWEEK")
  var qtyNo = rawDataColumns.indexOf("VOLUME")

  var rawRdd = paramData.rdd

  var testSort = rawRdd.groupBy(x=>{
    (x.getString(regionidNo))
  }).map(x=>{
    var key = x._1
    var data = x._2
    var sortedValue = data.toArray.sortBy(x=>{x.getString(yearweekNo)})
                                            //{ - (x.getString(yearweekNo)).toInt} = 내림차순

    // var zipData = sortedValue.zipWithIndex 는 0부터 155번까지 컬럼 로우 번호를 알려준다.
  })


  import scala.collection.mutable.ArrayBuffer
  var rowBuffer =  new ArrayBuffer[org.apache.spark.sql.Row]
  var genBuffer =  new ArrayBuffer[(String, String, String, Double)]

  var regionid = "A01"
  var product = "PRODUCT1"

  var currWeek = 201926
  var iterWeek = 24

  var currDate = 201926
  var i = 0
  while( i < iterWeek) {
    rowBuffer.append(Row(regionid,
      product,
      (currDate+i).toString,
      0.0d))
    i= i+1
  }




}

package com.haiteam


import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object Spark0618_DB {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("DataFrame").
      setMaster("local[*]")
    var sc = new SparkContext(conf)
    val spark = new SQLContext(sc)


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

    //rdd -> collectASmap (key,value)

    /////////////////////////////////////////////////////////////////////////////////
    // Set 이해 잘 안된다. 끝나고 다시 여쭤보기!!!!!!
    //var a= "10".toList.toSet
        var paramMap = filterRdd.map(x=>{
          var MIN_VALUE = 150000
          var qty = x.getString(qtyNo).toDouble

          var new_qty = qty
          if (qty < MIN_VALUE) {
            new_qty = MIN_VALUE
          }
          Row(
            x.getString(regionNo),
            x.getString(productGroupNo),
            x.getString(yearweekNo),new_qty)
        });

    var tempArray = Array("A01")
    var tempArray2 = Array("ST0001")


    var REGIONSET = if(paramMap.contains("COMMON","VALID_REGION")){
      paramMap("COMMON","VALID_REGION").toSet
    }else{
      List("ALL").toSet
    }
    var PRODUCTSET = if(paramMap.contains("COMMON","VALID_REGION")){
      paramMap("COMMON","VALID_REGION").toSet
    }else{
      List("ALL").toSet
    }

    var filterRdd1 = rawRdd.filter(x=>{
      var checkValid = false
      var productInfo = x.getString(productGroupNo)
      var regionInfo = x.getString(regionNo)
      if((REGIONSET.contains(regionInfo)) || PRODUCTSET.contains(productInfo)){
        checkValid = true
      }
      checkValid
    })
////////////////////////////////////////////////////////////////////////////////////////




    var testMap4 = filterRdd.map(x=>{
      var maxValue = 5000
      var qty = x.get(qtyNo).toString.toDouble
      var new_qty = qty
      if(qty > maxValue){
        new_qty = maxValue
      }
      Row(
        x.getString(regionNo),
        x.getString(yearweekNo),
        x.getString(productGroupNo),
        new_qty)

    })

    yearweekNo

    // (regionid, product, yearweek, qty,)

    // regionid(0), yearweek(1), product(2), qty(3)

    //재정의 하는 부분
    regionNo=0
    yearweekNo=1
    productGroupNo = 2
    // 다시 해보기

    var groupByRdd8 = testMap4.groupBy(x=>{
      (x.getString(0), x.getString(2))
    }).map(x=>{
      var key = x._1
      var data = x._2
      var sumation = data.map(x=>{
        x.getDouble(qtyNo)}).sum
      var size = data.size
      var average = 0.0d
      if(size !=0){
        average = sumation / size
      }

      (key,average)
    })

    //groupBy flatMap
    //꼭 디버깅을 한 상태에서 하자

    var tempMap = filterRdd.map(x=>{
      var qty = x.get(qtyNo).toString.toDouble
      var new_qty = qty
      Row(
        x.getString(regionNo),
        x.getString(yearweekNo),
        x.getString(productGroupNo),new_qty
      )
    })


    var flatGroup2 = tempMap.groupBy(x=>{
      (x.getString(regionNo),x.getString(2)
      )

    }).flatMap(x=>{
      var key = x._1
      var data = x._2
      var size = data.size
      var outputData = data.map(x=>{(
        x.getString(regionNo),
        x.getString(productGroupNo),
        x.getString(yearweekNo),
        x.getDouble(qtyNo),
        size
      )
      })
      outputData
    })

    var resultFlat = tempMap.groupBy(x=>{
      (x.getString(regionNo), x.getString(2)
      )
    }).flatMap(x=>{
      var key = x._1
      var data = x._2
      var size = data.size
      var sumation = data.map(x=>{
        x.getDouble(qtyNo)}).sum
      var average = 0.0d
      if(size != 0 ){
        average = sumation / size
      }

      var outputData = data.map(x=>{(
        x.getString(regionNo),
        x.getString(productGroupNo),
        size,
        average)
      })
      outputData
    })

  }

}


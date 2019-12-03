package com.haiteam

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext}

object 문제원형_0702 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().
      setAppName("DataLoading").
      setMaster("local[*]")
    var sc = new SparkContext(conf)
    val spark = new SQLContext(sc)
    import spark.implicits._

    var salesFile = "pro_actual_sales.csv"
    // 절대경로 입력
    var salesDf =
      spark.read.format("csv").
        option("header", "true").
        option("Delimiter", ",").
        load("C:/spark_orgin_2.2.0/bin/data/" + salesFile)

    // 데이터 확인 (3)
    print(salesDf.show(2))

    var salesColums = salesDf.columns.map(x => {
      x.toLowerCase()
    })
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

    var promotionDf =
      spark.read.format("csv").
        option("header", "true").
        option("Delimiter", ",").
        load("C:/spark_orgin_2.2.0/bin/data/" + promotionFile)

    print(promotionDf.show(2))

    var promotionColums = promotionDf.columns.map(x => {
      x.toLowerCase()
    })
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

    var promotionRdd = promotionDf.rdd
    var minPlanWeek = promotionRdd.map(x => {
      x.getString(planweekno).toInt
    }).min()

    var filteredPromotion = promotionRdd.filter(x => {
      var check = false
      var targetWeek = x.getString(targetweekno3)
      var map_price = x.getString(map_priceno)
      if (targetWeek.toInt >= minPlanWeek) {
        check = true
      }
      check
    })

    //디버깅코드 NaN check!!!!!!!!!!!
    //    var processRdd2 = filteredPromotion.groupBy(x => {
    //      (x.getString(productgroup2), x.getString(itemno))
    //    })
    //
    //    var x = processRdd2.filter(x=>{
    //      var chaeck = false
    //      if( (x._1._1 == "PG01") &&
    //        (x._1._2 == "ITEM0466")){
    //        chaeck=true
    //      }
    //      chaeck
    //    }).first()


    var processRdd = filteredPromotion.groupBy(x => {
      (x.getString(productgroup2), x.getString(itemno))
    }).flatMap(x => {
      var key = x._1
      var data = x._2

      var mapPrice = data.map(x => {
        x.getString(map_priceno)
      }).toArray

      var mapSum = data.map(x => {
        x.getString(map_priceno).toDouble
      }).sum

      var count_nz = data.filter(x => {
        var checkValid = false
        if (x.getString(map_priceno).toInt > 0) {
          checkValid = true
        }
        checkValid
      }).size

      var new_mapPrice =
        if (mapPrice.contains("0")) {
          if (count_nz != 0) {
            mapSum / count_nz
          } else {
            0
          }
        } else {
          mapPrice(0)
        }

      var result = data.map(x => {
        var pmap = if (new_mapPrice == 0) {
          0
        } else {
          new_mapPrice.toString.toDouble - x.getString(irno).toDouble
        }
        var pmap10 = if (pmap == 0) {
          0
        } else {
          pmap * 0.9
        }
        var pro_percent = if (pmap10 == 0) {
          0
        } else {
          1 - (pmap10 / new_mapPrice.toString.toDouble)
        }
        var ir = if (new_mapPrice == 0) {
          0
        } else {
          x.getString(irno)
        }

        (x.getString(regionidno),
          x.getString(salesidno),
          x.getString(productgroup2),
          x.getString(itemno),
          x.getString(targetweekno3),
          x.getString(planweekno),
          new_mapPrice.toString.toDouble,
          ir.toString.toDouble,
          pmap,
          math.round(pmap10),
          pro_percent)
      })
      result
    })

    var resultMap = processRdd.map(x => {
      (x._1, x._2, x._3, x._4, x._5, x._6, x._7, x._8, x._9, x._10, x._11)
    })


    var testDf = resultMap.toDF("REGIONSEG", "SALESID", "PRODUCTGROUP", "ITEM", "TARGETWEEK", "PLANWEEK", "MAP_PRICE", "IR", "PMAP", "PMAP10", "PRO_PERCENT")

    testDf.createOrReplaceTempView("filterPromotion")

    salesDf.createOrReplaceTempView("salesData")

    var leftJoinData = spark.sql("""SELECT A.*,B.MAP_PRICE,B.IR,B.PMAP,B.PMAP10,B.PRO_PERCENT FROM salesData A left join filterPromotion B ON A.regionseg1 = B.REGIONSEG AND A.productseg2 = B.PRODUCTGROUP AND A.regionseg2 = B.SALESID AND A.productseg3 = ITEM and A.yearweek = B.TARGETWEEK""")

    leftJoinData.
      coalesce(1). // 파일개수
      write.format("csv"). // 저장포맷
      mode("overwrite"). // 저장모드 append/overwrite
      option("header", "true"). // 헤더 유/무
      save("c:/spark/bin/data/leftJoin.csv") // 저장파일명

    //    testDf.
    //      coalesce(1). // 파일개수
    //      write.format("csv").  // 저장포맷
    //      mode("overwrite"). // 저장모드 append/overwrite
    //      option("header", "true"). // 헤더 유/무
    //      save("c:/spark/bin/data/refine_pro_promotion.csv") // 저장파일명

  }
}

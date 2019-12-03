package com.haiteam

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object 문제원형_0704 {
  def main(args: Array[String]): Unit = {
      val conf = new SparkConf().
        setAppName("DataLoading").
        setMaster("local[*]")
      var sc = new SparkContext(conf)
      val spark = new SQLContext(sc)
      import spark.implicits._

      var seasonFile = "seasonalityData.csv"
      // 절대경로 입력
      var seasonDf =
        spark.read.format("csv").
          option("header", "true").
          option("Delimiter", ",").
          load("C:/spark_orgin_2.2.0/bin/data/" + seasonFile)

    def postWeek(inputYearWeek: String, gapWeek: Int): String = {
      var currYear = inputYearWeek.substring(0, 4).toInt
      var currWeek = inputYearWeek.substring(4, 6).toInt

      val calendar = Calendar.getInstance();
      calendar.setMinimalDaysInFirstWeek(4);
      calendar.setFirstDayOfWeek(Calendar.MONDAY);

      var dateFormat = new SimpleDateFormat("yyyyMMdd");

      calendar.setTime(dateFormat.parse(currYear + "1231"));

      var maxWeek = calendar.getActualMaximum(Calendar.WEEK_OF_YEAR)

      var conversion = currWeek + gapWeek
      if (maxWeek < conversion) {
        while (maxWeek < conversion) {
          currWeek = conversion - maxWeek
          currYear = currYear + 1
          calendar.setTime(dateFormat.parse(currYear + "1231"));
          maxWeek = calendar.getActualMaximum(Calendar.WEEK_OF_YEAR)
          conversion = currWeek
        }
        return currYear.toString() + "%02d".format((currWeek))
      } else {
        return currYear.toString() + "%02d".format((currWeek + gapWeek))
      } // end of if
    }

    var seasonColums = seasonDf.columns.map(x => {x.toLowerCase()})
    var regionidno = seasonColums.indexOf("regionseg1")
    var productno2 = seasonColums.indexOf("productseg2")
    var regionidno2 = seasonColums.indexOf("regionseg2")
    var regionidno3 = seasonColums.indexOf("regionseg3")
    var productno3 = seasonColums.indexOf("productseg3")
    var yearweekno = seasonColums.indexOf("yearweek")
    var yearno = seasonColums.indexOf("year")
      var weekno = seasonColums.indexOf("week")
      var qtyno = seasonColums.indexOf("qty")
      var map_priceno = seasonColums.indexOf("map_price")
      var irno = seasonColums.indexOf("ir")
      var pmapno = seasonColums.indexOf("pmap")
      var pmap10no = seasonColums.indexOf("pmap10")
      var pro_percentno = seasonColums.indexOf("pro_percent")
      var promotionCheck = seasonColums.indexOf("promotion_check")
      var movingAvg = seasonColums.indexOf("moving_avg")
      var seasonalityNo = seasonColums.indexOf("seasonality")

      var seasonRdd = seasonDf.rdd

      //단종로직 step1 => 201620이 있는 제품들만 추출
      var validYearWeek = "201620"
      var exceptionFiltered = seasonRdd.groupBy(x=>{
        (x.getString(regionidno),
          x.getString(productno2),
        x.getString(regionidno2),
        x.getString(regionidno3),
        x.getString(productno3))
    }).filter(x=>{
      var checkValid = false
      var key = x._1
      var data = x._2
      var yearWeek = data.map(x=>{x.getString(yearweekno)}).max

      if(yearWeek >= validYearWeek){
        checkValid = true
      }
      checkValid
    }).flatMap(x=>{
      var key = x._1
      var data = x._2
      var result = data.map(x=>{
        (x.getString(regionidno),
          x.getString(productno2),
          x.getString(regionidno2),
          x.getString(regionidno3),
          x.getString(productno3),
          x.getString(yearweekno),
          x.getString(yearno),
          x.getString(weekno),
          x.getString(qtyno),
          x.getString(map_priceno),
          x.getString(irno),
          x.getString(pmapno),
          x.getString(pmap10no),
          x.getString(pro_percentno),
          x.getString(promotionCheck),
          x.getString(movingAvg),
          x.getString(seasonalityNo))
      })
      result
    })

    var sortedData = exceptionFiltered.sortBy(x=>(x._1,x._2,x._3,x._4,x._5,x._6))

    var sortDf = sortedData.toDF("REGIONSEG1", "PRODUCTSEG2", "REGIONSEG2", "REGIONSEG3", "PRODUCTSEG3", "YEARWEEK", "YEAR", "WEEK", "QTY", "MAP_PRICE", "IR", "PMAP", "PMAP10", "PRO_PERCENT","PROMOTION_CHECK","MOVING_AVG","SEASONALITY")

//    sortDf.
//      coalesce(1). // 파일개수
//      write.format("csv"). // 저장포맷
//      mode("overwrite"). // 저장모드 append/overwrite
//      option("header", "true"). // 헤더 유/무
//      save("c:/spark/bin/data/filtered.csv") // 저장파일명

    var forecastRdd = sortDf.rdd
    var season1Colums = sortDf.columns.map(x => {x.toLowerCase()})
    var regionidno0 = season1Colums.indexOf("regionseg1")
    var productno20 = season1Colums.indexOf("productseg2")
    var regionidno20 = season1Colums.indexOf("regionseg2")
    var regionidno30 = season1Colums.indexOf("regionseg3")
    var productno30 = season1Colums.indexOf("productseg3")
    var yearweekno0 = season1Colums.indexOf("yearweek")
    var yearno0 = season1Colums.indexOf("year")
    var weekno0 = season1Colums.indexOf("week")
    var qtyno0 = season1Colums.indexOf("qty")
    var map_priceno0 = season1Colums.indexOf("map_price")
    var irno0 = season1Colums.indexOf("ir")
    var pmapno0 = season1Colums.indexOf("pmap")
    var pmap10no0 = season1Colums.indexOf("pmap10")
    var pro_percentno0 = season1Colums.indexOf("pro_percent")
    var promotionCheck0 = season1Colums.indexOf("promotion_check")
    var movingAvg0 = season1Colums.indexOf("moving_avg")
    var seasonalityNo0 = season1Colums.indexOf("seasonality")

    var outYearWeek = "201631"

    var maxYearWeek = forecastRdd.map(x=>{
      var yearweek = x.getString(yearweekno0).toInt
      yearweek
    }).max

    var maxWeek = forecastRdd.map(x=>{
      var week = x.getString(weekno0).toInt
      week
    }).max


//    디버깅코드 NaN check!!!!!!!!!!!
//    var processRdd2 = forecastRdd.groupBy(x => {
//      (x.getString(regionidno0),
//        x.getString(productno20),
//        x.getString(regionidno20),
//        x.getString(regionidno30),
//        x.getString(productno30)
//      )
//    })
//    var x = processRdd2.filter(x=>{
//      var chaeck = false
//      if( (x._1._1 == "A01") &&
//        (x._1._2 == "PG05")&&
//        (x._1._3 == "SALESID0001")&&
//        (x._1._4 == "SITEID0001")&&
//        (x._1._5 == "ITEM0024")){
//        chaeck=true
//      }
//      chaeck
//    }).first()


    var forecastMap = forecastRdd.groupBy(x=>{
      (x.getString(regionidno0),
        x.getString(productno20),
        x.getString(regionidno20),
        x.getString(regionidno30),
        x.getString(productno30)
      )
    }).flatMap(x=>{
      var key = x._1
      var data = x._2

      //fcst구하기 201624,201625,201626,201627의 qty를 보두 합한 후 4로 나눈 값
      var filtered = data.filter(x=>{
        var yearweek = x.getString(yearweekno0).toInt
        maxYearWeek - yearweek <= 3
      })
      var size = 4
      var qtySum = if(filtered.size != 0){
        filtered.map(x=>{
          var qty = x.getString(qtyno0).toDouble
          qty
        }).sum
      }else{
        0d
      }
      var fcst = qtySum / size

      //201624,201625,201626,201627의 계절성지수 합에 4로 나누어서 시계열 계산에 들어감
      var seasonality = if(filtered.size != 0){
        filtered.map(x=>{
          var tempseason = x.getString(seasonalityNo0).toDouble
          tempseason
        }).sum
      }else{
        0d
      }
      var originAvgSeason = seasonality/size

      //201628,201629,201630,201631 행 추가
      var yearweekMin = data.map(x => x.getString(yearweekno0)).min
      var yearweek = data.map(x=>{x.getString(yearweekno0)}).toArray.sorted

      var i = 1
      var tempYearweek = Array(yearweekMin)
      while (tempYearweek.last < outYearWeek) {
        tempYearweek ++= Array(postWeek(yearweekMin.toString, i))
        i = i + 1
      }
      var conversionArray = tempYearweek.diff(yearweek)

      //이전 28,29,30,31주차 개별 seasonality 가져오기
      var weektemp = conversionArray.map(x=>{x.substring(4,6)})
      var temp = Array("28")
      var originWeek = data.map(x=>{x.getString(yearweekno0).substring(4,6)}).toArray.sorted

      var seasonaltyfiltered = data.filter(x=>{
        var check = false
        for (i <- temp) {
          if (originWeek.contains(i)) {
            check = true
          }
        }
        check
      })

      var seasonaltyValue = if(seasonaltyfiltered.size != 0){
        seasonaltyfiltered.map(x=>{
          var seasonality = x.getString(seasonalityNo0).toDouble
          seasonality
        }).sum
      }else{
        0d
      }
      var realseasonality = seasonaltyValue / size

      //시계열 구하기
      var fcst_timeseries = if (originAvgSeason != 0){
        ((fcst * seasonaltyValue) / originAvgSeason)
      }else{
        0d
      }

      val tmpMap = conversionArray.map(x => {
        val year = x.substring(0, 4)
        val week = x.substring(4, 6)
        val yearweek = year+week
        val qty = 0
        val map_price = 0
        val ir = 0
        val pmap = 0d
        val pmap10 = 0d
        val pro_percent = 0d
        val promotion_check = "N"
        val movingAVG =0d
        (key._1,
          key._2,
          key._3,
          key._4,
          key._5,
          yearweek,
          year,
          week,
          qty.toString,
          map_price.toString,
          ir.toString,
          pmap.toString,
          pmap10.toString,
          pro_percent.toString,
          promotion_check.toString,
          movingAVG.toString,
          realseasonality.toString,
          fcst.toString,
          fcst_timeseries.toString
          )
      })
      var result = data.map(x=>{
        var Allfcst = "0"
        var timeseies = "0"
        (x.getString(regionidno),
          x.getString(productno2),
          x.getString(regionidno2),
          x.getString(regionidno3),
          x.getString(productno3),
          x.getString(yearweekno),
          x.getString(yearno),
          x.getString(weekno),
          x.getString(qtyno),
          x.getString(map_priceno),
          x.getString(irno),
          x.getString(pmapno),
          x.getString(pmap10no),
          x.getString(pro_percentno),
          x.getString(promotionCheck),
          x.getString(movingAvg),
          x.getString(seasonalityNo),
          Allfcst,
          timeseies)
      })
      tmpMap ++ result
    })

  }
}

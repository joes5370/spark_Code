package com.haiteam

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object Spark0628_Test {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().
      setAppName("DataLoading").
      setMaster("local[*]")
    var sc = new SparkContext(conf)
    val spark = new SQLContext(sc)

    import spark.implicits._
    import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode}
    import org.apache.spark.sql.types.{StringType, StructField, StructType}

    var targetFile = "pro_actual_sales.csv"

    // 절대경로 입력
    var targetDf=
      spark.read.format("csv").
        option("header","true").
        option("Delimiter",",").
        load("C:/spark_orgin_2.2.0/bin/data/"+targetFile)

    // 데이터 확인 (3)
    print(targetDf.show(2))

    var salesColums = targetDf.columns.map(x=>{x.toLowerCase()})
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

    var targetRdd = targetDf.rdd


    //2019.06.28 1번문제
    //연결,연결이 안되있고, 한번에 구함... 연결시키는 쪽 공부 필요 어케하지???
    var targetA = targetRdd.groupBy(x=>{
      (x.getString(regionidno1),x.getString(productno2),x.getString(yearweekno))
    }).map(x=>{
      var key = x._1
      var data = x._2
      var qtySum = data.map(x=>{x.getString(qtyno).toDouble}).sum

      (key._1,key._2,key._3, qtySum)
    })


    var targetB = targetA.groupBy(x=>{
      (x._1,x._2)
    }).map(x=>{
      var key = x._1
      var data = x._2
      var size = data.size
      var qtySum = data.map(x=>{x._4}).sum
      var avg = 0.0d
      if(size != 0 ){
        avg = qtySum / size
      }else{
        avg
      }
      (key, avg)
    }).collectAsMap


    var processOne = targetA.map(x=>{
      var avg = 1.0d
      var ratio = 0.0d
      if(targetB.contains(x._1,x._2)){avg = targetB(x._1,x._2)}
      if(avg != 0){ratio = x._4 / avg}
      (x._1,x._2,x._3,ratio)
    })

    var answerOne = processOne.groupBy(x=>{
      x._3
    }).map(x=>{
      var key = x._1
      var data =x._2
      var ratio = data.map(x=>{x._4})

      (key,ratio)
    })




//    var targetB = targetRdd.groupBy(x=>{
//      (x.getString(regionidno1),x.getString(productno2))
//    }).flatMap(x=>{
//      var key = x._1
//      var data = x._2
//      var size = data.size
//
//      //targetA와 연결 어떻해 하는지 공부해야함
//      var qtySum = data.map(x=>{x.get(qtyno).toString.toDouble}).sum
//      var avg = 0.0d
//      if(size != 0){
//        avg = qtySum / size
//      }else{
//        avg = 0.0d
//      }
//
//      var finalData = data.map(x=>{
//        var ratio = 1.0d
//        var each_qty = x.get(qtyno).toString.toDouble
//        if(avg != 0){
//          ratio = each_qty / avg
//        }
//        (x.getString(yearweekno),ratio)
//      })
//      finalData
//    })




    //2019.06.28 2번문제(심화)
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
        while(maxWeek < conversion){
          currYear = currYear + 1
          currWeek = conversion - maxWeek
          conversion = currWeek
        }
        return currYear.toString() + "%02d".format((currWeek))
      } else {
        return currYear.toString() + "%02d".format((currWeek + gapWeek))
      } // end of if
    }

    def preWeek(inputYearWeek: String, gapWeek: Int): String = {
      val currYear = inputYearWeek.substring(0, 4).toInt
      val currWeek = inputYearWeek.substring(4, 6).toInt

      val calendar = Calendar.getInstance();
      calendar.setMinimalDaysInFirstWeek(4);
      calendar.setFirstDayOfWeek(Calendar.MONDAY);

      var dateFormat = new SimpleDateFormat("yyyyMMdd");

      calendar.setTime(dateFormat.parse(currYear + "1231"));
      //    calendar.getActualMaximum(Calendar.WEEK_OF_YEAR)

      if (currWeek <= gapWeek) {
        var iterGap = gapWeek - currWeek
        var iterYear = currYear - 1

        calendar.setTime(dateFormat.parse(iterYear.toString() + "1231"));
        var iterWeek = calendar.getActualMaximum(Calendar.WEEK_OF_YEAR)

        while (iterGap > 0) {
          if (iterWeek <= iterGap) {
            iterGap = iterGap - iterWeek
            iterYear = iterYear - 1
            calendar.setTime(dateFormat.parse(iterYear.toString() + "1231"))
            iterWeek = calendar.getActualMaximum(Calendar.WEEK_OF_YEAR)
          } else {
            iterWeek = iterWeek - iterGap
            iterGap = 0
          } // end of if
        } // end of while

        return iterYear.toString + "%02d".format(iterWeek)
      } else {
        var resultYear = currYear
        var resultWeek = currWeek - gapWeek

        return resultYear.toString + "%02d".format(resultWeek)
      } // end of if
    }


    //미래 주차 생성 increment
    var maxWeek = targetRdd.map(x=>{x.getString(yearweekno)}).max()
    var startWeek = postWeek(maxWeek,1)
    var FCST_WINDOW = startWeek


    //최근 4주차 정보 뽑기
    var beforFourWeek = preWeek(maxWeek,4)
    var fourWeekFiltered = targetRdd.filter(x=>{
      var checkOut = false
      if(x.getString(yearweekno).toInt > beforFourWeek.toInt){
        checkOut = true
      }
      checkOut
    })

    var answerMap = fourWeekFiltered.groupBy(x=>{
      (x.getString(regionidno1),x.getString(regionidno2),x.getString(productno2),x.getString(productno3))
    }).flatMap(x=>{
      var key = x._1
      var data = x._2
      var size = data.size
      var qtySum = data.map(x=>{x.get(qtyno).toString.toDouble}).sum
      var avg = 0.1d
      if(size != 0 ){
        avg = qtySum / size
      }


      var orderedData = data.toArray.sortBy(x=>{
        x.getString(yearweekno)
      })

      // RDD 풀이 1

      var dataSize = orderedData.size
      var FCST = Array.fill(dataSize)(key._1, key._2,key._3,key._4,FCST_WINDOW,avg)

      var result = orderedData.map(x=>{
        (FCST)

      })
      result
    })



  }
}



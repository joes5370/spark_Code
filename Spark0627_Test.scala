package com.haiteam

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object Spark0627_Test {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("DataLoading").setMaster("local[*]")
    var sc = new SparkContext(conf)
    val spark = new SQLContext(sc)
    import spark.implicits._


    //06.27 spark issue: 0번 문제 데이터 불러서 111번 서버에 저장하기
    var staticUrl = "jdbc:oracle:thin:@192.168.110.112:1521/orcl"
    var staticUser = "kopo"
    var staticPw = "kopo"
    var selloutDb = "kopo_channel_seasonality_new"

    val dataFromOracle = spark.read.format("jdbc").
      option("url", staticUrl).
      option("dbtable", selloutDb).
      option("user", staticUser).
      option("password", staticPw).load()

    var seasonColums = dataFromOracle.columns.map(x=>{x.toLowerCase()})
    var regionidno = seasonColums.indexOf("regionid")
    var productno = seasonColums.indexOf("product")
    var yearweektno = seasonColums.indexOf("yearweek")
    var qtyno = seasonColums.indexOf("qty")

    var seasonRdd = dataFromOracle.rdd

    var seasonMap = seasonRdd.groupBy(x=>{
      (x.getString(regionidno),x.getString(productno))
    }).map(x=>{
      var key = x._1
      var data = x._2
      var qtySum = data.map(x=>{x.get(qtyno).toString.toDouble}).sum
      //groupby를 하면 key의 1,2는 무조건 하나 그래서 어떤 집계가 필요하다.

      (key._1, key._2,qtySum)
    })

    var finalResult = seasonMap.distinct()

    var outputDf = finalResult.toDF("REGIONID","PRODUCT","QTY")

    var outputUrl = "jdbc:oracle:thin:@192.168.110.111:1521/orcl"
    var outputUser = "kopo"
    var outputPw = "kopo"

    // 데이터 저장 (2)
    val prop = new java.util.Properties
    prop.setProperty("driver", "oracle.jdbc.OracleDriver")
    prop.setProperty("user", outputUser)
    prop.setProperty("password", outputPw)
    val table = "kopo_2019_HYUNGSUK"
    //append
    outputDf.write.mode("overwrite").jdbc(outputUrl, table, prop)

    //06.27 spark issue(1): PRO_ACTUAL_SALES의 데이터 1번 문제 데이터의 빈 공간에 실적 0이라고 채우자!

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

    var targetMap = targetRdd.groupBy(x=>{
      (x.getString(regionidno1),x.getString(productno1),x.getString(productno2),
        x.getString(regionidno2),x.getString(regionidno3),x.getString(productno3))
    }).flatMap(x=>{
      var key = x._1
      var data = x._2
      var yearweek = data.map(x=>{x.getString(yearweekno)}).toArray.sorted
      var yearweekMax = data.map(x=>{x.getString(yearweekno)}).max
      var yearweekMin = data.map(x=>{x.getString(yearweekno)}).min
      var i = 1
      //빈베열에 int값 들어가게 하기
      var tempYearweek = Array(yearweekMin)
      //주차를 넣을 배열의 마지막 값이 맥스와 같아지면 while문 탈출
      while(tempYearweek.last < yearweekMax){
        tempYearweek ++= Array(postWeek(yearweekMin.toString, i))
        i = i+1
      }

      var conversionArray = tempYearweek.diff(yearweek)

      val tmpMap = conversionArray.map(yearweek=>{
        val year = yearweek.substring(0, 4)
        val week = yearweek.substring(4, 6)
        val qty = 0.0d
        (key._1,key._2,key._3,key._4,key._5,key._6,yearweek,year,week,qty)
      })
      var resultMap = data.map(x=>{
        (x.getString(regionidno1),x.getString(i = productno1)
          ,x.getString(productno2),x.getString(regionidno2)
          ,x.getString(regionidno3),x.getString(productno3)
          ,x.getString(yearweekno),x.getString(yearno)
          ,x.getString(weekno),x.getString(qtyno).toDouble
          )
      })
      tmpMap ++ resultMap
    })
    targetMap.collect.foreach(println)

  }
}

//      while(tempYearweek.last.toInt < yearweekMax.toInt){
//        tempYearweek ++= Array(postWeek(yearweekMin.toString, i))
//        i = i+1
//      }

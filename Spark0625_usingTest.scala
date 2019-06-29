package com.haiteam

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}


object Spark0625_usingTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Test").setMaster("local[4]")
    var sc = new SparkContext(conf)
    val spark = new SQLContext(sc)
    import spark.implicits._
    //////////////////////////////////////////////////////////////////////////////////////////////////
    // 1. data loading
    //////////////////////////////////////////////////////////////////////////////////////////////////
    var staticUrl = "jdbc:oracle:thin:@192.168.110.112:1521/orcl"
    var staticUser = "kopo"
    var staticPw = "kopo"
    var selloutDb = "PROMOTION_MASTER"
    var selloutDb2 = "SELLOUT_MASTER"

    //SELLOUTDATA
    val promotionD = spark.read.format("jdbc").
      options(Map("url" -> staticUrl, "dbtable" -> selloutDb, "user" -> staticUser, "password" -> staticPw)).load

    //PROMOTIONDATA
    val selloutD = spark.read.format("jdbc").
      options(Map("url" -> staticUrl, "dbtable" -> selloutDb2, "user" -> staticUser, "password" -> staticPw)).load

    //0번 문제 RDD
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    var promotionColums = promotionD.columns.map(x=>{x.toLowerCase()})
    var proaccountno = promotionColums.indexOf("accountid")
    var proyearweekno = promotionColums.indexOf("yearweek")
    var propercentno = promotionColums.indexOf("pro_percent")

    //거래처, 연주차 정보로 그룹바이 한 후 percent값 꺼낸다
    var promotionRdd = promotionD.rdd


    var promotionMap = promotionRdd.groupBy(x=>{
      (x.getString(proaccountno),x.getString(proyearweekno))
    }).map(x=>{
      var key = x._1
      var data = x._2
      var promotion = data.map(x=>{x.get(propercentno).toString.toDouble}).toArray
      (key, promotion(0))
    }).collectAsMap

    var selloutRdd = selloutD.rdd

    var finalResultAnswer = selloutRdd.map(x=>{
      var accountid = x.getString(1)
      var yearweek = x.getString(2)
      var qty = x.get(3).toString.toDouble
      var promotion =
        if(promotionMap.contains(accountid,yearweek)){promotionMap(accountid,yearweek)
      }else{
        0.0
      }
      (accountid,yearweek,qty,promotion)
    })


    var finalAnswer = finalResultAnswer.toDF("ACCOUNTID","YEARWEEK","QTY","PROMOTION")
    val prop = new java.util.Properties
    prop.setProperty("driver", "oracle.jdbc.OracleDriver")
    prop.setProperty("user", staticUser)
    prop.setProperty("password", staticPw)
    val table = "FINAL_QUIZ_1"

    finalAnswer.write.mode("overwrite").jdbc(staticUrl, table, prop)

    //////////////////////////////////////////////////////////////////////////////////////////////////
    // 10. Exam #4 Save your final result in oracle (FINAL_2019ST_NAME) in english

//    //PROMOTIONDATA
//    var rawRdd_PRO = promotionD.rdd
//    var rawDataColumns_PRO = promotionD.columns
//
//    var accountNo_PRO = rawDataColumns_PRO.indexOf("ACCOUNTID")
//    var yearweekNo_PRO = rawDataColumns_PRO.indexOf("YEARWEEK")
//    var promotionNo_PRO = rawDataColumns_PRO.indexOf("PRO_PERCENT")



//SELLOUTDATA
//    var rawRdd = selloutD.rdd
//    var rawDataColumns = selloutD.columns

//    var regionNo = rawDataColumns.indexOf("REGIONID")
//    var accountNo = rawDataColumns.indexOf("ACCOUNTID")
//    var yearweekNo = rawDataColumns.indexOf("YEARWEEK")
//    var qtyNo = rawDataColumns.indexOf("QTY")

//    var proRawRdd = rawRdd_PRO.groupBy(x=>{
//      x.getString(accountNo_PRO)
//    }).map(x=>{
//      var key = x._1
//      var data = x._2
//      var promotionInfo = data.map(x=>{x.get(promotionNo_PRO).toString.toDouble})
//      (key,promotionInfo)
//    })
//
//    var proRawRddAsMap = proRawRdd.collectAsMap()
//
//    var answer = rawRdd.map(x=>{
//      var regionid = x.getString(regionNo).trim()
//      var new_promotion = 0.0d
//
//      if(proRawRddAsMap.contains(regionid)){
//        new_promotion = proRawRddAsMap(regionid)
//      }else{
//        new_promotion = regionid
//      }
//      (regionid, new_promotion)
//    })

    //0번문제 쿼리

    //SELLOUTDATA
    promotionD.createOrReplaceTempView("JOIN_PROMOTION_MASTER")

    //PROMOTIONDATA
    selloutD.createOrReplaceTempView("JOIN_SELLOUT_MASTER")
    //JOIN할때 데이터가 뻥튀기되는 점을 알고 있어야한다
    var outputResult = spark.sql("""
       SELECT A.*,B.PRO_PERCENT
       FROM JOIN_SELLOUT_MASTER A
       LEFT JOIN JOIN_PROMOTION_MASTER B
       ON A.ACCOUNTID = B.ACCOUNTID AND A.YEARWEEK = B.YEARWEEK
       ORDER BY A.REGIONID, A.ACCOUNTID,A.YEARWEEK""")

    //////////////////////////////////////////////////////////////////////////////////////////////////
    //1번문제
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
    } // end of function // end of function


    //2번문제
    //////////////////////////////////////////////////////////////////////////////////////////////////
    var join1Name = "pro_actual_sales.csv"
    var join1Df=
      spark.read.format("csv").
        option("header","true").
        option("Delimiter",",").
        load("C:/spark_orgin_2.2.0/bin/data/"+join1Name)

    var joinRdd = join1Df.rdd

    var joinDataColumns = join1Df.columns

    var regionSeg1No = joinDataColumns.indexOf("regionSeg1")
    var productSeg2No = joinDataColumns.indexOf("productSeg2")
    var yearweekNo = joinDataColumns.indexOf("yearweek")
    var qtykNo = joinDataColumns.indexOf("qty")


    regionSeg1No = 0
    productSeg2No = 2
    yearweekNo = 6

    var sumMap = joinRdd.groupBy(x=>{
      (x.getString(regionSeg1No),x.getString(productSeg2No),x.getString(yearweekNo))
    }).map(x=>{
      var key = x._1
      var data =x._2
      var sumatiom = data.map(x=>{x.getString(qtykNo).toDouble}).sum
      (key, sumatiom)
    }).collectAsMap()


    //3번문제
    //////////////////////////////////////////////////////////////////////////////////////////////////
    //오라클로 보내기
//    var middleResult = join1Df.toDF("REGIONSEG1","PRODUCTSEG1","PRODUCTSEG2","REGIONSEG2","REGIONSEG3","PRODUCTSEG3","YEARWEEK","YEAR","WEEK","QTY")
//    middleResult.createOrReplaceTempView("MIDDLETABLE")
//
//    var outputUrl = "jdbc:oracle:thin:@192.168.110.24:1522/XE"
//    var outputUser = "JHS"
//    var outputPw = "JHS"
//
//    val prop = new java.util.Properties
//    prop.setProperty("driver", "oracle.jdbc.OracleDriver")
//    prop.setProperty("user", outputUser)
//    prop.setProperty("password", outputPw)
//    val table = "pro_actual_sales"
//
//    //////////////////////////////////////////////////////////////////////////////////////////////////
//    middleResult.write.mode("overwrite").jdbc(outputUrl, table, prop)
//    println("finished")



  }
}

package com.haiteam

import org.apache.spark.{SparkConf, SparkContext}

object 정형석_spark기말고사 {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.sql.SparkSession
    import scala.collection.mutable.ArrayBuffer
    import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode}
    import org.apache.spark.sql.types.{StringType, StructField, StructType}

    ////////////////////////////////////  Spark-session definition  ////////////////////////////////////
    //var spark = SparkSession.builder().config("spark.master","local").getOrCreate()
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
    var selloutDb = "kopo_channel_seasonality_final"

    val selloutDataFromOracle = spark.read.format("jdbc").
      options(Map("url" -> staticUrl, "dbtable" -> selloutDb, "user" -> staticUser, "password" -> staticPw)).load

    selloutDataFromOracle.createOrReplaceTempView("keydata")

    println(selloutDataFromOracle.show())
    println("oracle ok")

    //////////////////////////////////////////////////////////////////////////////////////////////////
    // 2. data refining #1
    //////////////////////////////////////////////////////////////////////////////////////////////////
    var rawData = spark.sql("select concat(a.regionid,'_',a.product) as keycol, " +
      "a.regionid as accountid, " +
      "a.product, " +
      "a.yearweek, " +
      "cast(a.qty as String) as qty, " +
      "'test' as productname from keydata a" )

    var rawDataColumns = rawData.columns
    var keyNo = rawDataColumns.indexOf("keycol")
    var accountidNo = rawDataColumns.indexOf("accountid")
    var productNo = rawDataColumns.indexOf("product")
    var yearweekNo = rawDataColumns.indexOf("yearweek")
    var qtyNo = rawDataColumns.indexOf("qty")
    var productnameNo = rawDataColumns.indexOf("productname")

    var rawRdd = rawData.rdd

    // Global Variables //
    var VALID_YEAR = 2015
    var VALID_WEEK = 52
    var VALID_PRODUCT = Array("PRODUCT1","PRODUCT2").toSet
    var MAX_QTY_VALUE = 9999999.0

    // groupRdd1.collectAsMap

    //////////////////////////////////////////////////////////////////////////////////////////////////
    // 3. data refining #2
    //////////////////////////////////////////////////////////////////////////////////////////////////
    // The abnormal value is refined using the normal information
    var filterRdd = rawRdd.filter(x=>{

      var checkValid = true
      // Assign yearweek information to variables
      var year = x.getString(yearweekNo).substring(0,4).toInt
      var week = x.getString(yearweekNo).substring(4,6).toInt
      // Assign abnormal to variables
      // filtering
      if ((week > VALID_WEEK) ||
        (year < VALID_YEAR) ||
        (!VALID_PRODUCT.contains(x.getString(productNo))))
      {
        checkValid = false
      }
      checkValid
    })
    // output: key, account, product, yearweek, qty, productname

    //////////////////////////////////////////////////////////////////////////////////////////////////
    // 4. data processing
    //////////////////////////////////////////////////////////////////////////////////////////////////
    var mapRdd = filterRdd.map(x=>{
      var qty = x.getString(qtyNo).toDouble
      if(qty > MAX_QTY_VALUE){qty = MAX_QTY_VALUE}
      Row( x.getString(keyNo),
        x.getString(accountidNo),
        x.getString(productNo),
        x.getString(yearweekNo),
        qty, //x.getString(qtyNo),
        x.getString(productnameNo))
    })
    // output: key, account, product, yearweek, qty, productname

    //////////////////////////////////////////////////////////////////////////////////////////////////
    // 5. Exam #1 Fill in the blanks
    //////////////////////////////////////////////////////////////////////////////////////////////////
    var groupRdd1 = mapRdd.
      groupBy(x=>{ (x.getString(accountidNo),
        x.getString(productNo))}).
      map(x=>{
        // GROUP BY (accountid, product)
        var key = x._1
        var data = x._2

        // Calculate the average for each group (key, average)
        // var avg = data.mean(qty)
        var avg =1.0d
        // !!!!!!!!! Blanks Start
        var sumation = data.map(x=>{x.get(qtyNo).toString.toDouble}).sum
        var size = data.size
        if (size != 0){
          avg = sumation/size
        }

        // !!!!!!!!! Blanks End
        // (KEY, VALUE)
        (key,avg)
      })
    // output: (key, avg)


    //여기서 collectAsMap을 통해 avg땡겨서 6번문제의 avg를 구할 필요가 없었다!!!!!!!!!!!!!!!!!!!!!!!!
    groupRdd1.collectAsMap
    //////////////////////////////////////////////////////////////////////////////////////////////////
    // 6. Exam #2 Fill in the blanks
    //////////////////////////////////////////////////////////////////////////////////////////////////
    var groupRdd2 = mapRdd.
      groupBy(x=>{ (x.getString(accountidNo),
        x.getString(productNo))}).
      flatMap(x=>{
        // GROUP BY (accountid, product)
        var key = x._1
        var data = x._2

          // Calculate the average for each group
          // var avg = data.
          var avg =1.0d

          var sumation = data.map(x=>{x.get(qtyNo).toString.toDouble}).sum
          var size = data.size
          if (size != 0){
            avg = sumation/size
          }
          // Calulate the ratio , ratio = each_qty / avg
          var finalData = data.map(x=>{

            var ratio = 1.0d
            var each_qty = x.getDouble(qtyNo)
            // ratio =
            // !!!!!!!!! Blanks Start
            // 분모 0 처리를 해주어야 한다.
          if(avg != 0){
            ratio = each_qty / avg
          }
          // !!!!!!!!! Blanks End
          (x.getString(accountidNo),
            x.getString(productNo),
            x.getString(yearweekNo),
            x.getDouble(qtyNo),
            avg.toDouble,
            ratio.toDouble)})
        finalData
      })
    // output: (accountid, product,yearweek, qty, avg_qty, ratio)

    //////////////////////////////////////////////////////////////////////////////////////////////////
    // 7. Data converting (RDD -> Dataframe)
    //////////////////////////////////////////////////////////////////////////////////////////////////
    // RDD -> Dataframe can be converted immediately without defining a row
    var middleResult = groupRdd2.toDF("REGIONID","PRODUCT","YEARWEEK","QTY","AVG_QTY","RATIO")
    println(middleResult.show)

    //////////////////////////////////////////////////////////////////////////////////////////////////
    // 8. Exam #3 Fill in the blanks
    //////////////////////////////////////////////////////////////////////////////////////////////////
    middleResult.createOrReplaceTempView("MIDDLETABLE")

    // Calculate the ratio per (region,product,week) not (region, product, year, week)
    //기말고사 QUERY 문제
    var  finalResult = spark.sql(
    "WITH ANSWER AS"+
    "("+
    "SELECT DISTINCT REGIONID,"+
    "PRODUCT,"+
    "SUBSTR(YEARWEEK,5,6) AS WEEK,"+
    "RATIO"+
    "FROM FINAL_2019ST_HYOKWAN"+
    ")"+
    "SELECT DISTINCT REGIONID,"+
    "PRODUCT"+
    ",WEEK"+
    ",AVG(RATIO)"+
    "FROM ANSWER A"+
    "GROUP BY REGIONID,PRODUCT,WEEK"
    )
    // output: (regionid, product, week, avg_ratio)
    // !!!!!!!!! Blanks Start
    //기말고사 RDD로 푼문제
    var rawDataColumns1 = middleResult.columns
    var resultregionNO = rawDataColumns1.indexOf("REGIONID")
    var resultproductNo = rawDataColumns1.indexOf("PRODUCT")
    var resultyearweekNo = rawDataColumns1.indexOf("YEARWEEK")
    var resultratioNo = rawDataColumns1.indexOf("RATIO")

    var resultFilterRdd = middleResult.rdd

    var finalAnswer = resultFilterRdd.groupBy(x=>{
      (x.getString(resultregionNO), x.getString(resultproductNo), x.getString(resultyearweekNo).substring(4,6).toInt)}).flatMap(x=>{
      var key = x._1
      var data = x._2
      var size = data.size
      var avg_ratio = 0.0d
      var sumation = data.map(x=>{x.getDouble(resultratioNo)}).sum
      if (size != 0){
        avg_ratio = sumation/size
      }
      var outputData = data.map(x=>{(
        x.getString(resultregionNO),
        x.getString(resultproductNo),
        x.getString(resultyearweekNo).substring(4,6).toInt,
        avg_ratio)
      })
      outputData
    })
    // !!!!!!!!! Blanks End

    var realFinalAnswer = finalAnswer.distinct()



    //////////////////////////////////////////////////////////////////////////////////////////////////
    // 9. Data unloading (memory -> oracle)
    //////////////////////////////////////////////////////////////////////////////////////////////////
    var outputUrl = "jdbc:oracle:thin:@192.168.110.24:1522/XE"
    var outputUser = "JHS"
    var outputPw = "JHS"

    val prop = new java.util.Properties
    prop.setProperty("driver", "oracle.jdbc.OracleDriver")
    prop.setProperty("user", outputUser)
    prop.setProperty("password", outputPw)
    val table = "FINAL_2019ST_HYUNGSUK"

    //////////////////////////////////////////////////////////////////////////////////////////////////
    // 10. Exam #4 Save your final result in oracle (FINAL_2019ST_NAME) in english
    var finalRddResult = realFinalAnswer.toDF("REGIONID","PRODUCT","WEEK","AVG_RATIO")
    println(finalRddResult.show)
    //////////////////////////////////////////////////////////////////////////////////////////////////
    finalRddResult.write.mode("overwrite").jdbc(outputUrl, table, prop)
    println("finished")
  }
}

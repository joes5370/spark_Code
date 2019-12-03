package com.haiteam

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object 문제원형 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().
      setAppName("DataLoading").
      setMaster("local[*]")
    var sc = new SparkContext(conf)
    val spark = new SQLContext(sc)

    import spark.implicits._
    import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode}
    import org.apache.spark.sql.types.{StringType, StructField, StructType}

    var salesFile = "pro_actual_sales.csv"
    // 절대경로 입력
    var salesDf=
      spark.read.format("csv").
        option("header","true").
        option("Delimiter",",").
        load("C:/spark_orgin_2.2.0/bin/data/"+salesFile)

    // 데이터 확인 (3)
    print(salesDf.show(2))

    var salesColums = salesDf.columns.map(x=>{x.toLowerCase()})
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

    var promotionDf=
      spark.read.format("csv").
        option("header","true").
        option("Delimiter",",").
        load("C:/spark_orgin_2.2.0/bin/data/"+promotionFile)

    print(promotionDf.show(2))

    var promotionColums = promotionDf.columns.map(x=>{x.toLowerCase()})
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


    //두개 RDD
    var salesRdd = salesDf.rdd
    var promotionRdd = promotionDf.rdd
    var minPlanWeek = promotionRdd.map(x=>{x.getString(planweekno).toInt}).min()


    var filteredPromotion = promotionRdd.filter(x=>{
      var check = false
      var targetWeek = x.getString(targetweekno3)
      var map_price = x.getString(map_priceno)
      if(targetWeek.toInt > minPlanWeek && map_price.toInt != 0){
        check = true
      }
      check
    })

//    var filteredPromotion = promotionRdd.filter(x=>{
//      var check = false
//      var targetWeek = x.getString(targetweekno3)
//      var map_price = x.getString(map_priceno)
//      if(targetWeek.toInt > minPlanWeek){
//        check = true
//      }
//      check
//    })

//    var maxTargetWeek = filteredPromotion.map(x=>{x.getString(targetweekno3).toInt}).min()

    var filterMap = filteredPromotion.map(x=>{
      (x.getString(regionidno),
        x.getString(salesidno),
        x.getString(productgroup2),
        x.getString(itemno),
        x.getString(targetweekno3),
        x.getString(planweekno),
        x.getString(map_priceno),
        x.getString(irno),
        x.getString(pmapno),
        x.getString(pmap10no),
        x.getString(pro_percentno))
    })


    var testDf = filterMap.toDF("REGIONSEG","SALESID","PRODUCTGROUP","ITEM","TARGETWEEK","PLANWEEK","MAP_PRICE","IR","PMAP","PMAP10","PRO_PERCENT")

    testDf.createOrReplaceTempView("filterPromotion")

    salesDf.createOrReplaceTempView("salesData")

    var leftJoinData = spark.sql("""SELECT A.*,B.MAP_PRICE,B.IR,B.PMAP,B.PMAP10,B.PRO_PERCENT FROM salesData A left join filterPromotion B ON A.regionseg1 = B.REGIONSEG AND A.productseg2 = B.PRODUCTGROUP AND A.regionseg2 = B.SALESID AND A.productseg3 = ITEM and A.yearweek = B.TARGETWEEK""")

    var innerJoinData = spark.sql("""SELECT A.*,B.MAP_PRICE,B.IR,B.PMAP,B.PMAP10,B.PRO_PERCENT FROM salesData A inner join filterPromotion B ON A.regionseg1 = B.REGIONSEG AND A.productseg2 = B.PRODUCTGROUP AND A.regionseg2 = B.SALESID AND A.productseg3 = ITEM and A.yearweek = B.TARGETWEEK""")

    leftJoinData.
      coalesce(1). // 파일개수
      write.format("csv").  // 저장포맷
      mode("overwrite"). // 저장모드 append/overwrite
      option("header", "true"). // 헤더 유/무
      save("c:/spark/bin/data/pro_actual_sales_season.csv") // 저장파일명

    innerJoinData.
      coalesce(1). // 파일개수
      write.format("csv").  // 저장포맷
      mode("overwrite"). // 저장모드 append/overwrite
      option("header", "true"). // 헤더 유/무
      save("c:/spark/bin/data/innerJoin.csv") // 저장파일명


    //계절성 지수 뽑기?
    var leftJoinColums = leftJoinData.columns.map(x=>{x.toLowerCase()})
    var regionidno1 = leftJoinColums.indexOf("regionseg1")
    var productno1 = leftJoinColums.indexOf("productseg1")
    var productno2 = leftJoinColums.indexOf("productseg2")
    var regionidno2 = leftJoinColums.indexOf("regionseg2")
    var regionidno3 = leftJoinColums.indexOf("regionseg3")
    var productno3 = leftJoinColums.indexOf("productseg3")
    var yearweekno = leftJoinColums.indexOf("yearweek")
    var yearno = leftJoinColums.indexOf("year")
    var weekno = leftJoinColums.indexOf("week")
    var qtyno = leftJoinColums.indexOf("qty")
    var map_priceno = leftJoinColums.indexOf("map_price")
    var irno = leftJoinColums.indexOf("ir")
    var pmapno = leftJoinColums.indexOf("pmap")
    var pmap10no = leftJoinColums.indexOf("pmap10")
    var pro_percentno = leftJoinColums.indexOf("pro_percent")


    var leftJoinRdd = leftJoinData.rdd















  }
}

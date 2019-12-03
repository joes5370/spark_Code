package com.haiteam

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.avg



object 문제원형_0703 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().
      setAppName("DataLoading").
      setMaster("local[*]")
    var sc = new SparkContext(conf)
    val spark = new SQLContext(sc)
    import spark.implicits._

    var testFile = "test.csv"
    // 절대경로 입력
    var AllDf =
      spark.read.format("csv").
        option("header", "true").
        option("Delimiter", ",").
        load("C:/spark_orgin_2.2.0/bin/data/" + testFile)

//    val moving = Window.partitionBy("QTY").orderBy("YEARWEEK").rowsBetween(-2,2)

    //5일 기준으로 이동평균 qty구하기
    var refinedproductData = AllDf.withColumn("QTY",$"QTY".cast("Double")).sort("REGIONSEG1","PRODUCTSEG2","REGIONSEG2","REGIONSEG3","PRODUCTSEG3","YEARWEEK")
    var movingAvg = refinedproductData.withColumn("MOVING_QTY",avg(refinedproductData("QTY")).over(Window.partitionBy("REGIONSEG1","REGIONSEG3","PRODUCTSEG2").rowsBetween(-2,2)))

    //이동평균 구한 데이터 재정의

    var sortAllDf = movingAvg.toDF("REGIONSEG1", "PRODUCTSEG2", "REGIONSEG2", "REGIONSEG3", "PRODUCTSEG3", "YEARWEEK", "YEAR", "WEEK", "QTY", "MAP_PRICE", "IR", "PMAP", "PMAP10", "PRO_PERCENT","PROMOTION_CHECK","MOVING_QTY")

    sortAllDf.
      coalesce(1). // 파일개수
      write.format("csv"). // 저장포맷
      mode("overwrite"). // 저장모드 append/overwrite
      option("header", "true"). // 헤더 유/무
      save("c:/spark/bin/data/movingResult.csv") // 저장파일명

    var movingData = "movingResultData.csv"
    // 절대경로 입력
    var movingDf =
      spark.read.format("csv").
        option("header", "true").
        option("Delimiter", ",").
        load("C:/spark_orgin_2.2.0/bin/data/" + movingData)

    var movingColums = movingDf.columns.map(x => {x.toLowerCase()})
    var regionidno = movingColums.indexOf("regionseg1")
    var productno2 = movingColums.indexOf("productseg2")
    var regionidno2 = movingColums.indexOf("regionseg2")
    var regionidno3 = movingColums.indexOf("regionseg3")
    var productno3 = movingColums.indexOf("productseg3")
    var yearweekno = movingColums.indexOf("yearweek")
    var yearno = movingColums.indexOf("year")
    var weekno = movingColums.indexOf("week")
    var qtyno = movingColums.indexOf("qty")
    var map_priceno = movingColums.indexOf("map_price")
    var irno = movingColums.indexOf("ir")
    var pmapno = movingColums.indexOf("pmap")
    var pmap10no = movingColums.indexOf("pmap10")
    var pro_percentno = movingColums.indexOf("pro_percent")
    var promotionCheck = movingColums.indexOf("promotion_check")
    var movingQty = movingColums.indexOf("moving_qty")

    var movingRdd = movingDf.rdd

    //계절성 지수 구해서 전체 컬럼 재정의
//    (x.getString(regionidno),x.getString(productno2),x.getString(regionidno2)
//      ,x.getString(productno3))
    var seasonalityRdd = movingRdd.groupBy(x=>{
      (x.getString(regionidno),x.getString(productno2),x.getString(regionidno3)
      )
    }).flatMap(x=>{
      var key = x._1
      var data = x._2

      var finalData = data.map(x=>{
        var ratio = 1.0d
        var each_movingqty = x.getString(movingQty).toDouble

        if(each_movingqty != 0){
          ratio = x.getString(qtyno).toDouble / each_movingqty
        }else{
          ratio
        }
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
          x.getString(movingQty),
          ratio.toString)
      })
      finalData
    })

    //productSeg2,regionidSeg3,productSeg3,yearweek기준 오름차순 정렬
    var sortedData = seasonalityRdd.sortBy(x=>(x._2,x._4,x._5,x._6))
    var sortDf = sortedData.toDF("REGIONSEG1", "PRODUCTSEG2", "REGIONSEG2", "REGIONSEG3", "PRODUCTSEG3", "YEARWEEK", "YEAR", "WEEK", "QTY", "MAP_PRICE", "IR", "PMAP", "PMAP10", "PRO_PERCENT","PROMOTION_CHECK","MOVING_AVG","SEASONALITY")

//    sortDf.
//      coalesce(1). // 파일개수
//      write.format("csv"). // 저장포맷
//      mode("overwrite"). // 저장모드 append/overwrite
//      option("header", "true"). // 헤더 유/무
//      save("c:/spark/bin/data/modifySeasonalityData.csv") // 저장파일명

    var dtInput = sortDf.withColumn("MOVING_AVG",$"MOVING_AVG".cast("Double")).
      withColumn("PRO_PERCENT",$"PRO_PERCENT".cast("Double")).
      withColumn("PROMOTION_CHECK",$"PROMOTION_CHECK".cast("Double")).
      withColumn("SEASONALITY",$"SEASONALITY".cast("Double"))

    import org.apache.spark.ml.Pipeline
    import org.apache.spark.ml.evaluation.RegressionEvaluator
    import org.apache.spark.ml.feature.VectorAssembler
    import org.apache.spark.ml.regression.{DecisionTreeRegressionModel, DecisionTreeRegressor}
    import org.apache.spark.ml.feature.{StringIndexer}

    //기간설정
    var targetWeek = "201624"
    var trainData = dtInput.filter($"YEARWEEK" <= targetWeek)
    var testData = dtInput.filter($"YEARWEEK" > targetWeek)
    //    var promotionIndexer = new StringIndexer().setInputCol("PROMOTION_CHECK").setOutputCol("PROMOTION_CHECK_IN")

    //훈련
    var assembler = new  VectorAssembler().setInputCols(Array("PROMOTION_CHECK","PRO_PERCENT","SEASONALITY")).setOutputCol("FEATURES")

    var dt = new DecisionTreeRegressor().setLabelCol("MOVING_AVG").setFeaturesCol("FEATURES")

    var pipeline = new Pipeline().setStages(Array(assembler,dt))

    var model = pipeline.fit(trainData)

    val predictions = model.transform(testData)

    predictions.select("REGIONSEG1","PRODUCTSEG2","REGIONSEG2","REGIONSEG3","PRODUCTSEG3","YEARWEEK","MOVING_AVG","FEATURES","PREDICTION").orderBy("YEARWEEK").show


  }
}

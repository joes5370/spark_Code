package com.haiteam

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object 문제원형_0705 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().
      setAppName("DataLoading").
      setMaster("local[*]")
    var sc = new SparkContext(conf)
    val spark = new SQLContext(sc)
    import spark.implicits._

    var testFile = "modifySeasonalityData.csv"
    // 절대경로 입력
    var AllDf =
      spark.read.format("csv").
        option("header", "true").
        option("Delimiter", ",").
        load("C:/spark_orgin_2.2.0/bin/data/" + testFile)

    var movingColums = AllDf.columns.map(x => {x.toLowerCase()})
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
    var movingQty = movingColums.indexOf("moving_avg")
    var seasonality = movingColums.indexOf("seasonality")

    var AllRdd = AllDf.rdd

    var refineRdd = AllRdd.map(x=>{
      var promotionCheckTest = x.getString(promotionCheck)
      if(promotionCheckTest == "N"){
        promotionCheckTest = "0"
      }else{
        promotionCheckTest = "1"
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
        promotionCheckTest,
        x.getString(movingQty),
        x.getString(seasonality)
      )

    })


    var sortedData = refineRdd.sortBy(x=>(x._1,x._2,x._3,x._4,x._5))
    var sortDf = sortedData.toDF("REGIONSEG1", "PRODUCTSEG2", "REGIONSEG2", "REGIONSEG3", "PRODUCTSEG3", "YEARWEEK", "YEAR", "WEEK", "QTY", "MAP_PRICE", "IR", "PMAP", "PMAP10", "PRO_PERCENT","PROMOTION_CHECK","MOVING_AVG","SEASONALITY")

//    sortDf.
//      coalesce(1). // 파일개수
//      write.format("csv"). // 저장포맷
//      mode("overwrite"). // 저장모드 append/overwrite
//      option("header", "true"). // 헤더 유/무
//      save("c:/spark/bin/data/aaaa.csv") // 저장파일명


    var testFile = "final_data.csv"
    // 절대경로 입력
    var AllDf =
      spark.read.format("csv").
        option("header", "true").
        option("Delimiter", ",").
        load("C:/spark_orgin_2.2.0/bin/data/" + testFile)

    var sortedDf = AllDf.toDF("REGIONSEG1", "REGIONSEG3", "SALESID", "PRODUCTGROUP", "ITEM", "YEARWEEK", "YEAR", "WEEK","MAP_PRICE", "IR", "PMAP", "PMAP10", "PRO_PERCENT","QTY", "PROMOTIONNY","MOVING_AVG","SEASONALITY","FCST","TIME_SERIES")


    import org.apache.spark.ml.Pipeline
    import org.apache.spark.ml.evaluation.RegressionEvaluator
    import org.apache.spark.ml.feature.VectorAssembler
    import org.apache.spark.ml.regression.{DecisionTreeRegressionModel, DecisionTreeRegressor}
    import org.apache.spark.ml.feature.{StringIndexer}
    //머신러닝
    var dtInput = sortedDf.withColumn("MOVING_AVG",$"MOVING_AVG".cast("Double")).
      withColumn("PRO_PERCENT",$"PRO_PERCENT".cast("Double")).
      withColumn("PROMOTIONNY",$"PROMOTIONNY".cast("Int")).
      withColumn("SEASONALITY",$"SEASONALITY".cast("Double")).
      withColumn("QTY",$"QTY".cast("Double")).withColumn("FCST",$"FCST".cast("Double"))

    //기간설정
    var targetWeek = "201627"
    var trainData = dtInput.filter($"YEARWEEK" <= targetWeek)
    var testData = dtInput.filter($"YEARWEEK" > targetWeek)

    //    var promotionIndexer = new StringIndexer().setInputCol("PROMOTION_CHECK").setOutputCol("PROMOTION_CHECK_IN")

    //훈련
    var assembler = new  VectorAssembler().setInputCols(Array("PROMOTIONNY","PRO_PERCENT","SEASONALITY")).setOutputCol("FEATURES")

    var dt = new DecisionTreeRegressor().setLabelCol("QTY").setFeaturesCol("FEATURES")
    var pipeline = new Pipeline().setStages(Array(assembler,dt))

    var model = pipeline.fit(trainData)

    val predictions = model.transform(testData)

    predictions.select("REGIONSEG1", "REGIONSEG3", "SALESID", "PRODUCTGROUP", "ITEM", "YEARWEEK", "YEAR", "WEEK","MAP_PRICE", "IR", "PMAP", "PMAP10", "PRO_PERCENT","QTY", "PROMOTIONNY","MOVING_AVG","SEASONALITY","FCST","TIME_SERIES","FEATURES","PREDICTION").show

    var resultOut =predictions.withColumn("MOVING_AVG",$"MOVING_AVG".cast("String")).
      withColumn("PRO_PERCENT",$"PRO_PERCENT".cast("String")).
      withColumn("PROMOTIONNY",$"PROMOTIONNY".cast("String")).
      withColumn("SEASONALITY",$"SEASONALITY".cast("String")).
      withColumn("QTY",$"QTY".cast("String")).withColumn("FCST",$"FCST".cast("String")).withColumn("FEATURES",$"FEATURES".cast("String"))

    resultOut.dtypes

    resultOut.
      coalesce(1). // 파일개수
      write.format("csv"). // 저장포맷
      mode("overwrite"). // 저장모드 append/overwrite
      option("header", "true"). // 헤더 유/무
      save("c:/spark/bin/data/result.csv") // 저장파일명


    val evaluatorRmse = new RegressionEvaluator().setLabelCol("QTY").setPredictionCol("prediction").setMetricName("rmse")

    val evauatorMae = new RegressionEvaluator().setLabelCol("QTY").setPredictionCol("prediction").setMetricName("mae")

    val rmse = evaluatorRmse.evaluate(predictions)
    val mae = evauatorMae.evaluate(predictions)


    //비지도학습

    var kmeansInput =sortedDf.withColumn("MOVING_AVG",$"MOVING_AVG".cast("Double")).
      withColumn("PRO_PERCENT",$"PRO_PERCENT".cast("Double")).
      withColumn("PROMOTIONNY",$"PROMOTIONNY".cast("Int")).
      withColumn("SEASONALITY",$"SEASONALITY".cast("Double")).
      withColumn("QTY",$"QTY".cast("Double")).withColumn("FCST",$"FCST".cast("Double"))
    kmeansInput.dtypes

    //k-means Clustering 라이브러리 추가
    import org.apache.spark.ml.feature.VectorAssembler
    import org.apache.spark.ml.Pipeline
    import org.apache.spark.ml.clustering.KMeans

    //훈련
    var assembler = new VectorAssembler().setInputCols(Array("FCST","PRO_PERCENT","PROMOTIONNY","SEASONALITY")).setOutputCol("FEATURES")
    var kValue = 2
    val kmeans = new KMeans().setK(kValue).setFeaturesCol("FEATURES").setPredictionCol("PREDICTION")
    val pipeline = new Pipeline().setStages(Array(assembler,kmeans))
    val kMeansPredictionModel = pipeline.fit(kmeansInput)

    //예측
    val predictionResult = kMeansPredictionModel.transform(kmeansInput)
    print(predictionResult.show(10))

    var beresultOut =predictionResult.withColumn("MOVING_AVG",$"MOVING_AVG".cast("String")).
      withColumn("PRO_PERCENT",$"PRO_PERCENT".cast("String")).
      withColumn("PROMOTIONNY",$"PROMOTIONNY".cast("String")).
      withColumn("SEASONALITY",$"SEASONALITY".cast("String")).
      withColumn("QTY",$"QTY".cast("String")).withColumn("FCST",$"FCST".cast("String")).withColumn("FEATURES",$"FEATURES".cast("String"))

    beresultOut.dtypes

    beresultOut.
      coalesce(1). // 파일개수
      write.format("csv"). // 저장포맷
      mode("overwrite"). // 저장모드 append/overwrite
      option("header", "true"). // 헤더 유/무
      save("c:/spark/bin/data/result.csv") // 저장파일명

  }
}

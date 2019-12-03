package com.haiteam

import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.{SparkConf, SparkContext}

object SummerVacationBook {
  def main(args: Array[String]): Unit = {
      def discountRatio(price:Double, rate:Double):Double = {
        var discount = price * rate
        var result = price - discount
        result
      }
    val conf = new SparkConf().setAppName("DataLoading").setMaster("local[*]")
    var sc = new SparkContext(conf)
    val spark = new SQLContext(sc)
    import spark.implicits

    var paramPath = "C:/spark_orgin_2.2.0/bin/data/"
    var paramFile = "KOPO_BATCH_SEASON_MPARA.txt"
    var paramData = spark.read.format("csv").option("header", "true").option("Delimiter",";").load(paramPath+paramFile)

    paramData.show()

    var staticUrl = "jdbc:oracle:thin:@192.168.110.112:1521/orcl"
    var staticUser = "kopo"
    var staticPw = "kopo"
    var selloutDb = "kopo_product_volume"

    var oracleSellouDb = spark.read.format("jdbc").option("url",staticUrl).option("dbtable",selloutDb).option("user",staticUser).option("password",staticPw).load()
    oracleSellouDb.show

    var masterPath = "C:/spark_orgin_2.2.0/bin/data/"
    var materFile = "KOPO_REGION_MASTER.csv"
    var masterData = spark.read.format("csv").option("header", "true").option("Delimiter",",").load(masterPath+materFile)

    var selloutFile = "kopo_product_volume.csv"
    var selloutData = spark.read.format("csv").option("header","true").option("Delimiter",",").load("C:/spark_orgin_2.2.0/bin/data/"+selloutFile)

    var filteredData = selloutData.filter($"YEARWEEK" > "201520" && $"PRODUCTGROUP" === "ST0002")

    var selectData = selloutData.select("REGIONID","PRODUCTGROUP","VOLUME")

    var missingValueFile = "missingValue.csv"

    var missingData = spark.read.format("csv").option("header","true").option("Delimiter",",").load("C:/spark_orgin_2.2.0/bin/data/"+missingValueFile)

    var missingFiltered = missingData.filter(row=>{row.anyNull})

    var insertValueNull = missingFiltered.na.fill("0")

    var specialFiltered = missingData.filter(($"VOLUME".isNull) || ($"TARGET".isNull))

    var targetColume = Array("VOLUME")
    var changeNull = specialFiltered.na.fill("0",targetColume)

    var promotionDataFile = "promotionData.csv"

    var promotionData = spark.read.format("csv").option("header","true").option("Delimiter",",").load("C:/spark_orgin_2.2.0/bin/data/"+promotionDataFile)

    var changeTypes = promotionData.withColumn("PRICE", $"PRICE".cast("Double")).withColumn("DISCOUNT",$"DISCOUNT".cast("DOUBLE"))

    import org.apache.spark.sql.functions._

    var promotionFinal = promotionData.withColumn("NEW_DISCOUNT",when($"DISCOUNT" > $"PRICE" , 0).otherwise($"DISCOUNT"))

    var innerJoin = selloutData.join(promotionData,Seq("REGIONID","PRODUCTGROUP","YEARWEEK"),joinType = "inner")

    var leftJoin = selloutData.join(promotionData,Seq("REGIONID","PRODUCTGROUP","YEARWEEK"),joinType = "left_outer")

    var refineSelloutData = selloutData.withColumn("VOLUME",$"VOLUME".cast("DOUBLE")).sort("REGIONID","PRODUCTGROUP","YEARWEEK")

    var movingAvgResult = refineSelloutData.withColumn("MV_AVG", avg(refineSelloutData("VOLUME")).over(Window.partitionBy("REGIONID").rowsBetween(-1,1)))

    var fileFath = "C:/spark_orgin_2.2.0/bin/data/"
    var fileName = "decisionTreeResult.csv"

    def uploadFile(Path:String,Name:String): org.apache.spark.sql.DataFrame ={
      var outDataFrame = spark.read.format("csv").option("header","true").option("Delimiter",",").load(Path + Name)
      outDataFrame
    }

    var dtResultData = uploadFile(fileFath,fileName)

    var refinedData = dtResultData.withColumn("SALES",$"SALES".cast("DOUBLE")).sort("PRODUCT","ITEM","YEARWEEK")

    var pivotDf2 = refinedData.groupBy("PRODUCT","ITEM","YEARWEEK").pivot("MEASURE",Seq("REAL_QTY","PREDICTION_QTY")).sum("SALES")

    var testArray = Array(10d,20d,30d,40d)
    var ArrayRdd = sc.parallelize(testArray)

    var selloutRdd = selloutData.rdd

    var name = "filterRddSample.csv"

    var filterRddSampleData = uploadFile(fileFath,name)

    var missingValueColums = filterRddSampleData.columns

    var regionidNo = missingValueColums.indexOf("REGIONID")

    var productgroupNo = missingValueColums.indexOf("PRODUCTGROUP")

    var yearweekNo = missingValueColums.indexOf("YEARWEEK")

    var volumeNo = missingValueColums.indexOf("VOLUME")

    var targetNo = missingValueColums.indexOf("TARGET")

    var filteredRdd = filterRddSampleData.rdd

    var yearweek_size = 6

    var sampleRdd = filteredRdd.filter(x=>{
      var checkVaild = true
      if(x.getString(yearweekNo).size != yearweek_size){
        checkVaild = false
      }
      checkVaild
    })

    //특정행 디버깅깅
   var x = filteredRdd.filter(x=>{
      (x.getString(volumeNo) == null)
    }).first

    var checkNull = filteredRdd.filter(x=>{
      var size = x.size-2
      var checkValid = true
      if(x.isNullAt(size) == true){
        checkValid = false
      }
      checkValid
    })

    var forcheck = filteredRdd.filter(row=>{
      var size = row.size
      var check = true

      for(i <-0 until size){
        if(row.isNullAt(i) == true){
          check = false
        }
      }
      check
    })

    var max_value = 700000

    var selloutColums = selloutData.columns
    var regionidNo = selloutColums.indexOf("REGIONID")
    var productgroupNo = selloutColums.indexOf("PRODUCTGROUP")
    var yearweekNo = selloutColums.indexOf("YEARWEEK")
    var volumeNo = selloutColums.indexOf("VOLUME")

    var mappingRdd = selloutRdd.map(x=>{
      var volume = x.getString(volumeNo).toDouble
      if(volume >= max_value){
        volume = max_value
      }
      Row(
       x.getString(regionidNo),x.getString(productgroupNo),x.getString(yearweekNo),volume
      )
    })

    var groupRdd = selloutRdd.groupBy(x=>{
      (x.getString(regionidNo),x.getString(productgroupNo))
    }).map(x=>{
      var key = x._1
      var data = x._2
      var size = data.size
      var sum = data.map(x=>{x.getString(volumeNo).toDouble}).sum
      var avg = 0d
      if(size != 0){
        avg = sum/size
      }
      (key,avg)
    })

    var flatMapGroupData = selloutRdd.groupBy(x=>{
      (x.getString(regionidNo),x.getString(productgroupNo))
    }).flatMap(x=>{
      var key = x._1
      var data = x._2
      var size = data.size
      var summation = data.map(x=>{x.getString(volumeNo).toDouble}).sum
      var avg = 0d
      if( size != 0 ){
        avg = summation / size
      }
      var finalData = data.map(x=>{
        (x.getString(regionidNo),
          x.getString(productgroupNo),
          x.getString(yearweekNo),
          x.getString(volumeNo),
          avg.toString)
      })
      finalData
    })

    var resultDf = flatMapGroupData.toDF("REGIONID","PRODCUTGROUP","YEARWEEK","VOLUME","AVG")

    var groupMapFuction = selloutRdd.groupBy(x=>{
      (x.getString(regionidNo),x.getString(productgroupNo))
    }).map(x=>{
      var key = x._1
      var data = x._2
      var size = data.size
      var summation = data.map(x=>{x.getString(volumeNo).toDouble}).sum
      var avg = 0d
      if( size != 0 ){
        avg = summation / size
      }
      (key,(size,avg))
    }).collectAsMap

    var path = "C:/spark_orgin_2.2.0/bin/data/"
    var fileName = "kopo_product_sellout.csv"

    def loadCsv(path:String, name:String):org.apache.spark.sql.DataFrame={
      var outData = spark.read.format("csv").option("header","true").option("Delimiter",",").load(path+name)
      outData
    }

    var sampleData = loadCsv(path,fileName)

    sampleData.createOrReplaceTempView("SELLOUT_VIEW")

    var Week = "53"
    var minVolume = 0
    var maxVolume = 700000
    var sqlQuery = """SELECT REGIONID,PRODUCT,YEARWEEK,CAST(CASE WHEN QTY < """+minVolume+""" THEN 0 WHEN QTY >"""+maxVolume+""" THEN 700000 ELSE QTY END AS DOUBLE) AS QTY FROM SELLOUT_VIEW WHERE 1=1 AND SUBSTR(YEARWEEK,5,6) !="""+Week
    var selloutDf = spark.sql(sqlQuery)

    var sampleDfColumns = selloutDf.columns.map(x=>{x.toLowerCase()})
    var regionidNo = sampleDfColumns.indexOf("regionid")
    var productNo = sampleDfColumns.indexOf("product")
    var yearweekNo = sampleDfColumns.indexOf("yearweek")
    var qtyNo = sampleDfColumns.indexOf("qty")

    var seasonRdd = selloutDf.rdd.groupBy(x=>{
      (x.getString(regionidNo),x.getString(productNo))
    }).flatMap(x=>{
      var key = x._1
      var data = x._2

      var sum_qty = data.map(x=>{x.getDouble(qtyNo)}).sum
      var size = data.size
      var avg = Math.round(sum_qty/size)

      var finalData = data.map(x=>{
        var ratio = 1.0d
        var each_qty = x.getDouble(qtyNo)
        ratio = each_qty / avg
        ratio = Math.round(ratio * 100.0) / 100.0d
        (x.getString(regionidNo),
          x.getString(productNo),
          x.getString(yearweekNo),
          x.getDouble(qtyNo),
          avg,
          ratio.toDouble)
      })
      finalData
    })

    var yearweekResult = seasonRdd.toDF("REGIONID","PRODUCT","YEARWEEK","QTY","AVG_QTY","RATIO")

    yearweekResult.createOrReplaceTempView("MIDDLE_VIEW")

    var seasonalityQuery = """SELECT REGIONID,PRODUCT,SUBSTRING(YEARWEEK,5,2) AS WEEK, ROUND(AVG(RATIO),2) AS RATIO FROM MIDDLE_VIEW GROUP BY REGIONID, PRODUCT, SUBSTRING(YEARWEEK,5,2)"""

    var seasonalityDf = spark.sql(seasonalityQuery)


    var path = "C:/spark_orgin_2.2.0/bin/data/"
    var fileName = "kopo_decisiontree_input.csv"

    var muchineData = loadCsv(path,fileName)

    var dtInput = muchineData.withColumn("QTY", $"QTY".cast("Double")).withColumn("PRO_PERCENT",$"PRO_PERCENT".cast("Double"))

    dtInput.dtypes.foreach(println)

    import org.apache.spark.ml.Pipeline
    import org.apache.spark.ml.evaluation.RegressionEvaluator
    import org.apache.spark.ml.feature.VectorAssembler
    import org.apache.spark.ml.regression.{DecisionTreeRegressionModel, DecisionTreeRegressor}
    import org.apache.spark.ml.feature.{StringIndexer}

    val holidayIndexer = new StringIndexer().setInputCol("HOLIDAY").setOutputCol("HOLIDAY_IN")

    val promotionIndexer = new StringIndexer().setInputCol("PROMOTION").setOutputCol("PROMOTION_IN")

    var targetYearweek = 201630

    val trainData = dtInput.filter($"YEARWEEK" <= targetYearweek)

    val testData = dtInput.filter($"YEARWEEK" > targetYearweek)

    val assembler = new VectorAssembler().setInputCols(Array("HOLIDAY_IN","PROMOTION_IN","PRO_PERCENT")).setOutputCol("FEATURES")

    var dt = new DecisionTreeRegressor().setLabelCol("QTY").setFeaturesCol("FEATURES")

    val pipeline = new Pipeline().setStages(Array(holidayIndexer,promotionIndexer,assembler,dt))

    val model = pipeline.fit(trainData)

    val predictions = model.transform(testData)

    predictions.select("REGIONID","PRODUCT","ITEM","YEARWEEK","QTY","FEATURES","PREDICTION").orderBy("YEARWEEK").show

    val evaluatorRmse = new RegressionEvaluator().setLabelCol("QTY").setPredictionCol("prediction").setMetricName("rmse")

    val evaluatorMae = new RegressionEvaluator().setLabelCol("QTY").setPredictionCol("prediction").setMetricName("mae")

    val rmse = evaluatorRmse.evaluate(predictions)

    val mae = evaluatorMae.evaluate(predictions)

    var fileName = "student_middle.csv"

    var fileLoading = loadCsv(path,fileName)

    var kmeansInput = fileLoading.withColumn("SW",$"SW".cast("Double")).withColumn("DB",$"DB".cast("Double")).withColumn("AND",$"AND".cast("Double"))

    import org.apache.spark.ml.feature.VectorAssembler
    import org.apache.spark.ml.Pipeline
    import org.apache.spark.ml.clustering.KMeans

    var assembler = new VectorAssembler().setInputCols(Array("SW","DB","AND")).setOutputCol("FEATURES")

    var kValue = 2

    val kmeans = new KMeans().setK(kValue).setFeaturesCol("FEATURES").setPredictionCol("PREDICTION")

    val pipelineKmeans = new Pipeline().setStages(Array(assembler,kmeans))

    val kMeansPredictionModel = pipelineKmeans.fit(kmeansInput)

    val predictionResult = kMeansPredictionModel.transform(kmeansInput)









  }
}

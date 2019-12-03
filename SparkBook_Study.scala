package com.haiteam

import org.apache.spark.sql.catalyst.analysis.TypeCoercion.WindowFrameCoercion
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object SparkBook_Study {
  def main(args: Array[String]): Unit = {
    //spark 세션생성
    val conf = new SparkConf().setAppName("DataLoading").setMaster("local[*]")
    var sc = new SparkContext(conf)
    val spark = new SQLContext(sc)
    import spark.implicits._

    //파일데이터 불러오기
    var paramFile = "KOPO_BATCH_SEASON_MPARA.txt"

    var paramData =
      spark.read.format("csv").
        option("header", "true").
        option("Delimiter", ";").
        load("c:/spark_orgin_2.2.0/bin/data/" + paramFile)

    print(paramData.show())

    //오라클 데이터 불러오기 , 다른 DB는 교수님 git참조하기
    var staticUrl = "jdbc:oracle:thin:@192.168.110.112:1521/orcl"
    var staticUser = "kopo"
    var staticPw = "kopo"
    var selloutDb = "kopo_product_volume"

    val dataFromOracle = spark.read.format("jdbc").
      option("url", staticUrl).
      option("dbtable", selloutDb).
      option("user", staticUser).
      option("password", staticPw).load()

    print(dataFromOracle.show(5))

    //하둡플랫폼 데이터 불러오기
    var hadoopData = "KOPO_BATCH_SEASON_MPARA.txt"
    val hdfsData = spark.read.option("header","true").format("csv").
      load("hdfs://192.168.0.30:9000/kopo/test.csv")

    print(hdfsData)

    //파일데이터 저장하기
    paramData.coalesce(1).write.format("csv").mode("overwrite").option("header","true").
      save("c:/spark/bin/kopo_test.txt")

    ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////


    //SparkSQL
    var selloutFile = "KOPO_PRODUCT_VOLUME.csv"
    var selloutData = spark.read.format("csv").option("header","true").option("Delimiter",",").load("c:/spark_orgin_2.2.0/bin/data/"+selloutFile)

    selloutData.createOrReplaceTempView("maindata")

    print(selloutData.schema)

    var sql = "select regionid, productgroup,yearweek, cast(volume as double) from maindata"
    var sqlCleansingData = spark.sql(sql)

    var practiceFile = "KOPO_PRODUCT_VOLUME_JOIN.csv"

    var practiceData = spark.read.format("csv").option("header","true").option("Delimiter",",").load("c:/spark_orgin_2.2.0/bin/data/"+practiceFile)

    practiceData.createOrReplaceTempView("sellout_view")

    var refinedDataExample = spark.sql("""select concat(regionid,product) as key,regionid,product,yearweek,qty from sellout_view where 1=1 and qty> 1000""")
    var orderedData = spark.sql("""select concat(regionid,product) as key,regionid,product,yearweek,qty from sellout_view where 1=1 and qty> 1000 order by regionid, product desc, yearweek""")


    var regionMasterFile = "KOPO_REGION_MASTER.csv"
    var regionMasterData = spark.read.format("csv").option("header","true").option("Delimiter",",").load("c:/spark_orgin_2.2.0/bin/data/"+regionMasterFile)

    practiceData.createOrReplaceTempView("selloutTable")
    regionMasterData.createOrReplaceTempView("masterTable")

    var innerJoin = spark.sql("""select b.regionname,a.regionid,a.product,a.yearweek,a.qty from selloutTable a inner join masterTable b on a.regionid=b.regionid""".stripMargin)

    var leftJoin = spark.sql("""select b.regionname,a.regionid,a.product,a.yearweek,a.qty from selloutTable a left join masterTable b on a.regionid=b.regionid""".stripMargin)

    var rightJoin = spark.sql("""select b.regionname,a.regionid,a.product,a.yearweek,a.qty from selloutTable a right join masterTable b on a.regionid=b.regionid""".stripMargin)

    var groupByExam = spark.sql("""select regionid, product, avg(qty) as avg_qty from selloutTable group by regionid, product order by regionid,product""".stripMargin)

    var partitionByExam = spark.sql("""select a.regionid,a.product,a.yearweek,a.qty,avg(a.qty) over(partition by a.regionid,a.product) as avg_qty from selloutTable a""".stripMargin)

    var subQueryExam = spark.sql("""select b.*, b.qty/b.avg_qty as ratio from(select a.regionid,a.product,a.yearweek,a.qty,avg(a.qty) over(partition by a.regionid,a.product) as avg_qty from selloutTable a)b""")

    var subqueryFuntionExam = spark.sql("""select b.*, round(case when b.avg_qty = 0 then 1 else b.qty/b.avg_qty end ,2) as ratio from(select a.*, round(avg(a.qty) over(partition by a.regionid, a.product),2) as avg_qty from selloutTable a where 1=1 and substr(yearweek,1,4) >= 2015 and substr(yearweek,5,2) != 53 and product in ('PRODUCT1','PRODUCT2'))b""".stripMargin)

    var pivotResult = subqueryFuntionExam.groupBy("REGIONID","PRODUCT").pivot("YEARWEEK",Seq("201501","201502")).sum("ratio")

    ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////


    //spark 데이터 프레임

    var selloutDataFile = "kopo_product_volume.csv"

    var productData =
    spark.read.format("csv").
      option("header","true").
      option("Delimeter",",").load("c:/spark_orgin_2.2.0/bin/data/"+selloutDataFile)
    print(productData.show(5))

    //select을 통해 원하는 컬럼값 조회
    //행조회
    var selectedData =productData.select("PRODUCTGROUP","VOLUME").filter(($"PRODUCTGROUP" === "ST0001")&&($"VOLUME" > 150000))

    //열조회
    var columnData = productData.select("REGIONID","PRODUCTGROUP","YEARWEEK")

    //null처리
    var missingValueFile = "missingValue.csv"

    var missingData = spark.read.format("csv").option("header","true").option("Delimiter",",").load("c:/spark_orgin_2.2.0/bin/data/"+missingValueFile)

    //뒤에 작성한 컬럼에 null이 있을 경우 출력
    var missingColumnData = missingData.filter(($"VOLUME".isNull) ||($"TARGET".isNull))

    //전체 컬럼에 하나라도 null값이 있는 경우 해당 값 출력
    var missingAnyData = missingData.filter(row=>{row.anyNull})

    //전체 컬럼 null값 채우기
    var filteredAllData = missingData.na.fill("0")

    //특정 컬럼 null값 채우기
    var targetColume = Array("VOLUME","TARGET")
    var filteredTargetData = missingData.na.fill("0",targetColume)

    // 데이터 프레임 가공
    var promotionDataFile = "promotionData.csv"
    var promotionData = spark.read.format("csv").option("header","true").option("Delimiter",",").load("c:/spark_orgin_2.2.0/bin/data/"+promotionDataFile)

    //데이터 타입 확인
    promotionData.dtypes.foreach(println)

    //타입 변경
    var promotionDataTypeChange = promotionData.withColumn("PRICE",$"PRICE".cast("Double")).withColumn("DISCOUNT",$"DISCOUNT".cast("Double"))

    //이상데이터 sql case when과 같은 고급함수를 사용하기 위한 import
    import org.apache.spark.sql.functions._
    //new_distcount칼럼을 만들고, discount가 price보다 크다면 0, 그렇지 않으면 discount값 그대로 출력
    var promotionDataFinal = promotionDataTypeChange.withColumn("NEW_DISCOUNT",when($"DISCOUNT" > $"PRICE",0).otherwise($"DISCOUNT"))

    //데이터 정렬(기본 오름차순)
    var sortedData = productData.sort("REGIONID","PRODUCTGROUP","YEARWEEK")

    //컬럼별 정렬 오름,내림 (regionid는 오름차순, productgroup은 내림차순, yearweek는 오름차순)
    var sortedDataPart = productData.sort($"REGIONID".asc,$"PRODUCTGROUP".desc,$"YEARWEEK".asc)

    //집계함수 = groupBy
    import org.apache.spark.sql.functions._
    // 지역,상품 별 평균 거래량
    var groupByData = productData.groupBy($"REGIONID",$"PRODUCTGROUP").agg(mean($"VOLUME") as "MEAN_VOLUME")

    //데이터 조인
    //inner join
    var innerjoin = productData.join(promotionData, Seq("REGIONID","PRODUCTGROUP","YEARWEEK"),joinType = "inner")
    //left join
    var leftjoin = productData.join(promotionData, Seq("REGIONID","PRODUCTGROUP","YEARWEEK"),joinType = "left_outer")

    //고급함수 구현(이동 집계함수) ->이동평균값 계산가능
    // 타입변경 및 정렬
    var refinedproductData = productData.withColumn("VOLUME",$"VOLUME".cast("Double")).sort("REGIONID","PRODUCTGROUP","YEARWEEK")

    //이동 평균 계산 -1,현재,+1 => 이동평균구간은 3
    import org.apache.spark.sql.expressions.Window
    var movingAvg = refinedproductData.withColumn("MV_AVG",avg(refinedproductData("VOLUME")).over(Window.partitionBy("REGIONID").rowsBetween(-1,1)))

    //데이터 프레임 피벗
    var pivotDataFile = "decisionTreeResult.csv"
    var pivotData = spark.read.format("csv").option("header","true").option("Delimiter",",").load("c:/spark_orgin_2.2.0/bin/data/"+pivotDataFile)

    var sortedPivotData = pivotData.withColumn("SALES",$"SALES".cast("Double")).sort("PRODUCT","ITEM","YEARWEEK")

    var pivotDataResult = sortedPivotData.groupBy("PRODUCT","ITEM","YEARWEEK").pivot("MEASURE",Seq("REAL_QTY","PREDICTION_QTY")).sum("SALES")

    ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    //spark Rdd
    var filteredDataFile = "filterRddSample.csv"

    var filteredData =
      spark.read.format("csv").
        option("header","true").
        option("Delimeter",",").load("c:/spark_orgin_2.2.0/bin/data/"+filteredDataFile)
    print(filteredData.show(2))

    var filteredColums = filteredData.columns.map(x=>{x.toLowerCase()})
    var regionidNo = filteredColums.indexOf("regionid")
    var productNo = filteredColums.indexOf("productgroup")
    var yearweekNo = filteredColums.indexOf("yearweek")
    var volumeNo = filteredColums.indexOf("volume")

    var filterSampleRdd = filteredData.rdd

    //filter 가공
   var yearweek_size = 6
    var filteredRdd = filterSampleRdd.filter(row=>{
      var checkValid = true
      if(row.getString(yearweekNo).size != yearweek_size){
        checkValid = false
      }
      checkValid
    })

    //filter null값 filter
    var missingFilteredRdd = filteredRdd.filter(row=>{
      var rowSize = row.size
      var checkValid = true
      for(i<-0 until(rowSize)){
        if(row.isNullAt(i) == true)
          checkValid = false
      }
      checkValid
    })

    //map을 통한 가공
    var max_volume = 700000
    var mappedRdd = missingFilteredRdd.map(row=>{
      var volume = row.getString(volumeNo).toDouble
      if(volume >=max_volume){
        volume = max_volume
      }
      Row(row.getString(regionidNo),
        row.getString(productNo),
        row.getString(yearweekNo),
        volume)
    })

    //집계함수 사용 - groupby->map
    var DataFile = "kopo_product_volume.csv"

    var outData =
      spark.read.format("csv").
        option("header","true").
        option("Delimeter",",").load("c:/spark_orgin_2.2.0/bin/data/"+DataFile)

    var outColums = outData.columns.map(x=>{x.toLowerCase()})
    var regionidNo = outColums.indexOf("regionid")
    var productNo = outColums.indexOf("productgroup")
    var yearweekNo = outColums.indexOf("yearweek")
    var volumeNo = outColums.indexOf("volume")

    var outRdd = outData.rdd

    var groupRdd = outRdd.groupBy(x=>{
      (x.getString(regionidNo),x.getString(productNo))
    }).map(x=>{
      var key = x._1
      var data = x._2
      var size = data.size
      var volumeSum = data.map(x=>x.getString(volumeNo).toDouble).sum
      var average = volumeSum/size
      (key,average)
    })

    //집계함수 - groupby -> flatmap

    var sampleRdd = outData.rdd
    var groupFlatMapRdd = sampleRdd.groupBy(x=>{
      (x.getString(regionidNo),x.getString(productNo))
    }).flatMap(x=>{
      var key = x._1
      var data = x._2
      var size = data.size
      var volumeSum = data.map(x=>x.getString(volumeNo).toDouble).sum
      var avg = volumeSum/size

      var finalData = data.map(x=>{
        (x.getString(regionidNo),
          x.getString(productNo),
          x.getString(yearweekNo),
          x.getString(volumeNo),
          avg)
      })
      finalData
    })

    //CollectAsMap -> key와 value를 저장한 후 사용할 수 있다.
    var collectSampleRdd = outData.rdd
    var collectAsMapRdd = collectSampleRdd.groupBy(x=>{
      (x.getString(regionidNo),x.getString(productNo))
    }).map(x=>{
      var key = x._1
      var data =x._2
      var size = data.size
      var volumeSum = data.map(x=>x.getString(volumeNo).toDouble).sum
      var avg = volumeSum / size
      (key,(size,avg))
    }).collectAsMap

    //데이터 프레임으로 변환
    var sampleRddTest = outRdd.map(x=>{
      (x.getString(regionidNo),x.getString(productNo),x.getString(yearweekNo),x.getString(volumeNo))})

    var resultDf = sampleRddTest.toDF("REGIONID","PRODUCT","YEARWEEK","VOLUME")


    var testRddRow = outRdd.map(x=>{
      Row(x.getString(regionidNo),
        x.getString(productNo),
        x.getString(yearweekNo),
        x.getString(volumeNo))
    })

    var  resultRdwDf = spark.createDataFrame(testRddRow,
      StructType(Seq(
        StructField("REGIONID",StringType),
        StructField("PRODUCT",StringType),
        StructField("YEARWEEK",StringType),
        StructField("VOLUME",StringType)
      )))



    ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    //통계적 분석 -> 계절설 지수 구하기
    //함수로 만들어서 파일 불러오기
    var filePath = "C:/spark_orgin_2.2.0/bin/data/"
    var sampleName = "kopo_product_sellout.csv"

    def csvFileLoading(workPath: String, fileName: String)
    :org.apache.spark.sql.DataFrame = {
      var outDataFrame =
        spark.read.format("csv").
          option("header","true").
          option("Delimiter",",").
          load(workPath+fileName)
      outDataFrame
    }

    var sampleData = csvFileLoading(filePath,sampleName)
    print(sampleData.show())

    //데이터 정제 -> 53주차제거, 생산량 700000변경(최대값), qty 마이너스값 0 변경
    sampleData.createOrReplaceTempView("SELLOUT_VIEW")
    var exceptWeek = "53"
    var minVolume = 0
    var maxVolume = 700000
    var refinedQuery ="""select regionid, product, yearweek, cast(case when qty < """+minVolume+""" then 0 when qty > """+maxVolume+""" then 700000 else qty end as DOUBLE) as qty from sellout_view where 1=1 and substr(yearweek,5,6) != """+exceptWeek


    var selloutDf = spark.sql(refinedQuery)
    print(selloutDf.show)

    var sampleDfColumes = selloutDf.columns.map(x=>{x.toLowerCase})
    var regionidNo = sampleDfColumes.indexOf("regionid")
    var productNo = sampleDfColumes.indexOf("product")
    var yearweekNo = sampleDfColumes.indexOf("yearweek")
    var qtyNo = sampleDfColumes.indexOf("qty")

    var seasonRdd = selloutDf.rdd.groupBy(x=>{
      (x.getString(regionidNo),x.getString(productNo))
    }).flatMap(x=>{
      var key = x._1
      var data = x._2
      var sum_qty = data.map(x=>{x.getDouble(qtyNo)}).sum

      var size = data.size
      var avg = math.round(sum_qty / size)
      var finalData = data.map(x=>{
        var ratio = 1.0d
        var each_qty = x.getDouble(qtyNo)
        ratio = each_qty / avg
        ratio = Math.round(ratio*100.0) / 100.0d

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

    var seasonlityQuery ="""select regionid, product,substr(yearweek,5,6) as week,round(avg(ratio),2) as ratio from MIDDLE_VIEW group by regionid, product,substr(yearweek,5,6)"""

    var seasonalityDf = spark.sql(seasonlityQuery)

    print(seasonalityDf.sort("REGIONID","PRODUCT","WEEK").show())

    ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    //머신러닝 -> 지도 학습

    var filePath = "C:/spark_orgin_2.2.0/bin/data/"
    var sampleName = "kopo_decisiontree_input.csv"

    def csvFileLoading(workPath: String, fileName: String)
    :org.apache.spark.sql.DataFrame = {
      var outDataFrame =
        spark.read.format("csv").
          option("header","true").
          option("Delimiter",",").
          load(workPath+fileName)
      outDataFrame
    }

    var sampleData = csvFileLoading(filePath,sampleName)
    print(sampleData.show())

    //데이터 타입 변경
    var dtInput = sampleData.withColumn("QTY",$"QTY".cast("Double")).withColumn("PRO_PERCENT",$"PRO_PERCENT".cast("Double"))

    //머신러닝 라이브러리
    import org.apache.spark.ml.Pipeline
    import org.apache.spark.ml.evaluation.RegressionEvaluator
    import org.apache.spark.ml.feature.VectorAssembler
    import org.apache.spark.ml.regression.{DecisionTreeRegressionModel,DecisionTreeRegressor}
    import org.apache.spark.ml.feature.{StringIndexer}

    //문자값에 인덱스 숫자를 적용시키는 방법 -> StringIndexer
    val holidayIndexer = new StringIndexer().setInputCol("HOLIDAY").setOutputCol("HOLIDAY_IN")
    val promotionIndexer = new StringIndexer().setInputCol("PROMOTION").setOutputCol("PROMOTION_IN")

    //학습과 예측 데이터 분리
    var targetYearweek = 201630
    val trainData = dtInput.filter($"YEARWEEK" <= targetYearweek)
    val testData = dtInput.filter($"YEARWEEK" > targetYearweek)

    //훈련
    val assembler = new VectorAssembler().setInputCols(Array("HOLIDAY_IN","PROMOTION_IN","PRO_PERCENT")).setOutputCol("FEATURES")

    var dt = new DecisionTreeRegressor().setLabelCol("QTY").setFeaturesCol("FEATURES")

    val pipeline = new Pipeline().setStages(Array(holidayIndexer,promotionIndexer,assembler,dt))

    val model = pipeline.fit(trainData)

    val predictions = model.transform(testData)

    predictions.select("REGIONID","PRODUCT","ITEM","YEARWEEK","QTY","FEATURES","PREDICTION").orderBy("YEARWEEK").show

    //MAE, RMSE 평가
    val evaluatorRmse = new RegressionEvaluator().setLabelCol("QTY").setPredictionCol("prediction").setMetricName("rmse")

    val evauatorMae = new RegressionEvaluator().setLabelCol("QTY").setPredictionCol("prediction").setMetricName("mae")

    val rmse = evaluatorRmse.evaluate(predictions)
    val mae = evauatorMae.evaluate(predictions)


    //머신러닝 -> 비지도학습

    var filePath = "C:/spark_orgin_2.2.0/bin/data/"
    var machineName = "student_middle.csv"

    def csvFileLoading(workPath: String, fileName: String)
    :org.apache.spark.sql.DataFrame = {
      var outDataFrame =
        spark.read.format("csv").
          option("header","true").
          option("Delimiter",",").
          load(workPath+fileName)
      outDataFrame
    }

    var machineData = csvFileLoading(filePath,machineName)
    print(machineData.show())

    //데이터 타입변경
    var kmeansInput = machineData.withColumn("SW",$"SW".cast("Double")).withColumn("DB",$"DB".cast("Double")).withColumn("AND",$"AND".cast("Double"))
    kmeansInput.dtypes

    //k-means Clustering 라이브러리 추가
    import org.apache.spark.ml.feature.VectorAssembler
    import org.apache.spark.ml.Pipeline
    import org.apache.spark.ml.clustering.KMeans

    //훈련
    var assembler = new VectorAssembler().setInputCols(Array("SW","DB","AND")).setOutputCol("FEATURES")
    var kValue = 2
    val kmeans = new KMeans().setK(kValue).setFeaturesCol("FEATURES").setPredictionCol("PREDICTION")
    val pipeline = new Pipeline().setStages(Array(assembler,kmeans))
    val kMeansPredictionModel = pipeline.fit(kmeansInput)

    //예측
    val predictionResult = kMeansPredictionModel.transform(kmeansInput)













  }
}

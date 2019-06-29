package com.haiteam
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext}


object Spark0620_Answer {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().
      setAppName("DataLoading").
      setMaster("local[*]")
    var sc = new SparkContext(conf)
    val spark = new SQLContext(sc)
    import spark.implicits._


    var join1Name = "joindata1.csv"

    // 절대경로 입력
    var join1Df=
      spark.read.format("csv").
        option("header","true").
        option("Delimiter",",").
        load("C:/spark_orgin_2.2.0/bin/data/"+join1Name)

    var join12Name = "joindata2.csv"

    // 절대경로 입력
    var join2Df=
      spark.read.format("csv").
        option("header","true").
        option("Delimiter",",").
        load("C:/spark_orgin_2.2.0/bin/data/"+join12Name)

    //collectAsMap
    var refdefMap = join2Df.rdd.map(x=>{
      var key = x.getString(0).trim()
      var value = x.getString(1).trim()
      (key,value)
    }).collectAsMap
    // Case#1: collectAsMap Method
    // update logic
    var updatedMap = join1Df.rdd.map(x=>{
      // get org_suta_tin
      var org_sutatin =  x.getString(7).trim()
      // define new suta_tim
      var changed_sutatin = org_sutatin

      // search the key
      if( refdefMap.contains(org_sutatin) ) {
        //if exist update it
        changed_sutatin = refdefMap(org_sutatin)
      }
      //output
      (org_sutatin, changed_sutatin)
    })

    // Case#2: collectAsMap Method
    join1Df.createOrReplaceTempView("join1Table")
    join2Df.createOrReplaceTempView("join2Table")
    var outputResult = spark.sql("""
    SELECT
    CASE WHEN B.SUTA_TIN IS NULL THEN A.SUTA_TIN
    ELSE B.MATA_TIN END AS UPDATED_SUTA_TIN,
    NVL2(B.SUTA_TIN, B.MATA_TIN, A.SUTA_TIN) AS UP2,
    A.*,B.*
    FROM join1Table A
    LEFT JOIN join2Table B
      ON A.SUTA_TIN = B.SUTA_TIN""")

    join2Df.columns

    // 컬럼 소문자 변환
    var join1DfColumns = join1Df.columns.map(x=>{x.toLowerCase()})

    // 컬럼 인덱스 정의
    var mainsutatinNo = join1DfColumns.indexOf("suta_tin")


    // 컬럼 소문자 변환
    var join2DfColumns = join2Df.columns.map(x=>{x.toLowerCase()})

    // 컬럼 인덱스 정의
    var sutatinNo = join2DfColumns.indexOf("suta_tin")
    var matatinNo = join2DfColumns.indexOf("mata_tin")

    var join2Rdd = join2Df.rdd
    // key ,value 형태로 묶겠다!!!
    var join2Map = join2Rdd.groupBy(x=>{
      x.getString(sutatinNo)
    }).map(x=>{

      var key = x._1.trim()
      var data = x._2
      var matatinValue = data.map(x=>{x.getString(matatinNo).trim()}).toArray
      (key, matatinValue(0))
    })

    var join2CMap = join2Map.collectAsMap()

    join2Map.collectAsMap()
    // 메인데이터
    var join1Rdd = join1Df.rdd

    var answer = join1Rdd.map(x=>{
      // 기존 suta_tin 번호를 가져온다
      var org_sutatin = x.getString(mainsutatinNo).trim()
      var new_sutatin = ""
      // 기존테이블정보가 수정테이블정보 있으면 수정테이블의 값으로 업데이트
      if(join2CMap.contains(org_sutatin)){
        new_sutatin = join2CMap(org_sutatin)
        print()
      }else{
        // 없으면 기존 org_sutatin 유지
        new_sutatin = org_sutatin
      }
      // Define output Layout
      (org_sutatin, new_sutatin)
    })

    var outputDf = answer.toDF("ORG_SUTATIN","NEW_SUTATIN")

    // 데이터베이스 주소 및 접속정보 설정 (1)
    var outputUrl = "jdbc:oracle:thin:@192.168.110.112:1521/orcl"
    var outputUser = "kopo"
    var outputPw = "kopo"

    // 데이터 저장 (2)
    val prop = new java.util.Properties
    prop.setProperty("driver", "oracle.jdbc.OracleDriver")
    prop.setProperty("user", outputUser)
    prop.setProperty("password", outputPw)
    val table = "JOINOUT"
    //append
    outputDf.write.mode("overwrite").jdbc(outputUrl, table, prop)

  }
}



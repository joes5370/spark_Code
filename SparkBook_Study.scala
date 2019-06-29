package com.haiteam

import org.apache.spark.sql.SQLContext
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

    ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
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

    ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    //spark 데이터 프레임

  }
}

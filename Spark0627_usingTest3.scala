package com.haiteam

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object Spark0627_usingTest3 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().
      setAppName("DataLoading").
      setMaster("local[*]")
    var sc = new SparkContext(conf)
    val spark = new SQLContext(sc)

    import spark.implicits._
    import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode}
    import org.apache.spark.sql.types.{StringType, StructField, StructType}

    // User Defined Function
    // Purpose of Funtion : Calculate pre week
    def preWeek(inputYearWeek: String, gapWeek: Int): String = {
      val currYear = inputYearWeek.substring(0, 4).toInt
      val currWeek = inputYearWeek.substring(4, 6).toInt

      val calendar = Calendar.getInstance();
      calendar.setMinimalDaysInFirstWeek(4);
      calendar.setFirstDayOfWeek(Calendar.MONDAY);

      var dateFormat = new SimpleDateFormat("yyyyMMdd");

      calendar.setTime(dateFormat.parse(currYear + "1231"));
      //    calendar.getActualMaximum(Calendar.WEEK_OF_YEAR)

      if (currWeek <= gapWeek) {
        var iterGap = gapWeek - currWeek
        var iterYear = currYear - 1

        calendar.setTime(dateFormat.parse(iterYear.toString() + "1231"));
        var iterWeek = calendar.getActualMaximum(Calendar.WEEK_OF_YEAR)

        while (iterGap > 0) {
          if (iterWeek <= iterGap) {
            iterGap = iterGap - iterWeek
            iterYear = iterYear - 1
            calendar.setTime(dateFormat.parse(iterYear.toString() + "1231"))
            iterWeek = calendar.getActualMaximum(Calendar.WEEK_OF_YEAR)
          } else {
            iterWeek = iterWeek - iterGap
            iterGap = 0
          } // end of if
        } // end of while

        return iterYear.toString + "%02d".format(iterWeek)
      } else {
        var resultYear = currYear
        var resultWeek = currWeek - gapWeek

        return resultYear.toString + "%02d".format(resultWeek)
      } // end of if
    } // end of function

    var targetFile = "pro_actual_sales.csv"

    // 절대경로 입력
    var targetDf=
      spark.read.format("csv").
        option("header","true").
        option("Delimiter",",").
        load("C:/spark_orgin_2.2.0/bin/data/"+targetFile)

    // 데이터 확인 (3)
    print(targetDf.show(2))


    var targetDfColumns = targetDf.columns.map(x=>{x.toLowerCase()})
    var regionseg1no = targetDfColumns.indexOf("regionseg1")
    var proseg2no = targetDfColumns.indexOf("productseg2")
    var proseg3no = targetDfColumns.indexOf("productseg3")
    var yearweekno = targetDfColumns.indexOf("yearweek")
    var qtyno = targetDfColumns.indexOf("qty")

    // 3. [심화문제] pro_actual_sales.csv 파일을 활용하여
    // max yearweek 20주간의 판매 실적을 활용하여
    // (regionseg1, productseg2) 내 productseg3의 판매량 상위0~30% 이상은 클러스터 0,
    // 31%~70%는 클러스터1 이외는 클러스터2를 산출하는 프로그램을 만들고
    // (regionseg1,productseg2, productseg3) 호출 시 클러스터 값이 나오도록 맵함수를 생성하세요.

    // step1: 최근 20주 정보에서
    // step2: regionseg1, proseg2, proseg3로 그룹바이한 후 qtysum 산출
    // step3: regionseg1, proseg2, 그룹바이한후
    //        내림차순 정렬하여 랭크계산 이후 maxsize로 나눈다
    // 랭크/maxsize를 산출 후 0~0.3->0  0.31~0.7->1 이외 2




    var targetRdd = targetDf.rdd

    var currWeek = targetRdd.map(x=>{x.getString(yearweekno)}).max()
    var valid_week = 20
    var start_week = preWeek(currWeek,valid_week)

    // step1: 최근 20주 정보에서
    var step1Rdd = targetRdd.filter(x=> {
      var checkValid = false
      if(x.getString(yearweekno).toInt >= start_week.toInt){
        checkValid = true
      }
      checkValid
    }).// step2: regionseg1, proseg2, proseg3로 그룹바이한 후 qtysum 산출
      groupBy(x=>{
      (x.getString(regionseg1no),
        x.getString(proseg2no),
        x.getString(proseg3no))
    }).map(x=>{
      var key = x._1
      var data = x._2
      var sumation = x._2.
        map(x=>{x.getString(qtyno).toDouble}).sum
      //regionseg1, proseg2, proseg3, qtysum
      (key._1, key._2, key._3,sumation)
    })

    // step3: regionseg1, proseg2, 그룹바이한후
    //        내림차순 정렬하여 랭크계산 이후 maxsize로 나눈다
    // 랭크/maxsize를 산출 후 0~0.3->0  0.31~0.7->1 이외 2
    var step2Rdd = step1Rdd.groupBy(x=>{
      //regionseg1, proseg2
      (x._1,x._2)
    }).flatMap(x=>{
      var key = x._1
      var data = x._2

      var orderedData = data.toArray.sortBy(x=>{-x._4})

      var zipData = orderedData.zipWithIndex
      // ((REGION, PRODUCT, ITEM, QTY), NUM)
      var calData = zipData.map(x=>{
        var rankNo = x._2+1
        (x._1._1, x._1._2,x._1._3,x._1._4,rankNo)
      })
      x._2
    })

    var step2Rdd = step1Rdd.groupBy(x=>{
      //regionseg1, proseg2
      (x._1,x._2)
    }).flatMap(x=> {

      var key = x._1
      var data = x._2

      var region = key._1
      var productg = key._2

      var orderedData = data.toArray.sortBy(x => {
        -x._4
      })

      // RDD 풀이 1
      var dataSize = orderedData.size
      var dataArray = Array.fill(dataSize)("region", "product", "item", 0.0d, 0, 0.0d, 0)

      var i = 0
      // regionid, product, cluster, rank
      while (i < dataSize) {
        var rank = i + 1
        var ratio = rank.toDouble / dataSize.toDouble
        var cluster = if (ratio <= 0.3) {
          0
        } else if (ratio <= 0.6) {
          1
        } else {
          2
        }
        dataArray(i) = (orderedData(i)._1,
          orderedData(i)._2,
          orderedData(i)._3,
          orderedData(i)._4,
          rank, ratio, cluster)
        i = i + 1
      }

      // RDD 풀이 2
      var zipData = orderedData.zipWithIndex

      var zipRankData = zipData.map(x=>{

        var rank = x._2+1
        var ratio = rank.toDouble/dataSize.toDouble
        var cluster = if(ratio <= 0.3){
          0
        }else if(ratio <= 0.6){
          1
        }else{
          2
        }
        (x._1._1,
          x._1._2,
          x._1._3,
          rank,ratio,cluster)
      })

      zipRankData
    })
    //여기까지







    var rddresult = step2Rdd.toDF("REGIONSEG1","PRODUCTSEG2","PRODUCTSEG3",
      "RANK2","RATIO2","CLUSTER2")

    // SPARK SQL 풀이
    targetDf.createOrReplaceTempView("test")

    var final2 = spark.sql(
      """
        select B.REGIONSEG1
 |, B.PRODUCTSEG2
 |, B.PRODUCTSEG3
 |, B.RANK2 AS RANK2
 |, B.RANK2/B.COUNT2 as RATIO,
 |case when rank2/count2 <= 0.3 then 0
 |     when rank2/count2 <= 0.6 then 1
 |     else 2 end as CLUSTER2
 |from (
 |select regionseg1,
 |productseg2,
 |productseg3,
 |sum_qty,
 |row_number() over (partition by regionseg1,productseg2 order by regionseg1, productseg2, sum_qty desc) as RANK2,
 |count(*) over (partition by regionseg1,productseg2) as count2
 |from(
 |    select regionseg1, productseg2, productseg3, sum(qty) as sum_qty
 |    from test
 |    where 1=1
 |    and yearweek >= 201607
 |    group by regionseg1, productseg2, productseg3))B""")

    var sqlresult = final2

    // 데이터베이스 주소 및 접속정보 설정 (1)
    //var outputUrl = "jdbc:oracle:thin:@192.168.110.112:1521/orcl"
    var outputUrl = "jdbc:oracle:thin:@127.0.0.1:1521/xe"
    var outputUser = "kopo"
    var outputPw = "kopo"

    // 데이터 저장 (2)
    val prop = new java.util.Properties
    prop.setProperty("driver", "oracle.jdbc.OracleDriver")
    prop.setProperty("user", outputUser)
    prop.setProperty("password", outputPw)
    val table = "kopo_out2"
    //append
    sqlresult.write.mode("overwrite").jdbc(outputUrl, table, prop)


    // PYTHON(3), SPARK(3)
  }
}

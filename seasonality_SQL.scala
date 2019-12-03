package com.haiteam

object Seasonality2 {

  def main(args: Array[String]): Unit = {

    import java.util.Calendar

    import org.apache.spark.sql.SparkSession

    var spark = SparkSession.builder().config("spark.master", "local").getOrCreate()

    // Time

    var Start_calendar = Calendar.getInstance()
    var StartTime = Start_calendar.getTime()
    var Start_Minutes = StartTime.getMinutes()
    var Start_second = StartTime.getSeconds()
    var Time1 = Start_Minutes*60+Start_second

    // 1. Data Loading (from Oracle DB)

//    var staticUrl = "jdbc:oracle:thin:@192.168.110.112:1521/orcl"
//    var staticUser = "kopo"
//    var staticPw = "kopo"
//    var selloutDb = "kopo_channel_seasonality_new"
//
//    val selloutDataFromOracle = spark.read.format("jdbc").options(Map("url" -> staticUrl, "dbtable" -> selloutDb, "user" -> staticUser, "password" -> staticPw)).load

    var salesFile = "kopo_channel_seasonality_new.csv"
    // 절대경로 입력
    var salesDf =
      spark.read.format("csv").
        option("header", "true").
        option("Delimiter", ",").
        load("C:/spark_orgin_2.2.0/bin/data/" + salesFile)


    salesDf.createOrReplaceTempView("rawData")

    // 2. Funtion, Query
    def wingLength(window: Int): Int = {
      if (window % 2 == 0) {
        //exception 던지기
        throw new IllegalArgumentException("Only an odd number, please.")
      }
      var wing = (window - 1) / 2
      wing
    }

    var window1 = 17 //  Need to be connected to the parameter map

    var window2 = 5

    var wing1 = wingLength(window1).toString() // i.e. 8 //이상치 제거 -> refined QTY

    var wing2 = wingLength(window2).toString() //  i.e. 2 //최종적 추세선을 구하기 위해서

    var validWeek = 52

    var query_rolling1 = """OVER (PARTITION BY REGIONID, PRODUCT
                        ORDER BY REGIONID, PRODUCT, YEARWEEK
                        ROWS BETWEEN """ + wing1 + " PRECEDING AND " + wing1 + " FOLLOWING) "

    var query_rolling2 = """OVER (PARTITION BY REGIONID, PRODUCT
                        ORDER BY REGIONID, PRODUCT, YEARWEEK
                        ROWS BETWEEN """ + wing2 + " PRECEDING AND " + wing2 + " FOLLOWING) "

    // 3. Spark SQL Query

    var query_cleansing = """SELECT REGIONID
                        ,PRODUCT
                        ,YEARWEEK
                        ,SUBSTR(YEARWEEK,5,2) AS WEEK
                        ,CASE WHEN QTY<0 THEN 0
                        ELSE QTY END AS QTY
                        FROM rawData
                        WHERE SUBSTR(YEARWEEK,5,2) <= """ + validWeek.toString()

    var data_cleansed = spark.sql(query_cleansing)

    println("...")
    println("=== Loading data from Oracle DB O.K. ===")

    data_cleansed.createOrReplaceTempView("data_cleansed")

    var query_ma = "SELECT A.*, STDDEV(MA) " + query_rolling2 + """ AS STDDEV
                    FROM ( SELECT REGIONID
                    ,PRODUCT
                    ,YEARWEEK
                    ,QTY
                    ,AVG(QTY) """ + query_rolling1 + " AS MA " + "FROM data_cleansed) A"

    var data_ma = spark.sql(query_ma)

    println("...")
    println("=== Data Cleansing Process O.K. ===")

    data_ma.createOrReplaceTempView("data_ma")

    var query_smoothing = "SELECT A.* " + ",AVG(REFINED) " + query_rolling2 + """ AS SMOOTHED
                          FROM ( SELECT REGIONID
                            ,PRODUCT
                            ,YEARWEEK
                            ,QTY
                            ,MA
                            ,STDDEV
                            ,(MA + STDDEV) AS UPPER_BOUND
                            ,(MA - STDDEV) AS LOWER_BOUND
                            ,CASE WHEN QTY > (MA + STDDEV) THEN (MA + STDDEV)
                                WHEN QTY < (MA - STDDEV) THEN (MA - STDDEV)
                                ELSE QTY END AS REFINED FROM data_ma) A"""

    var data_smoothed = spark.sql(query_smoothing)

    data_smoothed.createOrReplaceTempView("data_smoothed")

    var query_seasonal = """SELECT A.*
                           ,CASE WHEN SMOOTHED = 0 THEN 1
                               ELSE (QTY / SMOOTHED) END AS SEASONALITY_STABLE
                           ,CASE WHEN SMOOTHED = 0 THEN 1
                               ELSE (REFINED / SMOOTHED) END AS SEASONALITY_UNSTABLE
                           FROM data_smoothed A"""

    var data_seasonal = spark.sql(query_seasonal)

    println("...")
    println("=== Calculating Seasonality Index Process O.K. ===")

    data_seasonal.createOrReplaceTempView("data_seasonal")

    println("...")
    println("=== Showing Seasonality Index Table O.K. ===")
    data_seasonal.show(10)

    var query_seasonal_final = """SELECT REGIONID
                                  ,PRODUCT
                                  ,QTY
                                  ,SUBSTR(YEARWEEK, 5, 2) AS WEEK
                                  ,AVG(SEASONALITY_STABLE) AS SEASONALITY_STABLE
                                  ,AVG(SEASONALITY_UNSTABLE) AS SEASONALITY_UNSTABLE
                                FROM data_seasonal
                                GROUP BY REGIONID, PRODUCT, QTY, SUBSTR(YEARWEEK, 5, 2)"""

    var data_seasonal_final = spark.sql(query_seasonal_final)

    data_seasonal_final.createOrReplaceTempView("data_seasonal_final")

    var query_forecast_Qty =
      """SELECT A.*, (A.QTY * A.SEASONALITY_STABLE) AS FORECAST_QTY
        FROM(SELECT * FROM data_seasonal_final)A""".stripMargin

    var forecast_qty_final = spark.sql(query_forecast_Qty)

    println("...")
    println("=== Showing Final Seasonality Index Table O.K. ===")
    forecast_qty_final.show(10)

    //4. Data Unloading (

    //    val prop = new java.util.Properties
    //    prop.setProperty("driver", "oracle.jdbc.OracleDriver")
    //    prop.setProperty("user", staticUser)
    //    prop.setProperty("password", staticPw)
    //    val table1 = "team4_seasonality_sql"
    //    val table2 = "team4_seasonality_final_sql"
    //
    //    data_seasonal_final.write.mode("overwrite").jdbc(staticUrl, table1, prop)
    //    println("Seasonality model completed, Data Inserted in Oracle DB")

//    var outputUrlPG = "jdbc:postgresql://192.168.110.111:5432/kopo"
//    var propPG = new java.util.Properties
//    var staticUserPsgre = "kopo"
//    var staticPwPsgre = "kopo"
//    propPG.setProperty("driver", "org.postgresql.Driver")
//    propPG.setProperty("user", staticUserPsgre)
//    propPG.setProperty("password", staticPwPsgre)
//
//    val table1 = "team4_seasonality_sql"
//    val table2 = "team4_seasonality_final_sql"
//    data_seasonal.write.mode("overwrite").jdbc(outputUrlPG, table1, propPG)
//    data_seasonal_final.write.mode("overwrite").jdbc(outputUrlPG, table2, propPG)
//
//    println("...")
//    println("=== Seasonality model completed. Data Inserted in PostgreSQL DB ===")
//
//    // Time
//
//    var End_calendar = Calendar.getInstance()
//    var EndTime = End_calendar.getTime()
//    var End_Minutes = EndTime.getMinutes()
//    var End_second = EndTime.getSeconds()
//    var Time2 = End_Minutes*60+End_second
//
//    var time_diff = Time2 - Time1
//    println("�� �ҿ�ð�: " + time_diff + "�� �Դϴ�.")

  }

}

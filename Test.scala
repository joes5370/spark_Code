package com.haiteam

    import org.apache.spark.sql.SQLContext
    import org.apache.spark.{SparkConf, SparkContext}


    object Test {
      def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("DataFrame").
          setMaster("local[4]")
        var sc = new SparkContext(conf)
        val spark = new SQLContext(sc)

        ///////////////////////////  데이터 파일 로딩  ////////////////////////////////////
//         접속정보 설정 (1)
        var staticUrl = "jdbc:postgresql://127.0.0.1:5432/postgres"

        var staticUser = "postgres"
        var staticPw = "postgres"
        var selloutDb = "kopo_product_volume"

        // 관계형 데이터베이스 Oracle 연결 (2)
        val dataFromPostgresql= spark.read.format("jdbc").
          option("url",staticUrl).
          option("dbtable",selloutDb).
          option("user",staticUser).
          option("password",staticPw).load
//
//    // 데이터 확인 (3)
    println(dataFromPostgresql.show(5))
//
//
//    var outputUrl = "jdbc:postgresql://192.168.110.24:5432/postgres"
//    var outputUser = "postgres"
//    var outputPw = "postgres"
//
//    // 데이터 저장 (2)
//    val prop = new java.util.Properties
//                                //oracle.jdbc.OracleDriver이걸 쓰면 안됨 왜? 포스트그레로 보낼꺼니까
//    prop.setProperty("driver", "org.postgresql.Driver")
//    prop.setProperty("user", outputUser)
//    prop.setProperty("password", outputPw)
//    val table = "kopo_product_volume_20190523_jhs"
//      //append
//    dataFromPostgresql.write.mode("overwrite").jdbc(outputUrl, table, prop)


//        ///////////////////////////     Oracle 데이터 로딩 ////////////////////////////////////
//        // 접속정보 설정
//var staticUrl = "jdbc:oracle:thin:@192.168.110.112:1521/orcl"
//        var staticUser = “kopo"
//          var staticPw = “kopo"
//          var selloutDb = "kopo_product_volume"
//
////
////          // jdbc (java database connectivity) 연결
//        val selloutDataFromOracle= spark.read.format("jdbc").
//          option("url",staticUrl).
//          option("dbtable",selloutDb).
//          option("user",staticUser).
//          option("password",staticPw).load
//
//        print(selloutDataFromOracle.show(5))
////
//


      }

}

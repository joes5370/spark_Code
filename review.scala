package com.haiteam

import org.apache.spark
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext


object review {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("DataFrame").
      setMaster("local[4]")
    var sc = new SparkContext(conf)
    val spark = new SQLContext(sc)


    var staticUrl = "jdbc:oracle:thin:@192.168.110.112:1521/orcl"
    var staticUser = "kopo"
    var staticPw = "kopo"
    var selloutDb = "kopo_product_volume"

      // jdbc (java database connectivity) 연결
      var selloutDataFromOracle = spark.read.format("jdbc").
      option("url",staticUrl).
      option("dbtable",selloutDb).
      option("user",staticUser).
      option("password",staticPw).load

      selloutDataFromOracle.show()
      // 메모리 테이블 생성
      selloutDataFromOracle.createOrReplaceTempView("selloutTable")


      }

}

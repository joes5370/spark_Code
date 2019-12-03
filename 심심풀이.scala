package com.haiteam

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}

object 심심풀이 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().
      setAppName("DataLoading").
      setMaster("local[*]")
    var sc = new SparkContext(conf)
    val spark = new SQLContext(sc)

    var paramFile = "kopo_product_volume.csv"

    // 절대경로 입력
    var paramData=
      spark.read.format("csv").
        option("header","true").
        option("Delimiter",","). // csv파일 불러올때는 꼭 확인해주자
        load("C:/spark_orgin_2.2.0/bin/data/"+paramFile)

    // 데이터 확인 (3)
    print(paramData.show)
  }
}

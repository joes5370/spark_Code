package com.haiteam
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
object pratice {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("DataFrame").
      setMaster("local[4]")
    var sc = new SparkContext(conf)
    val spark = new SQLContext(sc)

    var intValue = 10
    var doubleValue = 10.0

    print(intValue + doubleValue)

  }
}

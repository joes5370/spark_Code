package com.haiteam

import java.text.SimpleDateFormat
import java.util.Calendar
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext}
import scala.collection.mutable.ArrayBuffer


object 문제원형최종_0708 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().
      setAppName("DataLoading").
      setMaster("local[*]")
    var sc = new SparkContext(conf)
    val spark = new SQLContext(sc)
    import spark.implicits._

    var actualSales =
      spark.read.format("csv").
        option("header", "true").
        option("Delimiter", ",").
        load("c:/spark/bin/data/pro_actual_sales.csv")
    var promotion =
      spark.read.format("csv").
        option("header", "true").
        option("Delimiter", ",").
        load("c:/spark/bin/data/pro_promotion.csv")

    var actualSalesColumns = actualSales.columns
    var promotionColumns = promotion.columns

    var regionSeg1No = actualSalesColumns.indexOf("regionSeg1")
    var productSeg1No = actualSalesColumns.indexOf("productSeg1")
    var productSeg2No = actualSalesColumns.indexOf("productSeg2")
    var regionSeg2No = actualSalesColumns.indexOf("regionSeg2")
    var regionSeg3No = actualSalesColumns.indexOf("regionSeg3")
    var productSeg3No = actualSalesColumns.indexOf("productSeg3")
    var yearweekNo = actualSalesColumns.indexOf("yearweek")
    var yearNo = actualSalesColumns.indexOf("year")
    var weekNo = actualSalesColumns.indexOf("week")
    var qtyNo = actualSalesColumns.indexOf("qty")

    var regionsegNo = promotionColumns.indexOf("regionseg")
    var salesidNo = promotionColumns.indexOf("salesid")
    var productgroupNo = promotionColumns.indexOf("productgroup")
    var itemNo = promotionColumns.indexOf("item")
    var targetweekNo = promotionColumns.indexOf("targetweek")
    var planweeNo = promotionColumns.indexOf("planwee")
    var map_priceNo = promotionColumns.indexOf("map_price")
    var irNo = promotionColumns.indexOf("ir")
    var pmapNo = promotionColumns.indexOf("pmap")
    var pmap10No = promotionColumns.indexOf("pmap10")
    var pro_percentNo = promotionColumns.indexOf("pro_percent")

    var actualSalesRdd = actualSales.rdd
    var promotionRdd = promotion.rdd

    var filterPromotionRdd = promotionRdd.filter(x => {

      var targetweek = x.getString(targetweekNo)
      var planwee = x.getString(planweeNo)

      targetweek >= planwee

    })

    var fillPromotionRdd = filterPromotionRdd.groupBy(x => {

      var regionseg = x.getString(regionsegNo)
      var salesid = x.getString(salesidNo)
      var productgroup = x.getString(productgroupNo)
      var item = x.getString(itemNo)

      (regionseg,
        salesid,
        productgroup,
        item)

    }).flatMap(x => {

      var key = x._1
      var data = x._2

      var filterData = data.filter(x => {

        var map_price = x.getString(map_priceNo).toDouble

        map_price > 0

      })

      var size = filterData.size

      var sum = filterData.map(x => {

        var map_price = x.getString(map_priceNo).toDouble

        map_price

      }).sum

      var avg = if (size > 0) {

        sum / size

      } else {

        0d

      }

      var result = data.map(x => {

        var regionseg = x.getString(regionsegNo)
        var salesid = x.getString(salesidNo)
        var productgroup = x.getString(productgroupNo)
        var item = x.getString(itemNo)
        var targetweek = x.getString(targetweekNo)
        var planwee = x.getString(planweeNo)
        var map_price = x.getString(map_priceNo).toDouble
        var ir = x.getString(irNo).toDouble
        var pmap = x.getString(pmapNo).toDouble
        var pmap10 = x.getString(pmap10No).toDouble
        var pro_percent = x.getString(pro_percentNo).toDouble

        if (map_price == 0) {

          map_price = avg

          if (map_price == 0) {

            ir = 0d
            pmap = 0d
            pmap10 = 0d
            pro_percent = 0d

          } else {

            pmap = map_price - ir
            pmap10 = pmap * 0.9
            pro_percent = 1 - (pmap10 / map_price)

          }

        }

        Row(regionseg,
          salesid,
          productgroup,
          item,
          targetweek,
          planwee,
          map_price,
          ir,
          pmap,
          pmap10,
          pro_percent)

      })

      result

    })

    var collectPromotionRdd = fillPromotionRdd.groupBy(x => {

      var regionseg = x.getString(regionsegNo)
      var salesid = x.getString(salesidNo)
      var productgroup = x.getString(productgroupNo)
      var item = x.getString(itemNo)
      var targetweek = x.getString(targetweekNo)

      (regionseg,
        salesid,
        productgroup,
        item,
        targetweek)

    }).map(x => {

      var key = x._1
      var data = x._2

      var result = data.map(x => {

        var map_price = x.getDouble(map_priceNo)
        var ir = x.getDouble(irNo)
        var pmap = x.getDouble(pmapNo)
        var pmap10 = x.getDouble(pmap10No)
        var pro_percent = x.getDouble(pro_percentNo)

        (map_price,
          ir,
          pmap,
          pmap10,
          pro_percent)

      }).head

      (key, result)

    }).collectAsMap()

    var leftjoinActualSales = actualSalesRdd.groupBy(x => {

      var regionSeg1 = x.getString(regionSeg1No)
      var regionSeg2 = x.getString(regionSeg2No)
      var productSeg2 = x.getString(productSeg2No)
      var productSeg3 = x.getString(productSeg3No)
      var yearweek = x.getString(yearweekNo)

      (regionSeg1,
        regionSeg2,
        productSeg2,
        productSeg3,
        yearweek)

    }).map(x => {

      var key = x._1
      var data = x._2

      var map_price = 0d
      var ir = 0d
      var pmap = 0d
      var pmap10 = 0d
      var pro_percent = 0d
      var promotionYN = "N"

      if (collectPromotionRdd.contains(key)) {

        map_price = collectPromotionRdd(key)._1
        ir = collectPromotionRdd(key)._2
        pmap = collectPromotionRdd(key)._3
        pmap10 = collectPromotionRdd(key)._4
        pro_percent = collectPromotionRdd(key)._5

        if (map_price != 0) {

          promotionYN = "Y"

        }

      }

      var result = data.map(x => {

        var regionSeg1 = x.getString(regionSeg1No)
        var productSeg1 = x.getString(productSeg1No)
        var productSeg2 = x.getString(productSeg2No)
        var regionSeg2 = x.getString(regionSeg2No)
        var regionSeg3 = x.getString(regionSeg3No)
        var productSeg3 = x.getString(productSeg3No)
        var yearweek = x.getString(yearweekNo)
        var year = x.getString(yearNo)
        var week = x.getString(weekNo)
        var qty = x.getString(qtyNo)

      })

      result

    })

    map_priceNo += 4
    irNo += 4
    pmapNo += 4
    pmap10No += 4
    pro_percentNo += 4

    var promotionYNNo = pro_percentNo + 1

    def preWeek(inputYearWeek: String, gapWeek: Int): String = {

      val currYear = inputYearWeek.substring(0, 4).toInt
      val currWeek = inputYearWeek.substring(4, 6).toInt

      val calendar = Calendar.getInstance()
      calendar.setMinimalDaysInFirstWeek(4)
      calendar.setFirstDayOfWeek(Calendar.MONDAY)
      var dateFormat = new SimpleDateFormat("yyyyMMdd")
      calendar.setTime(dateFormat.parse(currYear + "1231"))

      if (currWeek <= gapWeek) {

        var iterGap = gapWeek - currWeek
        var iterYear = currYear - 1

        calendar.setTime(dateFormat.parse(iterYear.toString() + "1231"))

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

          }

        }

        return iterYear.toString + "%02d".format(iterWeek)

      } else {

        var resultYear = currYear
        var resultWeek = currWeek - gapWeek

        return resultYear.toString + "%02d".format(resultWeek)

      }

    }

    def postWeek(inputYearWeek: String, gapWeek: Int): String = {

      val currYear = inputYearWeek.substring(0, 4).toInt
      val currWeek = inputYearWeek.substring(4, 6).toInt

      val calendar = Calendar.getInstance()
      calendar.setMinimalDaysInFirstWeek(4)
      calendar.setFirstDayOfWeek(Calendar.MONDAY)
      var dateFormat = new SimpleDateFormat("yyyyMMdd")
      calendar.setTime(dateFormat.parse(currYear + "1231"))

      var maxWeek = calendar.getActualMaximum(Calendar.WEEK_OF_YEAR)

      if (maxWeek < currWeek + gapWeek) {

        var iterGap = gapWeek + currWeek - maxWeek
        var iterYear = currYear + 1

        calendar.setTime(dateFormat.parse(iterYear.toString() + "1231"))

        var iterWeek = calendar.getActualMaximum(Calendar.WEEK_OF_YEAR)

        while (iterGap > 0) {

          if (iterWeek < iterGap) {

            iterGap = iterGap - iterWeek
            iterYear = iterYear + 1

            calendar.setTime(dateFormat.parse(iterYear.toString() + "1231"))

            iterWeek = calendar.getActualMaximum(Calendar.WEEK_OF_YEAR)

          } else {

            iterWeek = iterGap
            iterGap = 0

          }

        }

        return iterYear.toString() + "%02d".format(iterWeek)

      } else {

        return currYear.toString() + "%02d".format((currWeek + gapWeek))

      }

    }

    def lagWeek(week1: String, week2: String): Int = {

      var fromWeek = ""
      var toWeek = ""

      if (week1.toInt < week2.toInt) {

        fromWeek = week1
        toWeek = week2

      } else {

        fromWeek = week2
        toWeek = week1

      }

      val calendar = Calendar.getInstance()
      calendar.setMinimalDaysInFirstWeek(4)
      calendar.setFirstDayOfWeek(Calendar.MONDAY)
      var dateFormat = new SimpleDateFormat("yyyyMMdd")

      var currYear = fromWeek.substring(0, 4).toInt
      var currWeek = fromWeek.substring(4, 6).toInt

      calendar.setTime(dateFormat.parse(currYear + "1231"))

      var maxWeek = calendar.getActualMaximum(Calendar.WEEK_OF_YEAR)

      if (maxWeek < currWeek) {

        return 0

      }

      currYear = toWeek.substring(0, 4).toInt
      currWeek = toWeek.substring(4, 6).toInt

      calendar.setTime(dateFormat.parse(currYear + "1231"))

      maxWeek = calendar.getActualMaximum(Calendar.WEEK_OF_YEAR)

      if (maxWeek < currWeek) {

        return 0

      }

      var calcGapNum = 0
      var calcWeek = toWeek

      while (fromWeek.toInt != calcWeek.toInt) {

        calcGapNum = calcGapNum + 1
        calcWeek = preWeek(toWeek, calcGapNum)

      }

      calcGapNum

    }

    var fillActualSales = leftjoinActualSales.groupBy(x => {

      var regionSeg1 = x.getString(regionSeg1No)
      var productSeg1 = x.getString(productSeg1No)
      var productSeg2 = x.getString(productSeg2No)
      var regionSeg2 = x.getString(regionSeg2No)
      var regionSeg3 = x.getString(regionSeg3No)
      var productSeg3 = x.getString(productSeg3No)

      (regionSeg1,
        productSeg1,
        productSeg2,
        regionSeg2,
        regionSeg3,
        productSeg3)

    }).flatMap(x => {

      var key = x._1
      var data = x._2

      var start = data.map(x => {

        var yearweek = x.getString(yearweekNo)

        yearweek

      }).min

      var end = data.map(x => {

        var yearweek = x.getString(yearweekNo)

        yearweek

      }).max

      var gap = lagWeek(start, end)

      var allYearweekArray = Array.empty[String]

      var i = 0

      while (i <= gap) {

        allYearweekArray ++= Array(postWeek(start, i))

        i += 1

      }

      var yearweekArray = data.map(x => {

        var yearweek = x.getString(yearweekNo)

        yearweek

      }).toArray

      var diffYearweekArray = allYearweekArray.diff(yearweekArray)

      var regionSeg1 = data.map(x => {

        var regionSeg1 = x.getString(regionSeg1No)

        regionSeg1

      }).head

      var productSeg1 = data.map(x => {

        var productSeg1 = x.getString(productSeg1No)

        productSeg1

      }).head

      var productSeg2 = data.map(x => {

        var productSeg2 = x.getString(productSeg2No)

        productSeg2

      }).head

      var regionSeg2 = data.map(x => {

        var regionSeg2 = x.getString(regionSeg2No)

        regionSeg2

      }).head

      var regionSeg3 = data.map(x => {

        var regionSeg3 = x.getString(regionSeg3No)

        regionSeg3

      }).head

      var productSeg3 = data.map(x => {

        var productSeg3 = x.getString(productSeg3No)

        productSeg3

      }).head

      var insertData = new ArrayBuffer[org.apache.spark.sql.Row]

      i = 0

      while (i < diffYearweekArray.size) {

        var yearweek = diffYearweekArray(i)
        var year = yearweek.substring(0, 4)
        var week = yearweek.substring(4, 6)

        insertData.append(Row(regionSeg1, productSeg1, productSeg2, regionSeg2, regionSeg3, productSeg3, yearweek, year, week, "0", 0d, 0d, 0d, 0d, 0d, "N"))

        i += 1

      }

      var result = data ++ insertData

      result

    })

    var sortActualSales = fillActualSales.sortBy(x => {

      var regionSeg1 = x.getString(regionSeg1No)
      var productSeg2 = x.getString(productSeg2No)
      var regionSeg2 = x.getString(regionSeg2No)
      var regionSeg3 = x.getString(regionSeg3No)
      var productSeg3 = x.getString(productSeg3No)
      var yearweek = x.getString(yearweekNo)

      (regionSeg1,
        productSeg2,
        regionSeg2,
        regionSeg3,
        productSeg3,
        yearweek)

    })

    var collectActualSales = sortActualSales.groupBy(x => {

      var regionSeg1 = x.getString(regionSeg1No)
      var productSeg1 = x.getString(productSeg1No)
      var productSeg2 = x.getString(productSeg2No)
      var regionSeg2 = x.getString(regionSeg2No)
      var regionSeg3 = x.getString(regionSeg3No)
      var productSeg3 = x.getString(productSeg3No)
      var yearweek = x.getString(yearweekNo)

      (regionSeg1,
        productSeg1,
        productSeg2,
        regionSeg2,
        regionSeg3,
        productSeg3,
        yearweek)

    }).map(x => {

      var key = x._1
      var data = x._2

      var qty = data.map(x => {

        var qty = x.getString(qtyNo).toDouble

        qty

      }).head

      (key, qty)

    }).collectAsMap()

    var seasonalityActualSales = sortActualSales.groupBy(x => {

      var regionSeg1 = x.getString(regionSeg1No)
      var productSeg1 = x.getString(productSeg1No)
      var productSeg2 = x.getString(productSeg2No)
      var regionSeg2 = x.getString(regionSeg2No)
      var regionSeg3 = x.getString(regionSeg3No)
      var productSeg3 = x.getString(productSeg3No)

      (regionSeg1,
        productSeg1,
        productSeg2,
        regionSeg2,
        regionSeg3,
        productSeg3)

    }).flatMap(x => {

      var key = x._1
      var data = x._2

      var seasonalityArray = data.map(x => {

        var regionSeg1 = x.getString(regionSeg1No)
        var productSeg1 = x.getString(productSeg1No)
        var productSeg2 = x.getString(productSeg2No)
        var regionSeg2 = x.getString(regionSeg2No)
        var regionSeg3 = x.getString(regionSeg3No)
        var productSeg3 = x.getString(productSeg3No)
        var yearweek = x.getString(yearweekNo)
        var qty = x.getString(qtyNo).toDouble

        var yearweekArray = Array(yearweek)

        for (i <- (1 to 2)) {

          yearweekArray ++= Array(preWeek(yearweek, i))
          yearweekArray ++= Array(postWeek(yearweek, i))

        }

        var qtyArray = Array.empty[Double]

        for (i <- yearweekArray) {

          var key = (regionSeg1, productSeg1, productSeg2, regionSeg2, regionSeg3, productSeg3, i)

          if (collectActualSales.contains(key)) {

            qtyArray ++= Array(collectActualSales(key))

          }

        }

        var size = qtyArray.size
        var qtySum = qtyArray.sum
        var avg = qtySum / size

        var seasonality = if (avg != 0) {

          qty / avg

        } else {

          0d

        }

        seasonality

      }).toArray

      var zipData = data.zipWithIndex

      var result = zipData.map(x => {

        var data = x._1
        var index = x._2

        var regionSeg1 = data.getString(regionSeg1No)
        var productSeg1 = data.getString(productSeg1No)
        var productSeg2 = data.getString(productSeg2No)
        var regionSeg2 = data.getString(regionSeg2No)
        var regionSeg3 = data.getString(regionSeg3No)
        var productSeg3 = data.getString(productSeg3No)
        var yearweek = data.getString(yearweekNo)
        var year = data.getString(yearNo)
        var week = data.getString(weekNo)
        var qty = data.getString(qtyNo)
        var map_price = data.getDouble(map_priceNo)
        var ir = data.getDouble(irNo)
        var pmap = data.getDouble(pmapNo)
        var pmap10 = data.getDouble(pmap10No)
        var pro_percent = data.getDouble(pro_percentNo)
        var promotionYN = data.getString(promotionYNNo)

        var seasonality = seasonalityArray(index)

        Row(regionSeg1,
          productSeg1,
          productSeg2,
          regionSeg2,
          regionSeg3,
          productSeg3,
          yearweek,
          year,
          week,
          qty,
          map_price,
          ir,
          pmap,
          pmap10,
          pro_percent,
          promotionYN,
          seasonality)

      })

      result

    })

    var seasonalityNo = promotionYNNo + 1

    var maxYearweek = seasonalityActualSales.map(x => {

      var yaerweek = x.getString(yearweekNo).toInt

      yaerweek

    }).max

    var filterActualSales = seasonalityActualSales.groupBy(x => {

      var regionSeg1 = x.getString(regionSeg1No)
      var productSeg1 = x.getString(productSeg1No)
      var productSeg2 = x.getString(productSeg2No)
      var regionSeg2 = x.getString(regionSeg2No)
      var regionSeg3 = x.getString(regionSeg3No)
      var productSeg3 = x.getString(productSeg3No)

      (regionSeg1,
        productSeg1,
        productSeg2,
        regionSeg2,
        regionSeg3,
        productSeg3)

    }).flatMap(x => {

      var key = x._1
      var data = x._2

      var yearweek = data.map(x => {

        var yearweek = x.getString(yearweekNo).toInt

        yearweek

      }).max

      var result = data.filter(x => {

        maxYearweek - yearweek <= 7

      })

      result

    })

    var collectSeasonalityActualSales = filterActualSales.groupBy(x => {

      var regionSeg1 = x.getString(regionSeg1No)
      var productSeg1 = x.getString(productSeg1No)
      var productSeg2 = x.getString(productSeg2No)
      var regionSeg2 = x.getString(regionSeg2No)
      var regionSeg3 = x.getString(regionSeg3No)
      var productSeg3 = x.getString(productSeg3No)
      var week = x.getString(weekNo)

      (regionSeg1,
        productSeg1,
        productSeg2,
        regionSeg2,
        regionSeg3,
        productSeg3,
        week)

    }).map(x => {

      var key = x._1
      var data = x._2

      var week = data.map(x => {

        var week = x.getString(weekNo)

      }).head

      var size = data.size

      var seasonalitySum = 0d
      var seasonalityAvg = 0d

      if (size > 0) {

        seasonalitySum = data.map(x => {

          var seasonality = x.getDouble(seasonalityNo)

          seasonality

        }).sum

        seasonalityAvg = seasonalitySum / size

      }

      (key, seasonalityAvg)

    }).collectAsMap()

    var forecastActualSales = filterActualSales.groupBy(x => {

      var regionSeg1 = x.getString(regionSeg1No)
      var productSeg1 = x.getString(productSeg1No)
      var productSeg2 = x.getString(productSeg2No)
      var regionSeg2 = x.getString(regionSeg2No)
      var regionSeg3 = x.getString(regionSeg3No)
      var productSeg3 = x.getString(productSeg3No)

      (regionSeg1,
        productSeg1,
        productSeg2,
        regionSeg2,
        regionSeg3,
        productSeg3)

    }).flatMap(x => {

      var key = x._1
      var data = x._2

      var regionSeg1 = data.map(x => {

        var regionSeg1 = x.getString(regionSeg1No)

        regionSeg1

      }).head

      var productSeg1 = data.map(x => {

        var productSeg1 = x.getString(productSeg1No)

        productSeg1

      }).head

      var productSeg2 = data.map(x => {

        var productSeg2 = x.getString(productSeg2No)

        productSeg2

      }).head

      var regionSeg2 = data.map(x => {

        var regionSeg2 = x.getString(regionSeg2No)

        regionSeg2

      }).head

      var regionSeg3 = data.map(x => {

        var regionSeg3 = x.getString(regionSeg3No)

        regionSeg3

      }).head

      var productSeg3 = data.map(x => {

        var productSeg3 = x.getString(productSeg3No)

        productSeg3

      }).head

      var filterData = data.filter(x => {

        var yearweek = x.getString(yearweekNo).toInt

        maxYearweek - yearweek <= 3

      })

      var size = 4
      var filterDataSize = filterData.size

      var qtySum = 0d
      var seasonalitySum = 0d

      if (filterDataSize > 0) {

        qtySum = filterData.map(x => {

          var qty = x.getString(qtyNo).toDouble

          qty

        }).sum

        seasonalitySum = filterData.map(x => {

          var seasonality = x.getDouble(seasonalityNo)

          seasonality

        }).sum

      }

      var forecast = Math.round(qtySum / size).toDouble
      var seasonalityAvg = seasonalitySum / size

      var postYearweekArray = Array.empty[String]
      var postWeekArray = Array.empty[String]

      var insertData = new ArrayBuffer[org.apache.spark.sql.Row]

      for (i <- 1 to 4) {

        var postYearweek = postWeek(maxYearweek.toString, i)
        var year = postYearweek.substring(0, 4)
        var targetWeek = postYearweek.substring(4, 6)

        postYearweekArray ++= Array(postYearweek)
        postWeekArray ++= Array(targetWeek)

        var containsKey = (regionSeg1, productSeg1, productSeg2, regionSeg2, regionSeg3, productSeg3, targetWeek)

        var weekSeasonalityAvg = if (collectSeasonalityActualSales.contains(containsKey)) {

          collectSeasonalityActualSales(containsKey)

        } else {

          0d

        }

        var forecastTimeseries = if (seasonalityAvg > 0) {

          forecast * weekSeasonalityAvg / seasonalityAvg

        } else {

          0d

        }

        insertData.append(Row(regionSeg1, productSeg1, productSeg2, regionSeg2, regionSeg3, productSeg3, postYearweek, year, targetWeek, "0", 0d, 0d, 0d, 0d, 0d, "N", 0d, forecast, weekSeasonalityAvg, forecastTimeseries))

      }

      var fillData = data.map(x => {

        var regionSeg1 = x.getString(regionSeg1No)
        var productSeg1 = x.getString(productSeg1No)
        var productSeg2 = x.getString(productSeg2No)
        var regionSeg2 = x.getString(regionSeg2No)
        var regionSeg3 = x.getString(regionSeg3No)
        var productSeg3 = x.getString(productSeg3No)
        var yearweek = x.getString(yearweekNo)
        var year = x.getString(yearNo)
        var week = x.getString(weekNo)
        var qty = x.getString(qtyNo)
        var map_price = x.getDouble(map_priceNo)
        var ir = x.getDouble(irNo)
        var pmap = x.getDouble(pmapNo)
        var pmap10 = x.getDouble(pmap10No)
        var pro_percent = x.getDouble(pro_percentNo)
        var promotionYN = x.getString(promotionYNNo)
        var seasonality = x.getDouble(seasonalityNo)

        Row(regionSeg1,
          productSeg1,
          productSeg2,
          regionSeg2,
          regionSeg3,
          productSeg3,
          yearweek,
          year,
          week,
          qty,
          map_price,
          ir,
          pmap,
          pmap10,
          pro_percent,
          promotionYN,
          seasonality,
          0d,
          0d,
          0d)

      })

      var result = fillData ++ insertData

      result

    })

    forecastActualSales.foreach(println)

    var forecastActualSalesMap = forecastActualSales.map(x=>{
      (x._1,x._2,x._3,x._4,x._5,x._6,x._7,x._8,x._9,x._10,x._11,x._12,x._13,x._14,x._15,x._16,x._17,x._18,x._19,x._20)
    })



    ////////////////////////////////////////////////////////////////////////////산업특화로직///////////////////////////////////////////////////////////////

//    var testFile = "gd.csv"
//    // 절대경로 입력
//    var AllDf =
//      spark.read.format("csv").
//        option("header", "true").
//        option("Delimiter", ",").
//        load("C:/spark_orgin_2.2.0/bin/data/" + testFile)
//
//    var movingColums = AllDf.columns.map(x => {x.toLowerCase()})
//    var regionidno = movingColums.indexOf("regionseg1")
//    var productno1 = movingColums.indexOf("productseg1")
//    var productno2 = movingColums.indexOf("productseg2")
//    var regionidno2 = movingColums.indexOf("regionseg2")
//    var regionidno3 = movingColums.indexOf("regionseg3")
//    var productno3 = movingColums.indexOf("productseg3")
//    var yearweekno = movingColums.indexOf("yearweek")
//    var yearno = movingColums.indexOf("year")
//    var weekno = movingColums.indexOf("week")
//    var qtyno = movingColums.indexOf("qty")
//    var map_priceno = movingColums.indexOf("map_price")
//    var irno = movingColums.indexOf("ir")
//    var pmapno = movingColums.indexOf("pmap")
//    var pmap10no = movingColums.indexOf("pamp10")
//    var pro_percentno = movingColums.indexOf("pro_percent")
//    var promotionCheck = movingColums.indexOf("promotionyn")
//    var movingQty = movingColums.indexOf("seasonality")
//
//    var testRdd = AllDf.rdd
//
//    var testFilterd = testRdd.filter(x=>{
//      var check = false
//      if(x.getString(yearweekno).toInt > 201527){
//        check = true
//      }
//      check
//    })
//
//    var AllMap = testFilterd.groupBy(x=>{
//      (x.getString(regionidno),x.getString(productno2),x.getString(regionidno3))
//    }).flatMap(x=>{
//      var key = x._1
//      var data = x._2
//      var result = data.map(x=>{
//        (x.getString(regionidno),
//          x.getString(productno1),
//          x.getString(productno2),
//          x.getString(regionidno2),
//          x.getString(regionidno3),
//          x.getString(productno3),
//          x.getString(yearweekno),
//          x.getString(yearno),
//          x.getString(weekno),
//          x.getString(qtyno),
//          x.getString(map_priceno),
//          x.getString(irno),
//          x.getString(pmapno),
//          x.getString(pmap10no),
//          x.getString(pro_percentno),
//          x.getString(promotionCheck),
//          x.getString(movingQty))
//      })
//      result
//    })
//
//    var sortedDf = AllMap.toDF("REGIONSEG1", "PRODUCTSEG1" ,"PRODUCTSEG2", "REGIONSEG2", "REGIONSEG3", "PRODUCTSEG3", "YEARWEEK", "YEAR", "WEEK","QTY","MAP_PRICE", "IR", "PMAP", "PMAP10", "PRO_PERCENT", "PROMOTIONYN","SEASONALITY")
//
//
//    import org.apache.spark.ml.Pipeline
//    import org.apache.spark.ml.evaluation.RegressionEvaluator
//    import org.apache.spark.ml.feature.VectorAssembler
//    import org.apache.spark.ml.regression.{DecisionTreeRegressionModel, DecisionTreeRegressor}
//    import org.apache.spark.ml.feature.{StringIndexer}
//    //머신러닝
//    var dtInput = sortedDf.withColumn("PRO_PERCENT",$"PRO_PERCENT".cast("Double")).
//      withColumn("SEASONALITY",$"SEASONALITY".cast("Double")).
//      withColumn("QTY",$"QTY".cast("Double"))
//
//    //기간설정
//    var targetWeek = "201623"
//    var trainData = dtInput.filter($"YEARWEEK" <= targetWeek)
//    var testData = dtInput.filter($"YEARWEEK" > targetWeek)
//
//    var promotionIndexer = new StringIndexer().setInputCol("PROMOTIONYN").setOutputCol("PROMOTION_CHECK_IN")
//
//    //훈련
//    var assembler = new  VectorAssembler().setInputCols(Array("PROMOTION_CHECK_IN","PRO_PERCENT","SEASONALITY")).setOutputCol("FEATURES")
//    //    "SEASONALITY"
//
//    var dt = new DecisionTreeRegressor().setLabelCol("QTY").setFeaturesCol("FEATURES")
//    var pipeline = new Pipeline().setStages(Array(promotionIndexer,assembler,dt))
//
//    var model = pipeline.fit(trainData)
//
//    val predictions = model.transform(testData)
//
//    predictions.select("REGIONSEG1", "PRODUCTSEG1" ,"PRODUCTSEG2", "REGIONSEG2", "REGIONSEG3", "PRODUCTSEG3", "YEARWEEK", "YEAR", "WEEK","QTY","MAP_PRICE", "IR", "PMAP", "PMAP10", "PRO_PERCENT", "PROMOTIONYN","SEASONALITY","FEATURES","PREDICTION").show
//
//
//
//    var resultOut =predictions.withColumn("PRO_PERCENT",$"PRO_PERCENT".cast("String")).
//      withColumn("SEASONALITY",$"SEASONALITY".cast("String")).
//      withColumn("PROMOTIONYN",$"PROMOTIONYN".cast("String")).
//      withColumn("QTY",$"QTY".cast("String")).withColumn("FEATURES",$"FEATURES".cast("String"))
//
//    resultOut.dtypes
//
//    resultOut.
//      coalesce(1). // 파일개수
//      write.format("csv"). // 저장포맷
//      mode("overwrite"). // 저장모드 append/overwrite
//      option("header", "true"). // 헤더 유/무
//      save("c:/spark/bin/data/OneYear.csv") // 저장파일명
//
//    val evaluatorRmse = new RegressionEvaluator().setLabelCol("QTY").setPredictionCol("prediction").setMetricName("rmse")
//
//    val evauatorMae = new RegressionEvaluator().setLabelCol("QTY").setPredictionCol("prediction").setMetricName("mae")
//
//    val rmse = evaluatorRmse.evaluate(predictions)
//    val mae = evauatorMae.evaluate(predictions)
//

//    var testFile = "finalData.csv"
//    // 절대경로 입력
//    var AllDf =
//      spark.read.format("csv").
//        option("header", "true").
//        option("Delimiter", ",").
//        load("C:/spark_orgin_2.2.0/bin/data/" + testFile)
//
//    var sortedDf = AllDf.toDF("REGIONSEG1", "PRODUCTSEG1" ,"PRODUCTSEG2", "REGIONSEG2", "REGIONSEG3", "PRODUCTSEG3", "YEARWEEK", "YEAR", "WEEK","QTY","MAP_PRICE", "IR", "PMAP", "PMAP10", "PRO_PERCENT", "PROMOTIONYN","SEASONALITY","FORECAST","WEEKSEASONALITY","FORECASETIMESERIES")
//
//
//    import org.apache.spark.ml.Pipeline
//    import org.apache.spark.ml.evaluation.RegressionEvaluator
//    import org.apache.spark.ml.feature.VectorAssembler
//    import org.apache.spark.ml.regression.{DecisionTreeRegressionModel, DecisionTreeRegressor}
//    import org.apache.spark.ml.feature.{StringIndexer}
//    //머신러닝
//    var dtInput = sortedDf.withColumn("PRO_PERCENT",$"PRO_PERCENT".cast("Double")).
//      withColumn("SEASONALITY",$"SEASONALITY".cast("Double")).
//      withColumn("QTY",$"QTY".cast("Double")).withColumn("FORECAST",$"FORECAST".cast("Double")).withColumn("WEEKSEASONALITY",$"WEEKSEASONALITY".cast("Double"))
//
//    //기간설정
//    var targetWeek = "201627"
//    var trainData = dtInput.filter($"YEARWEEK" <= targetWeek)
//    var testData = dtInput.filter($"YEARWEEK" > targetWeek)
//
//    var promotionIndexer = new StringIndexer().setInputCol("PROMOTIONYN").setOutputCol("PROMOTION_CHECK_IN")
//
//    //훈련
//    var assembler = new  VectorAssembler().setInputCols(Array("PROMOTION_CHECK_IN","PRO_PERCENT","SEASONALITY")).setOutputCol("FEATURES")
////    "SEASONALITY"
//
//    var dt = new DecisionTreeRegressor().setLabelCol("QTY").setFeaturesCol("FEATURES")
//    var pipeline = new Pipeline().setStages(Array(promotionIndexer,assembler,dt))
//
//    var model = pipeline.fit(trainData)
//
//    val predictions = model.transform(testData)
//
//    predictions.select("REGIONSEG1", "PRODUCTSEG1" ,"PRODUCTSEG2", "REGIONSEG2", "REGIONSEG3", "PRODUCTSEG3", "YEARWEEK", "YEAR", "WEEK","QTY","MAP_PRICE", "IR", "PMAP", "PMAP10", "PRO_PERCENT", "PROMOTIONYN","SEASONALITY","FORECAST","WEEKSEASONALITY","FORECASETIMESERIES","FEATURES","PREDICTION").show
//
//
//
//    var resultOut =predictions.withColumn("PRO_PERCENT",$"PRO_PERCENT".cast("String")).
//      withColumn("SEASONALITY",$"SEASONALITY".cast("String")).
//      withColumn("PROMOTIONYN",$"PROMOTIONYN".cast("String")).
//      withColumn("QTY",$"QTY".cast("String")).
//      withColumn("FORECAST",$"FORECAST".cast("String")).withColumn("FEATURES",$"FEATURES".cast("String"))
//
//    resultOut.dtypes
//
//    resultOut.
//      coalesce(1). // 파일개수
//      write.format("csv"). // 저장포맷
//      mode("overwrite"). // 저장모드 append/overwrite
//      option("header", "true"). // 헤더 유/무
//      save("c:/spark/bin/data/TestData.csv") // 저장파일명
//
//    val evaluatorRmse = new RegressionEvaluator().setLabelCol("QTY").setPredictionCol("prediction").setMetricName("rmse")
//
//    val evauatorMae = new RegressionEvaluator().setLabelCol("QTY").setPredictionCol("prediction").setMetricName("mae")
//
//    val rmse = evaluatorRmse.evaluate(predictions)
//    val mae = evauatorMae.evaluate(predictions)


//    비지도학습
//
//    var kmeansInput =sortedDf.withColumn("PRO_PERCENT",$"PRO_PERCENT".cast("Double")).
//      withColumn("SEASONALITY",$"SEASONALITY".cast("Double")).
//      withColumn("QTY",$"QTY".cast("Double")).withColumn("FORECAST",$"FORECAST".cast("Double"))
//    kmeansInput.dtypes
//
//    //k-means Clustering 라이브러리 추가
//    import org.apache.spark.ml.feature.VectorAssembler
//    import org.apache.spark.ml.Pipeline
//    import org.apache.spark.ml.clustering.KMeans
//
//    //훈련
//    var promotionIndexer = new StringIndexer().setInputCol("PROMOTIONYN").setOutputCol("PROMOTION_CHECK_IN")
//    var assembler = new VectorAssembler().setInputCols(Array("QTY","PRO_PERCENT","PROMOTION_CHECK_IN","SEASONALITY")).setOutputCol("FEATURES")
//    var kValue = 2
//    val kmeans = new KMeans().setK(kValue).setFeaturesCol("FEATURES").setPredictionCol("PREDICTION")
//    val pipeline = new Pipeline().setStages(Array(promotionIndexer,assembler,kmeans))
//    val kMeansPredictionModel = pipeline.fit(kmeansInput)
//
//    //예측
//    val predictionResult = kMeansPredictionModel.transform(kmeansInput)
//    print(predictionResult.show(10))
//
//    var beresultOut =predictionResult.withColumn("PRO_PERCENT",$"PRO_PERCENT".cast("String")).
//      withColumn("SEASONALITY",$"SEASONALITY".cast("String")).
//      withColumn("PROMOTIONYN",$"PROMOTIONYN".cast("String")).
//      withColumn("QTY",$"QTY".cast("String")).
//      withColumn("FORECAST",$"FORECAST".cast("String")).withColumn("FEATURES",$"FEATURES".cast("String"))
//
//    beresultOut.dtypes
//
//    beresultOut.
//      coalesce(1). // 파일개수
//      write.format("csv"). // 저장포맷
//      mode("overwrite"). // 저장모드 append/overwrite
//      option("header", "true"). // 헤더 유/무
//      save("c:/spark/bin/data/result.csv") // 저장파일명

  }
}

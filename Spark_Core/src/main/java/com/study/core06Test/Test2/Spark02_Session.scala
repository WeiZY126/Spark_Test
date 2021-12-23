package com.study.core06Test.Test2

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_Session {
  def main(args: Array[String]): Unit = {
    //TODO-Top10热门品类
    val sparkConf: SparkConf = new SparkConf().setMaster("local[3]").setAppName("Top10")
    val sc = new SparkContext(sparkConf)

    val actionRDD: RDD[String] = sc.textFile("datas/user_visit_action.txt")
    actionRDD.cache()
    val top10Ids: Array[String] = top10Category(actionRDD)

    //1.过滤原始数据,保留点击和前十
    val filterRDD: RDD[String] = actionRDD.filter(action => {
      val dates: Array[String] = action.split("_")
      if (dates(6) != "-1") {
        top10Ids.contains(dates(6))
      } else {
        false
      }
    })

    //2.根据品类id和sessionId进行点击量的统计
    val reduceRDD: RDD[((String, String), Int)] = filterRDD.map(action => {
      val dates: Array[String] = action.split("_")
      ((dates(6), dates(2)), 1)
    }).reduceByKey(_ + _)

    //3.将统计的结果进行结构的转换
    //((品类ID,SessionID),sum)=>(品类ID,(SessionID,sum))
    val mapRDD: RDD[(String, (String, Int))] = reduceRDD.map {
      case ((cid, sid), sum) => (cid, (sid, sum))
    }
    //4.根据相同品类分组
    val groupRDD: RDD[(String, Iterable[(String, Int)])] = mapRDD.groupByKey()
    //5.将分组后的数据进行点击排名，取前十
    val resultRDD: RDD[(String, List[(String, Int)])] = groupRDD.mapValues(iter => {
      iter.toList.sortBy(_._2)(Ordering[Int].reverse).take(10)
    })
    resultRDD.collect().foreach(println)
    sc.stop()
  }

  def top10Category(actionRDD: RDD[String]): Array[String] = {
    val flatRDD: RDD[(String, (Int, Int, Int))] = actionRDD.flatMap(action => {
      val datas: Array[String] = action.split("_")
      if (datas(6) != "-1")
        List((datas(6), (1, 0, 0)))
      else if (datas(8) != "null") {
        val ids: Array[String] = datas(8).split(",")
        ids.map(id => (id, (0, 1, 0)))
      }
      else if (datas(10) != "null") {
        val ids: Array[String] = datas(10).split(",")
        ids.map(id => (id, (0, 0, 1)))
      }
      else
        Nil
    })

    val analysisRDD: RDD[(String, (Int, Int, Int))] = flatRDD.reduceByKey((t1, t2) => (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3))

    analysisRDD.sortBy(_._2, false).take(10).map(_._1)
  }

}

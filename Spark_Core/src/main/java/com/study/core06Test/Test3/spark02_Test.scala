package com.study.core06Test.Test3

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.text.SimpleDateFormat
import java.util.Date
import scala.Console.println

object spark02_Test {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[3]").setAppName("test1")
    val sc = new SparkContext(sparkConf)

    //TODO-单跳转化率
    val actionRDD: RDD[String] = sc.textFile("datas/user_visit_action.txt")
    actionRDD.cache()
    val actionDataRDD: RDD[UserVisitAction] = actionRDD.map(action => {
      val datas: Array[String] = action.split("_")
      UserVisitAction(datas(0), datas(1).toLong, datas(2), datas(3).toLong, datas(4), datas(5), datas(6).toLong, datas(7).toLong, datas(8), datas(9), datas(10), datas(11), datas(12).toLong)
    })
    //TODO-计算分母
    val pageIdCountMap: Map[Long, Int] = actionDataRDD.map(action => {
      (action.page_id, 1)
    }).reduceByKey(_ + _).collect().toMap
    //TODO-计算分子
    val sortRDD: RDD[(String, List[UserVisitAction])] = actionDataRDD.groupBy(_.session_id).mapValues(_.toList.sortBy(_.action_time))
    val slidingRDD: RDD[Iterator[List[UserVisitAction]]] = sortRDD.map(_._2.sliding(2))
    val jumpRDD: RDD[((Long, Long), Int)] = slidingRDD.flatMap(iter => {
      iter.map(list => if (list.size > 1) ((list(0).page_id, list(1).page_id), 1) else null)
    }).filter(_ != null).reduceByKey(_ + _)
    //TODO-计算转换率
    jumpRDD.foreach(pageJump => {
      println(s"页面${pageJump._1._1}到${pageJump._1._2}的跳转率为${pageJump._2.toDouble / pageIdCountMap.get(pageJump._1._1).get.toDouble}")
    })
    sc.stop()
  }

  //用户访问动作表
  case class UserVisitAction(
                              date: String, //用户点击行为的日期
                              user_id: Long, //用户的 ID
                              session_id: String, //Session 的 ID
                              page_id: Long, //某个页面的 ID
                              action_time: String, //动作的时间点
                              search_keyword: String, //用户搜索的关键词
                              click_category_id: Long, //某一个商品品类的 ID
                              click_product_id: Long, //某一个商品的 ID
                              order_category_ids: String, //一次订单中所有品类的 ID 集合
                              order_product_ids: String, //一次订单中所有商品的 ID 集合
                              pay_category_ids: String, //一次支付中所有品类的 ID 集合
                              pay_product_ids: String, //一次支付中所有商品的 ID 集合
                              city_id: Long //城市 id
                            )

}

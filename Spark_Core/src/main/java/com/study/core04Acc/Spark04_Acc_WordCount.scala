package com.study.core04Acc

import org.apache.spark.rdd.RDD
import org.apache.spark.util.{AccumulatorV2, LongAccumulator}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Spark04_Acc_WordCount {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[String] = sc.makeRDD(List("Hello", "Scala", "spark"))

    //累加器:WordCount
    //创建累加器对象
    val wcAcc = new MyAccumulator
    //向park进行注册
    sc.register(wcAcc, "wordCountAcc")
    new LongAccumulator
    rdd.foreach(word => {
      //数据的累加
      wcAcc.add(word)
    })
    println(wcAcc.value)
    sc.stop()
  }

  /**
   * 自定义累加器
   * 1.继承AccumulatorV2，定义泛型
   * IN:累加器输入的数据类型
   * OUT:累加器返回的数据类型
   * 2.重写方法
   */
  class MyAccumulator extends AccumulatorV2[String, mutable.Map[String, Long]] {
    private var wcMap = mutable.Map[String, Long]()

    //判断是否为初始状态
    override def isZero: Boolean = wcMap.isEmpty

    override def copy(): AccumulatorV2[String, mutable.Map[String, Long]] = {
      new MyAccumulator
    }

    //重置
    override def reset(): Unit = {
      wcMap.clear()
    }

    //获取累加器需要计算的值
    override def add(word: String): Unit = {
      val newCount: Long = wcMap.getOrElse(word, 0L) + 1
      wcMap.update(word, newCount)
    }

    //Driver合并多个累加器
    override def merge(other: AccumulatorV2[String, mutable.Map[String, Long]]): Unit = {
      val map1: mutable.Map[String, Long] = this.wcMap
      val map2: mutable.Map[String, Long] = other.value
      map2.foreach {
        case (word, count) => {
          val newCount: Long = map1.getOrElse(word, 0L) + count
          map1.update(word, count)
        }
      }
    }

    //累加器结果
    override def value: mutable.Map[String, Long] = {
      wcMap
    }
  }
}

package com.study.Core07Framework.common

import com.study.Core07Framework.util.EnvUtil
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

trait TDao {
  def readFile(path:String)={
    val sc: SparkContext = EnvUtil.take()
    val lines: RDD[String] = sc.textFile(path)
    lines
  }
}

package com.study.Core07Framework.common

import com.study.Core07Framework.controller.WordCountController
import com.study.Core07Framework.util.EnvUtil
import org.apache.spark.{SparkConf, SparkContext}

trait TApplication {
  def start(master: String = "local[*]", app: String = "Application")(op: => Unit) = {
    //TODO 建立和spark框架的链接
    val sparkConf = new SparkConf().setMaster(master).setAppName(app)
    val sc = new SparkContext(sparkConf)
    EnvUtil.put(sc)
    try {
      op
    } catch {
      case ex => println(ex.getMessage)
    }
    //TODO 关闭链接
    sc.stop()
    EnvUtil.clear()
  }

}

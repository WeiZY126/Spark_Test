package com.study.Core07Framework.util

import org.apache.spark.SparkContext

object EnvUtil {
  private val scLocal: ThreadLocal[SparkContext] = new ThreadLocal[SparkContext]()

  def put(sc: SparkContext) = {
    scLocal.set(sc)
  }

  def take() = {
    scLocal.get()
  }

  def clear() = {
    scLocal.remove()
  }

}

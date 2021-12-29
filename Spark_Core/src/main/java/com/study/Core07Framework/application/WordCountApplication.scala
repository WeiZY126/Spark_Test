package com.study.Core07Framework.application

import com.study.Core07Framework.common.TApplication
import com.study.Core07Framework.controller.WordCountController
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object WordCountApplication extends App with TApplication{
  start(){
    val wordCountController: WordCountController = new WordCountController
    wordCountController.dispatch()
  }

}

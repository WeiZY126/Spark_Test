package com.study.Core07Framework.controller

import com.study.Core07Framework.common.TController
import com.study.Core07Framework.service.WordCountService

/*
控制层
 */
class WordCountController extends TController{
  private val wordCountService: WordCountService = new WordCountService
  //调度
  override def dispatch() = {
    val array: Array[(String, Int)] = wordCountService.dataAnalysis()
    array.foreach(println)
  }
}

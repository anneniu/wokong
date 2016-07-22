package com.kumyan.companyinfo.parser

import org.jsoup.Jsoup
import net.minidev.json.JSONObject

/**
  * Created by niujiaojiao on 2016/7/21.
  */
//股本结构
//四部分：限售解禁，股本结构，历年股本变动，股本构成

object CapitalStructure {

  /**
    * 股本结构模块入口
    * @param stockCode 股票代码字符串
    * @return  整个部分的综合json 字符串
    */
  def parseCapitalStructure(stockCode: String): String = {

    var json = ""

    val map = new java.util.HashMap[String, Object]()

    if (stockCode.isEmpty) {
      ""
    } else {

      val doc = Jsoup.connect("http://f10.eastmoney.com/f10_v2/CapitalStockStructure.aspx?code=" + stockCode).get()

      //限售解禁
      val tableTop = doc.getElementById("xsjj")
        .nextElementSibling()
        .select("tbody").first()
        .getElementsByTag("tr")

      //股本结构
      val partTwoTableOne = doc.getElementById("gbjg_div_bg")
        .select("tbody").first().getElementsByTag("tr")

      val partTwoTableTwo = doc.getElementById("gbjg_div_bg")
        .select("tbody").get(1).getElementsByTag("tr")

      //历年股本变动
      val partThree = doc.getElementById("lngbbd_Table")
        .select("tbody").first().getElementsByTag("tr")

      //股本构成
      val partFour = doc.getElementById("gbgc")
        .nextElementSibling()
        .select("tbody").first().getElementsByTag("tr")

      val mapOne = CpnyExecutives.parseSingleTable(tableTop, 0)

      val mapTwoTableOne = CpnyExecutives.parseSingleTable(partTwoTableOne, 0)

      val mapTwoTableTwo = CpnyExecutives.parseSingleTable(partTwoTableTwo, 0)

      val mapPartThree = CpnyExecutives.parseSingleTable(partThree, 1)

      val mapPartFour = CpnyExecutives.parseSingleTable(partFour, 1)

      map.put("限售解禁", mapOne)

      map.put("股本结构", new java.util.HashMap[String, Object]() {
        {
          put("1", mapTwoTableOne)
          put("2", mapTwoTableTwo)
        }
      })

      map.put("历年股本变动", mapPartThree)

      map.put("股本构成", mapPartFour)

      json = JSONObject.toJSONString(map)
    }

    json

  }

}

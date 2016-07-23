package com.kumyan.companyinfo.parser

import org.jsoup.Jsoup
import net.minidev.json.JSONObject
import org.jsoup.select.Elements

/**
  * Created by niujiaojiao on 2016/7/21.
  */
//股本结构
//四部分：限售解禁，股本结构，历年股本变动，股本构成

object CapitalStructure {

  /**
    * 股本结构模块入口
    *
    * @param stockCode 股票代码字符串
    * @return 整个部分的综合json 字符串
    */
  def parseCapitalStructure(stockCode: String): String = {

    var json = ""

    val map = new java.util.HashMap[String, Object]()

    if (stockCode.isEmpty) {
      ""
    } else {

      val doc = Jsoup.connect("http://f10.eastmoney.com/f10_v2/CapitalStockStructure.aspx?code=" + stockCode).timeout(6000).get()

      try {

        //限售解禁
        var tableTop = doc.getElementById("xsjj").nextElementSibling().select("tbody")

        if (tableTop.toString.nonEmpty) {

          tableTop = tableTop.first().getElementsByTag("tr")

        } else {

          tableTop = null
        }

        //股本结构
        var partTwoTableOne = new Elements()

        if (doc.getElementById("gbjg_div_bg")
          .select("tbody").toString.nonEmpty) {

          partTwoTableOne = doc.getElementById("gbjg_div_bg")
            .select("tbody").first().getElementsByTag("tr")

        } else {

          partTwoTableOne = null
        }

        var partTwoTableTwo = new Elements()

        if (doc.getElementById("gbjg_div_bg")
          .select("tbody").get(1).toString.nonEmpty) {

          partTwoTableTwo = doc.getElementById("gbjg_div_bg")
            .select("tbody").get(1).getElementsByTag("tr")

        } else {

          partTwoTableTwo = null
        }

        //历年股本变动
        var partThree = new Elements()

        if (doc.toString.contains("lngbbd_Table")) {

          partThree = doc.getElementById("lngbbd_Table")
            .select("tbody").first().getElementsByTag("tr")

        } else {

          partThree = null
        }

        //股本构成
        var partFour = new Elements()

        if (doc.getElementById("gbgc")
          .nextElementSibling()
          .select("tbody").toString.nonEmpty) {

          partFour = doc.getElementById("gbgc")
            .nextElementSibling()
            .select("tbody").first().getElementsByTag("tr")

        } else {

          partFour = null
        }


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
      catch {
        case e: Exception =>
          e.printStackTrace()

      }
    }

    json

  }

}

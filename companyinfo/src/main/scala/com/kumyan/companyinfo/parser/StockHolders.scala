package com.kumyan.companyinfo.parser


import org.jsoup.select.Elements
import net.minidev.json.JSONObject
import org.jsoup.Jsoup


/**
  * Created by zhangruibo on 2016/7/21.
  */
object StockHolders extends App {

  def parseStockHolders(stockCode: String): String = {

    var json = ""

    val map = new java.util.HashMap[String, Object]()

    if (stockCode.isEmpty) {
      ""
    } else {

      val doc = Jsoup.connect("http://f10.eastmoney.com/f10_v2/ShareholderResearch.aspx?code=" + stockCode).get()

      //十大流通股东
      val tableTop = doc.getElementById("TTCS_Table_Div")
        .getElementsByTag("table").select("tbody")

      val partOneDates = doc.getElementById("sdltgd").nextElementSibling().getElementsByTag("li")

      assert(tableTop.size == partOneDates.size)

      //十大股东
      val tableBot = doc.getElementById("TTS_Table_Div")
        .getElementsByTag("table").select("tbody")

      val partTwoDates = doc.getElementById("sdgd").nextElementSibling().getElementsByTag("li")

      assert(tableBot.size == partTwoDates.size)

      val mapTop = getJson(tableTop, partOneDates)

      val mapBot = getJson(tableBot, partTwoDates)

      map.put("十大流通股东", mapTop)

      map.put("十大股东", mapBot)

      json = JSONObject.toJSONString(map)
    }

    json

  }


  def getJson(children: Elements, allDates: Elements): java.util.HashMap[String, Object] = {

    val map = new java.util.HashMap[String, Object]()

    for (i <- 0 until children.size) {

      val tr = children.get(i).select("tr")

      val thkeys = tr.first().getElementsByTag("th")

      val mapOut = new java.util.HashMap[String, Object]()

      for (j <- 1 until tr.size) {

        val subvalue = tr.get(j).getElementsByTag("th")

        val values = tr.get(j).getElementsByTag("td")

        val mapIn = new java.util.HashMap[String, Object]()

        for (k <- 0 until values.size) {

          if (k == 0) {
            mapIn.put(thkeys.get(0).text(), subvalue.text)
          } else {
            mapIn.put(thkeys.get(k + 1).text(), values.get(k).text)
          }

        }

        mapOut.put(j.toString, mapIn)
      }

      map.put(allDates.get(i).text, mapOut)

    }

    map

  }

}

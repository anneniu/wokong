package com.kumyan.companyinfo.parser

import org.jsoup.Jsoup
import org.jsoup.select.Elements
import net.minidev.json.JSONObject

/**
  * Created by niujiaojiao on 2016/7/21.
  */
//两部分：基本资料,发行相关
object CpnyInstructure {

  /**
    * 公司概况解析入口
    *
    * @param stockCode 公司股票代码
    * @return 返回总体的json字符串
    */
  def parseCpnyInstructure(stockCode: String): String = {

    var json = ""

    var map = new java.util.HashMap[String, Object]()

    if (stockCode.isEmpty) {
      ""
    } else {

      val doc = Jsoup.connect("http://f10.eastmoney.com/f10_v2/CompanySurvey.aspx?code=" + stockCode).get()

      //基本资料
      val tableTop = doc.select("table#Table0 tbody")

      //发行相关
      val tableBot = doc.getElementById("fxxg").nextElementSibling().getElementsByTag("table").select("tbody")

      val mapTop = getJson(tableTop)

      val mapBot = getJson(tableBot)

      map.put("基本资料", mapTop)

      map.put("发行相关", mapBot)

      json = JSONObject.toJSONString(map)
    }

    json

  }


  /**
    * 传入table tbody一个对象
    *
    * @param children table tbody 对象（键值一一对应的表格）
    * @return
    */
  def getJson(children: Elements): java.util.HashMap[String, Object] = {

    var json = ""

    var map = new java.util.HashMap[String, Object]()

    val keys = children.first().getElementsByTag("th")

    val values = children.first().getElementsByTag("td")

    assert(keys.size == values.size)

    for (i <- 0 until keys.size) {

      map.put(keys.get(i).text, values.get(i).text)
    }

    map

  }

}

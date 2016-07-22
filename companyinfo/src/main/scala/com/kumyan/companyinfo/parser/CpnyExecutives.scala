package com.kumyan.companyinfo.parser

import org.jsoup.Jsoup
import org.jsoup.select.Elements
import net.minidev.json.JSONObject

/**
  * Created by niujiaojiao on 2016/7/21.
  */
//高管列表，高管持股变动，管理层简介
object CpnyExecutives {

  /**
    * 公司高管模块解析入口
    *
    * @param stockCode 股票代码
    * @return
    */
  def parseCpnyExecutives(stockCode: String): String = {

    var json = ""

    val map = new java.util.HashMap[String, Object]()

    if (stockCode.isEmpty) {
      ""
    } else {

      val doc = Jsoup.connect("http://f10.eastmoney.com/f10_v2/CompanyManagement.aspx?code=" + stockCode).get()

      //高管列表
      val tableTop = doc.select("table#gglb_table tbody").first.getElementsByTag("tr")

      //高管持股变动
      val tableMid = doc.getElementById("cgbd")
        .nextElementSibling().getElementsByTag("table").select("tbody").first().getElementsByTag("tr")

      //管理层简介

      val tableBot = doc.getElementById("glcjj").nextElementSibling()
        .getElementsByTag("table")

      val mapTop = parseSingleTable(tableTop, 0)

      val mapMid = parseSingleTable(tableMid, 0)

      val mapBot = parseMultiTables(tableBot)

      map.put("高管列表", mapTop)

      map.put("高管持股变动", mapMid)

      map.put("管理层简介", mapBot)

      json = JSONObject.toJSONString(map)
    }

    json

  }

  /**
    * 管理层简介
    *
    * @param children 多个Table对象
    * @return map 信息集合
    */
  def parseMultiTables(children: Elements): java.util.HashMap[String, Object] = {

    val mapOut = new java.util.HashMap[String, Object]()

    for (i <- 0 until children.size) {

      val list = children.get(i)

      val tagTd = list.getElementsByTag("td")

      val mapIn = new java.util.HashMap[String, Object]()

      val nameText = tagTd.get(0).text

      val name = nameText.substring(1, nameText.length).trim()

      for (j <- 1 until tagTd.size) {

        val total = tagTd.get(j).text()

        if (total.startsWith(name)) {

          val key = name
          val value = tagTd.get(j).text()
          mapIn.put(key, value)

        } else {

          val key = total.split(":")(0)
          val value = total.split(":")(1)
          mapIn.put(key, value)

        }

      }

      mapOut.put(name, mapIn)

    }

    mapOut

  }

  /**
    * 高管列表的json字符串信息提取
    *
    * @param children 提取到tr 标签的table信息（行列都有作为键值的表格）
    * @param index    赋值 1 : 作为map 键值的key 中第一个是th标签
    *                 <tr>   <tr class="highlightbg"><th class="tips-fieldnameL">总股本</th><td class="tips-dataR">8,347.00</td><td class="tips-dataR">8,347.00</td><td class="tips-dataR">6,260.00</td></tr><tr>
    * @return
    */
  def parseSingleTable(children: Elements, index: Int): java.util.HashMap[String, Object] = {

    val map = new java.util.HashMap[String, Object]()

    val rowkeys = children.first().getElementsByTag("th")

    for (i <- 1 until children.size) {

      index match {

        case 0 => {

          val values = children.get(i).getElementsByTag("td")

          val mapIn = new java.util.HashMap[String, Object]()

          for (j <- 1 until values.size) {

            mapIn.put(rowkeys.get(j).text, values.get(j).text)

          }

          map.put(values.get(0).text, mapIn)

        }
        case 1 => {

          val th = children.get(i).getElementsByTag("th")
          val values = children.get(i).getElementsByTag("td")

          val mapIn = new java.util.HashMap[String, Object]()

          for (j <- 0 until values.size) {

            mapIn.put(rowkeys.get(j + 1).text, values.get(j).text)

          }

          map.put(th.text(), mapIn)
        }
        case _ => {
          println("Please give correct value!")
        }


      }

    }

    map

  }


}

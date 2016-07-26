package com.kunyan.companyinfo.parser

import scala.collection.mutable.ListBuffer
import scala.util.parsing.json.JSON

/**
  * Created by niujiaojiao  on 2016/7/24.
  * 股东研究
  */
object stockHolderSql {

  /**
    * 股东研究解析读取数据写入数据库
    *
    * @param totalJson 从hbase 表里读取的字符串json集合
    * @return
    */
  def parse(totalJson: String): (ListBuffer[ListBuffer[ListBuffer[String]]], ListBuffer[ListBuffer[ListBuffer[String]]]) = {

    var groupOne = new ListBuffer[ListBuffer[ListBuffer[String]]]()

    var groupTwo = new ListBuffer[ListBuffer[ListBuffer[String]]]()

    val jsonInfo = JSON.parseFull(totalJson)

    if (jsonInfo.isEmpty) {

      println("\"JSON parse value is empty,please have a check!\"")

    } else {

      jsonInfo match {

        case Some(mapInfo) => {

          val floatHolders = mapInfo.asInstanceOf[Map[String, AnyVal]].getOrElse("十大流通股东", "").asInstanceOf[Map[String, AnyVal]]

          groupOne = getHolders(floatHolders)

          val holders = mapInfo.asInstanceOf[Map[String, AnyVal]].getOrElse("十大股东", "").asInstanceOf[Map[String, AnyVal]]

          groupTwo = getHolders(holders)
        }
        case None => println("Parsing failed!")

        case other => println("Unknown data structure :" + other)
      }

    }

    (groupOne, groupTwo)
  }

  /**
    * 获取字符串集合
    *
    * @param mapJson map集合
    * @return 需要插入数据库的字符串集合
    */
  def getHolders(mapJson: Map[String, AnyVal]): ListBuffer[ListBuffer[ListBuffer[String]]] = {

    //first map key is date (serveral tabs) 日期
    //最外层的ListBuffer[] 存放所有日期的数据

    val dateKeys = mapJson.keys

    var outList = new ListBuffer[ListBuffer[ListBuffer[String]]]()

    dateKeys.foreach {
      //日期

      x => {

        val subJson = mapJson.getOrElse(x, "")

        if (subJson.toString == "") {
          return outList
        } else {

          val res = subJson.asInstanceOf[Map[String, AnyVal]]

          //中间的ListBuffer 是存放多行的数据，代表着一个日期的数据
          var midList = new ListBuffer[ListBuffer[String]]()

          val ids = res.keys

          ids.foreach {

            index => {

              //最里层的ListBuffer[] 得到一行的数据
              var inList = new ListBuffer[String]()
              //每个行的数据
              val values = res.getOrElse(index, "")

              inList += x.toString //在每一行数据之前先添加日期

              if (values.toString.nonEmpty) {

                val sub = {
                  values.asInstanceOf[Map[String, AnyVal]]
                }

                val keys = sub.keys

                keys.foreach {

                  y => {

                    var result = sub.getOrElse(y, "")

                    if (result.toString.startsWith("--")) {
                      result = ""
                    }

                    inList += result.toString

                  }

                }

              }

              midList += inList
            }

          }

          outList += midList

        }

      }

    }

    outList
  }

}

package com.kunyan.companyinfo.parser

import scala.collection.mutable.ListBuffer
import scala.util.parsing.json.JSON

/**
  * Created by niujiaojiao on 2016/7/24.
  * 股本结构模块
  */
object CapitalStructureSql {

  /**
    * 股本结构解析hbase表获取的字符串入口
    *
    * @param totalJson hbase 表获取的字符串
    * @return 将要插入数据库的数据集合：分为三个表
    */
  def parse(totalJson: String): (ListBuffer[ListBuffer[String]], ListBuffer[ListBuffer[String]]
    , ListBuffer[ListBuffer[String]]) = {

    var groupOne = new ListBuffer[ListBuffer[String]]()
    var groupTwo = new ListBuffer[ListBuffer[String]]()
    var groupThree = new ListBuffer[ListBuffer[String]]()
    val jsonInfo = JSON.parseFull(totalJson)

    if (jsonInfo.isEmpty) {

      println("\"JSON parse value is empty,please have a check!\"")

    } else {

      jsonInfo match {

        case Some(mapInfo) => {

          val partOne = mapInfo.asInstanceOf[Map[String, AnyVal]].getOrElse("股本结构", "").asInstanceOf[Map[String, AnyVal]]

          val subPartOne = partOne.getOrElse("1", "").asInstanceOf[Map[String, AnyVal]]
          val subPartTwo = partOne.getOrElse("2", "").asInstanceOf[Map[String, AnyVal]]
          groupOne = getTables(subPartOne, 0)
          groupTwo = getTables(subPartTwo, 0)

          val partThree = mapInfo.asInstanceOf[Map[String, AnyVal]].getOrElse("历年股本变动", "").asInstanceOf[Map[String, AnyVal]]

          groupThree = specialStuation(partThree)

        }

        case None => println("Parsing failed!")
        case other => println("Unknown data structure :" + other)

      }

    }

    (groupOne, groupTwo, groupThree)
  }

  /**
    *
    * @param mapJson    Map 集合键值对
    * @param identifier 如若数字中存在逗号，需要把逗号去除，得到整个的数字字符串
    * @return 所需要的插入数据库的数据
    */
  def getTables(mapJson: Map[String, AnyVal], identifier: Int): ListBuffer[ListBuffer[String]] = {

    var outList = new ListBuffer[ListBuffer[String]]()

    //最外层的键值
    val ids = mapJson.keys

    ids.foreach {

      index => {

        //最里层的ListBuffer[] 得到一行的数据
        var inList = new ListBuffer[String]()
        //每个行的数据
        val values = mapJson.getOrElse(index, "")
        inList += index.toString //在每一行数据之前先添加第一列的key值

        if (values.toString.nonEmpty) {

          val sub = values.asInstanceOf[Map[String, AnyVal]]
          val keys = sub.keys

          keys.foreach {

            y => {

              var result = sub.getOrElse(y, "")

              if (result.toString.startsWith("--")) {
                result = ""
              }

              if (identifier == 0) {

                if (result.toString.contains(",")) {
                  result = result.toString.replace(",", "")
                }

              }

              //对于result 为"--"的情况
              inList += result.toString

            }

          }

        }

        outList += inList

      }

    }

    outList
  }

  /**
    * 针对股本结构；历年股本变动这一模块的解析，hbase 表数据存储，取得的是表格一行，数据库设计的
    * 是需要该表格一列的数据，需要进行转换
    *
    * @param mapJson map 键值对集合
    * @return 所需要的表格中的每列数据的集合
    */
  def specialStuation(mapJson: Map[String, AnyVal]): ListBuffer[ListBuffer[String]] = {

    var outList = new ListBuffer[ListBuffer[String]]()

    val ids = mapJson.keys

    ids.foreach {

      index => {

        //最里层的ListBuffer[] 得到一行的数据
        var inList = new ListBuffer[String]()
        val values = mapJson.getOrElse(index, "")

        if (values.toString.nonEmpty) {

          val sub = values.asInstanceOf[Map[String, AnyVal]]
          val keys = sub.keys

          keys.foreach {

            y => {

              if (y.toString != "") {

                var result = sub.getOrElse(y, "")

                //也要加上第一行的key值： 针对历年股本变动

                if (result.toString.startsWith("--")) {
                  result = ""
                }

                if (result.toString.contains(",")) {
                  result = result.toString.replace(",", "")
                }

                //加上第一行第一列的元素Key
                inList += index.toString
                inList += y.toString
                inList += result.toString

              }

            }

          }

        }

        outList += inList

      }

    }

    var tranList = new ListBuffer[ListBuffer[String]]()

    if (outList.toString().nonEmpty) {

      if (outList.nonEmpty) {

        if (outList.head.nonEmpty) {

          val size = outList.head.size / 3
          var date = ""
          var rowkey = ""

          for (i <- 0 until size) {

            var tranInList = new ListBuffer[String]()

            for (j <- outList.indices) {

              date = outList(j)(i * 3 + 1)
              rowkey = outList(j).head
              val value = outList(j)(i * 3 + 2)
              tranInList += rowkey
              tranInList += value

            }

            tranInList += date
            tranList += tranInList

          }

        }

      }

    }

    tranList
  }

}

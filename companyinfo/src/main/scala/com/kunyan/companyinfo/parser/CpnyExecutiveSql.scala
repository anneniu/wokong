package com.kunyan.companyinfo.parser

import scala.collection.mutable.ListBuffer
import scala.util.parsing.json.JSON

/**
  * Created by niujiaojiao on 2016/7/24.
  * 公司高管
  */
object CpnyExecutiveSql {

  def parse(totalJson: String): (ListBuffer[ListBuffer[String]], ListBuffer[ListBuffer[String]]) = {

    var groupOne = new ListBuffer[ListBuffer[String]]()
    var groupTwo = new ListBuffer[ListBuffer[String]]()
    var jsonInfo = JSON.parseFull(totalJson)

    if (jsonInfo.isEmpty) {

      println("\"JSON parse value is empty,please have a check!\"")

    } else {

      jsonInfo match {

        case Some(mapInfo) => {

          val partOne = mapInfo.asInstanceOf[Map[String, AnyVal]].getOrElse("高管列表", "").asInstanceOf[Map[String, AnyVal]]
          groupOne = CapitalStructureSql.getTables(partOne, 1)
          val partTwo = mapInfo.asInstanceOf[Map[String, AnyVal]].getOrElse("管理层简介", "").asInstanceOf[Map[String, AnyVal]]
          groupTwo = getResult(partTwo)

        }
        case None => println("Parsing failed!")
        case other => println("Unknown data structure :" + other)

      }

    }

    (groupOne, groupTwo)
  }

  def getResult(mapJson: Map[String, AnyVal]): ListBuffer[ListBuffer[String]] = {

    var outList = new ListBuffer[ListBuffer[String]]()
    val ids = mapJson.keys

    ids.foreach {

      index => {

        //最里层的ListBuffer[] 得到一行的数据
        var inList = new ListBuffer[String]()
        //每个行的数据
        val values = mapJson.getOrElse(index, "")
        inList += index.toString //在每一行数据之前先添加第一列的key值(name)

        if (values.toString.nonEmpty) {

          val sub = values.asInstanceOf[Map[String, AnyVal]]
          val keys = sub.keys
          inList += sub.getOrElse("性别", "").toString
          inList += sub.getOrElse("学历", "").toString
          inList += sub.getOrElse("职务", "").toString

          keys.foreach {

            y => {
              if (y.toString != "性别" && y.toString != "年龄" && y.toString != "学历" && y.toString != "职务" && y.toString != "任职时间")
                inList += sub.getOrElse(y, "").toString
            }

          }

        }

        outList += inList
      }

    }

    outList
  }

}

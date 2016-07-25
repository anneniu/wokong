package com.kumyan.companyinfo.parser

import scala.collection.mutable.ListBuffer
import scala.util.parsing.json.JSON

/**
  * Created by anne on 2016/7/24.
  */
object CapitalStructureSql {

  def parse(totalJson: String): (ListBuffer[ListBuffer[String]], ListBuffer[ListBuffer[String]]
    ,ListBuffer[ListBuffer[String]]) = {

    var groupOne = new ListBuffer[ListBuffer[String]]()

    var groupTwo = new ListBuffer[ListBuffer[String]]()

    var groupThree = new ListBuffer[ListBuffer[String]]()

    var jsonInfo = JSON.parseFull(totalJson)

    if (jsonInfo.isEmpty) {

      println("\"JSON parse value is empty,please have a check!\"")

    } else {

      jsonInfo match {

        case Some(mapInfo) => {

          val partOne = mapInfo.asInstanceOf[Map[String, AnyVal]].getOrElse("股本结构", "").asInstanceOf[List[Map[String, AnyVal]]]

          val subPartOne = partOne.head.getOrElse("1", "").asInstanceOf[Map[String,AnyVal]]

          val subPartTwo = partOne(1).getOrElse("2", "").asInstanceOf[Map[String,AnyVal]]

          groupOne = getTables(subPartOne)

          groupTwo = getTables(subPartTwo)


          val partThree = mapInfo.asInstanceOf[Map[String, AnyVal]].getOrElse("历年股本变动", "").asInstanceOf[Map[String, AnyVal]]

          groupThree = specialStuation(partThree)
        }
        case None => println("Parsing failed!")

        case other => println("Unknown data structure :" + other)
      }

    }

    (groupOne, groupTwo,groupThree)

  }

  //从hbase 表取得数据进行解析
  def getTables(mapJson: Map[String, AnyVal]): ListBuffer[ListBuffer[String]] = {

    var outList = new ListBuffer[ListBuffer[String]]()

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

              val result = sub.getOrElse(y, "")

              inList += result.toString

            }

          }

        }

        outList+= inList

      }
    }

   outList
  }


  //从hbase 表取得数据进行解析
  def specialStuation(mapJson: Map[String, AnyVal]): ListBuffer[ListBuffer[String]] = {

    var outList = new ListBuffer[ListBuffer[String]]()

    val ids = mapJson.keys

    ids.foreach {

      index => {

        //最里层的ListBuffer[] 得到一行的数据
        var inList = new ListBuffer[String]()
        //每个行的数据
        val values = mapJson.getOrElse(index, "")

//        inList += index.toString //在每一行数据之前先添加第一列的key值

        if (values.toString.nonEmpty) {

          val sub = values.asInstanceOf[Map[String, AnyVal]]

          val keys = sub.keys

          keys.foreach {

            y => {

              val result = sub.getOrElse(y, "")

              //也要加上第一行的key值： 针对历年股本变动

              inList += y.toString

              inList += result.toString

            }

          }

        }

        outList+= inList

      }
    }

    var tranList = new ListBuffer[ListBuffer[String]]()



    if(null != outList){

      val size = outList.head.indices

      for(i<- size){

        var tranInList = new ListBuffer[String]()

        for(j<- outList.indices){

          val value = outList(j)(i)

          tranInList +=  value

        }

        tranList += tranInList

      }

    }

    tranList
  }


}

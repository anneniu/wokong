package com.kumyan.companyinfo.parser

import scala.collection.mutable.ListBuffer
import scala.util.parsing.json.JSON

/**
  * Created by niujiaojiao on 2016/7/24.
  */
object CpnyExecutiveSql {

  def parse(totalJson: String): (ListBuffer[ListBuffer[String]], ListBuffer[ListBuffer[String]] )= {

    var groupOne = new ListBuffer[ListBuffer[String]]()

    var groupTwo = new ListBuffer[ListBuffer[String]]()

    var jsonInfo = JSON.parseFull(totalJson)

    if (jsonInfo.isEmpty) {

      println("\"JSON parse value is empty,please have a check!\"")

    } else {

      jsonInfo match {

        case Some(mapInfo) => {

          val partOne = mapInfo.asInstanceOf[Map[String, AnyVal]].getOrElse("高管列表", "").asInstanceOf[Map[String, AnyVal]]


          groupOne = CapitalStructureSql.getTables(partOne)



          val partTwo = mapInfo.asInstanceOf[Map[String, AnyVal]].getOrElse("管理层简介", "").asInstanceOf[Map[String, AnyVal]]

          groupTwo = CapitalStructureSql.getTables(partTwo)
        }
        case None => println("Parsing failed!")

        case other => println("Unknown data structure :" + other)
      }

    }

    (groupOne, groupTwo)

  }






}

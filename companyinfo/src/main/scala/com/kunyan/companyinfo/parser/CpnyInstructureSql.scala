package com.kunyan.companyinfo.parser

import scala.collection.mutable.ListBuffer
import scala.util.parsing.json.JSON

/**
  * Created by niujiaojiao on 2016/7/24.
  * updated by zhangruibo on 2016/7/25.
  */
object CpnyInstructureSql {

  def parse(totalJson: String): ListBuffer[String] = {

    var groupOne = new ListBuffer[String]()

    val jsonInfo = JSON.parseFull(totalJson)

    if (jsonInfo.isEmpty) {

      println("\"JSON parse value is empty,please have a check!\"")

    } else {

      jsonInfo match {

        case Some(mapInfo) => {

          val partOne = mapInfo.asInstanceOf[Map[String, AnyVal]].getOrElse("基本资料", "").asInstanceOf[Map[String, AnyVal]]

          groupOne = getTables(partOne)
        }
        case None => println("Parsing failed!")

        case other => println("Unknown data structure :" + other)
      }

    }

    groupOne
  }

  def getTables(mapJson: Map[String, AnyVal]): ListBuffer[String] = {

    var inList = new ListBuffer[String]()

    if (mapJson.toString.nonEmpty) {

      val companyName = mapJson.getOrElse("公司名称", "").toString
      val companyEngName = mapJson.getOrElse("英文名称", "").toString
      val usedName = mapJson.getOrElse("曾用名", "").toString
      val AStockcode = mapJson.getOrElse("A股代码", "").toString
      val AShort = mapJson.getOrElse("A股简称", "").toString
      val BStockcode = mapJson.getOrElse("B股代码", "").toString
      val BShort = mapJson.getOrElse("B股简称", "").toString
      val HStockcodeHStockcode = mapJson.getOrElse("H股代码", "").toString
      val HShort = mapJson.getOrElse("H股简称", "").toString
      val securityType = mapJson.getOrElse("证券类别", "").toString
      val industryInvolved = mapJson.getOrElse("所属行业", "").toString
      val ceo = mapJson.getOrElse("总经理", "").toString
      val lawPerson = mapJson.getOrElse("法人代表", "").toString
      val secretary = mapJson.getOrElse("董秘", "").toString
      val chairman = mapJson.getOrElse("董事长", "").toString
      val securityAgent = mapJson.getOrElse("证券事务代表", "").toString
      val independentDirector = mapJson.getOrElse("独立董事", "").toString
      val companyTel = mapJson.getOrElse("联系电话", "").toString
      val companyEmail = mapJson.getOrElse("电子信箱", "").toString
      val companyFax = mapJson.getOrElse("传真", "").toString
      val companyWebsite = mapJson.getOrElse("公司网址", "").toString
      val businessAddress = mapJson.getOrElse("办公地址", "").toString
      val regAddress = mapJson.getOrElse("注册地址", "").toString
      val area = mapJson.getOrElse("区域", "").toString
      val postCode = mapJson.getOrElse("邮政编码", "").toString
      val regCaptical = mapJson.getOrElse("注册资本(元)", "").toString
      val businessRegistration = mapJson.getOrElse("工商登记", "").toString
      val employeeNum = mapJson.getOrElse("雇员人数", "").toString
      val adminNum = mapJson.getOrElse("管理人员人数", "").toString
      val lawFirm = mapJson.getOrElse("律师事务所", "").toString
      val accountingFirm = mapJson.getOrElse("会计师事务所", "").toString
      val company_intro = mapJson.getOrElse("公司简介", "").toString
      val business_scope = mapJson.getOrElse("经营范围", "").toString

      inList += companyName
      inList += companyEngName
      inList += usedName
      inList += AStockcode
      inList += AShort
      inList += BStockcode
      inList += BShort
      inList += HStockcodeHStockcode
      inList += HShort
      inList += securityType
      inList += industryInvolved
      inList += ceo
      inList += lawPerson
      inList += secretary
      inList += chairman
      inList += securityAgent
      inList += independentDirector
      inList += companyTel
      inList += companyEmail
      inList += companyFax
      inList += companyWebsite
      inList += businessAddress
      inList += regAddress
      inList += area
      inList += postCode
      inList += regCaptical
      inList += businessRegistration
      inList += employeeNum
      inList += adminNum
      inList += lawFirm
      inList += accountingFirm
      inList += company_intro
      inList += business_scope

    }

    inList
  }

}

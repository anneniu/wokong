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

      val company_name = mapJson.getOrElse("公司名称", "").toString
      val company_eng_name = mapJson.getOrElse("英文名称", "").toString
      val used_name = mapJson.getOrElse("曾用名", "").toString
      val A_stockcode = mapJson.getOrElse("A股代码", "").toString
      val A_short = mapJson.getOrElse("A股简称", "").toString
      val B_stockcode = mapJson.getOrElse("B股代码", "").toString
      val B_short = mapJson.getOrElse("B股简称", "").toString
      val H_stockcodeH_stockcode = mapJson.getOrElse("H股代码", "").toString
      val H_short = mapJson.getOrElse("H股简称", "").toString
      val security_type = mapJson.getOrElse("证券类别", "").toString
      val industry_involved = mapJson.getOrElse("所属行业", "").toString
      val ceo = mapJson.getOrElse("总经理", "").toString
      val law_person = mapJson.getOrElse("法人代表", "").toString
      val secretary = mapJson.getOrElse("董秘", "").toString
      val chairman = mapJson.getOrElse("董事长", "").toString
      val security_agent = mapJson.getOrElse("证券事务代表", "").toString
      val independent_director = mapJson.getOrElse("独立董事", "").toString
      val company_tel = mapJson.getOrElse("联系电话", "").toString
      val company_email = mapJson.getOrElse("电子信箱", "").toString
      val company_fax = mapJson.getOrElse("传真", "").toString
      val company_website = mapJson.getOrElse("公司网址", "").toString
      val business_address = mapJson.getOrElse("办公地址", "").toString
      val reg_address = mapJson.getOrElse("注册地址", "").toString
      val area = mapJson.getOrElse("区域", "").toString
      val post_code = mapJson.getOrElse("邮政编码", "").toString
      val reg_captical = mapJson.getOrElse("注册资本(元)", "").toString
      val business_registration = mapJson.getOrElse("工商登记", "").toString
      val employee_num = mapJson.getOrElse("雇员人数", "").toString
      val admin_num = mapJson.getOrElse("管理人员人数", "").toString
      val law_firm = mapJson.getOrElse("律师事务所", "").toString
      val accounting_firm = mapJson.getOrElse("会计师事务所", "").toString
      val company_intro = mapJson.getOrElse("公司简介", "").toString
      val business_scope = mapJson.getOrElse("经营范围", "").toString

      inList += company_name
      inList += company_eng_name
      inList += used_name
      inList += A_stockcode
      inList += A_short
      inList += B_stockcode
      inList += B_short
      inList += H_stockcodeH_stockcode
      inList += H_short
      inList += security_type
      inList += industry_involved
      inList += ceo
      inList += law_person
      inList += secretary
      inList += chairman
      inList += security_agent
      inList += independent_director
      inList += company_tel
      inList += company_email
      inList += company_fax
      inList += company_website
      inList += business_address
      inList += reg_address
      inList += area
      inList += post_code
      inList += reg_captical
      inList += business_registration
      inList += employee_num
      inList += admin_num
      inList += law_firm
      inList += accounting_firm
      inList += company_intro
      inList += business_scope

    }

    inList
  }

}

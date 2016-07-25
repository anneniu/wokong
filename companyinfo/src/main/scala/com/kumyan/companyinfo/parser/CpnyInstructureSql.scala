package com.kumyan.companyinfo.parser

import scala.collection.mutable.ListBuffer
import scala.util.parsing.json.JSON

/**
  * Created by anne on 2016/7/24.
  */
object CpnyInstructureSql {

  def parse(totalJson: String): ListBuffer[String] = {

    var groupOne = new ListBuffer[String]()

    var jsonInfo = JSON.parseFull(totalJson)

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

  //从hbase 表取得数据进行解析
  def getTables(mapJson: Map[String, AnyVal]): ListBuffer[String] = {

    var inList = new ListBuffer[String]()

    if (mapJson.toString.nonEmpty) {

      val keys = mapJson.keys

      val company_name = mapJson.getOrElse("公司名称", "").toString
//      val company_eng_name = mapJson.getOrElse(y, "")
//      val used_name = mapJson.getOrElse(y, "")
//      val A_stockcode = mapJson.getOrElse(y, "")
//      val A_short = mapJson.getOrElse(y, "")
//      val B_stockcode = mapJson.getOrElse(y, "")
//      val B_short = mapJson.getOrElse(y, "")
//      val B_short = mapJson.getOrElse(y, "")
//      val H_stockcodeH_stockcode = mapJson.getOrElse(y, "")
//
//      val H_short = mapJson.getOrElse(y, "")
//      val security_type = mapJson.getOrElse(y, "")
//      val industry_involved = mapJson.getOrElse(y, "")
//      val ceo = mapJson.getOrElse(y, "")
//      val law_person = mapJson.getOrElse(y, "")
//      val secretary = mapJson.getOrElse(y, "")
//      val chairman = mapJson.getOrElse(y, "")
//      val security_agent = mapJson.getOrElse(y, "")
//      val independent_director = mapJson.getOrElse(y, "")
//      val company_tel = mapJson.getOrElse(y, "")
//      val company_email = mapJson.getOrElse(y, "")
//      val company_fax = mapJson.getOrElse(y, "")
//      val company_website = mapJson.getOrElse(y, "")
//      val business_address = mapJson.getOrElse(y, "")
//      val reg_address = mapJson.getOrElse(y, "")
//      val area = mapJson.getOrElse(y, "")
//      val post_code = mapJson.getOrElse(y, "")
//      val reg_captical = mapJson.getOrElse(y, "")
//      val business_registration = mapJson.getOrElse(y, "")
//      val employee_num = mapJson.getOrElse(y, "")
//      val admin_num = mapJson.getOrElse(y, "")
//      val law_firm = mapJson.getOrElse(y, "")
//      val accounting_firm = mapJson.getOrElse(y, "")
//      val company_intro = mapJson.getOrElse(y, "")
//      val business_scope = mapJson.getOrElse(y, "")

      inList += company_name
//      inList+= company_eng_name
//      inList += used_name
//        A_stockcode,A_short,B_stockcode,B_short)
//      inList += (
//        H_stockcodeH_stockcode,H_short,security_type,industry_involved,ceo,
//        law_person,secretary,chairman,security_agent,independent_director)
//      inList +=
//        company_tel,company_email, company_fax, company_website,
//        business_address, reg_address, area, post_code, reg_captical,
//        business_registration, employee_num, admin_num, law_firm,
//        accounting_firm, company_intro, business_scope)

    }

    inList

  }

}

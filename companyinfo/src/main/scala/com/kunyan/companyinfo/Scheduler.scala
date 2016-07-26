package com.kunyan.companyinfo

import java.io.{File, PrintWriter}
import java.sql.{PreparedStatement, DriverManager}
import com.kunyan.companyinfo.db.DbConnection
import com.kunyan.companyinfo.parser.{stockHolderSql, CpnyExecutiveSql, CpnyInstructureSql, CapitalStructureSql}
import org.apache.hadoop.hbase.{TableName, HBaseConfiguration}

import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.xml.XML

/**
  * Created by niujiaojiao on 2016/7/21.
  * 主程序入口
  */
object Scheduler {

  /**
    * 主程序操作 四个部分 八个表写入数据库，从hbase表的相应表名读取字符串
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {

    val configFile = XML.loadFile("E:/wokong/companyinfo/src/main/resources/configFile.xml")

    //hbase 连接信息
    val hbaseConf = HBaseConfiguration.create

    hbaseConf.set("hbase.rootdir", (configFile \ "hbase" \ "rootDir").text)
    hbaseConf.set("hbase.zookeeper.quorum", (configFile \ "hbase" \ "ip").text)

    val hbaseConnection = org.apache.hadoop.hbase.client.ConnectionFactory.createConnection(hbaseConf)

    val familyname = List[String](DbConnection.COLUMN_FAMILY_NAME)

    val hbaseTable = hbaseConnection.getTable(TableName.valueOf(DbConnection.TABLE_NAME))

    //    mysql 连接信息
    Class.forName("com.mysql.jdbc.Driver")

    val mysqlconnection = DriverManager.getConnection((configFile \ "mysql" \ "url").text,
      (configFile \ "mysql" \ "username").text, (configFile \ "mysql" \ "password").text)

    mysqlconnection.setAutoCommit(true)

    //股票代码
    val file = Source.fromFile("E:/wokong/companyinfo/src/main/resources/StockCode.txt").getLines()

    file.foreach {

      x => {

        val getRes = DbConnection.query(DbConnection.TABLE_NAME, x, hbaseConnection)

        //第一部分
        if (null != getRes._1) {

          val result = CpnyInstructureSql.parse(getRes._1)
          println(result.size)
          val stock_code = x
          val company_name = result(0)
          val company_eng_name = result(1)
          val used_name = result(2)
          val A_stockcode = result(3)
          val A_short = result(4)
          val B_stockcode = result(5)
          val B_short = result(6)
          val H_stockcode = result(7)
          val H_short = result(8)
          val security_type = result(9)
          val industry_involved = result(10)
          val ceo = result(11)
          val law_person = result(12)
          val secretary = result(13)
          val chairman = result(14)
          val security_agent = result(15)
          val independent_director = result(16)
          val company_tel = result(17)
          val company_email = result(18)
          val company_fax = result(19)
          val company_website = result(20)
          val business_address = result(21)
          val reg_address = result(22)
          val area = result(23)
          val post_code = result(24)
          val reg_captical = result(25)
          val business_registration = result(26)
          val employee_num = result(27)
          val admin_num = result(28)
          val law_firm = result(29)
          val accounting_firm = result(30)
          val company_intro = result(31)
          val business_scope = result(32)

          val company_profileSql = mysqlconnection.prepareStatement("INSERT INTO company_profile (stock_code,company_name, company_eng_name, used_name, A_stockcode,A_short,B_stockcode,B_short, H_stockcode,H_short,security_type,industry_involved,ceo, law_person,secretary,chairman,security_agent,independent_director, company_tel,company_email, company_fax, company_website, business_address, reg_address, area, post_code, reg_captial, business_registration, employee_num, admin_num, law_firm, accounting_firm, company_intro, business_scope) VALUES(?,?,?,?,?,?,?,?,?,?, ?,?,?,?,?,?,?,?,?,?, ?,?,?,?,?,?,?,?,?,?, ?,?,?,?)")

          DbConnection.insert(company_profileSql, stock_code, company_name, company_eng_name, used_name,
            A_stockcode, A_short, B_stockcode, B_short,
            H_stockcode, H_short, security_type, industry_involved, ceo,
            law_person, secretary, chairman, security_agent, independent_director,
            company_tel, company_email, company_fax, company_website,
            business_address, reg_address, area, post_code, reg_captical,
            business_registration, employee_num, admin_num, law_firm,
            accounting_firm, company_intro, business_scope)

        }

        //第二部分
        if (null != getRes._2) {

          val result = CpnyExecutiveSql.parse(getRes._2)

          val stockExecutiveSql = mysqlconnection.prepareStatement("INSERT INTO stock_executive " +
            "(stock_code,number,name,sex,age,education,duty)" +
            " VALUES (?,?,?,?,?,?,?)")

          val executivesProfileSql = mysqlconnection.prepareStatement("INSERT INTO executives_profile " +
            "(stock_code,name,sex,education,position,brief_intro)" +
            " VALUES (?,?,?,?,?,?)")

          //Table one
          result._1.foreach {

            info => {

              if (info != null && info.toString() != "") {

                val stock_code = x
                val number = info(0).toInt
                val name = info(1)
                val sex = info(4)
                var age = 0

                if (info(2) != "") {
                  age = info(2).toInt
                }

                val education = info(3)
                val duty = info(5)

                DbConnection.insert(stockExecutiveSql, stock_code, number, name, sex, age, education, duty)

              }

            }

          }


          //Table two
          result._2.foreach {

            info => {

              if (info != null) {

                val stock_code = x
                val name = info.head
                val sex = info(1)
                val education = info(2)
                var position = ""

                if (info.size >= 4) {

                  if (info(3).nonEmpty) {
                    position = info(3)
                  }

                }

                var brief_intro = ""

                if (info.size >= 5) {

                  if (info(4).nonEmpty) {
                    brief_intro = info(4)
                  }

                }

                DbConnection.insert(executivesProfileSql, stock_code, name, sex, education, position, brief_intro)

              }

            }

          }

        }

        //第三部分
        if (null != getRes._3) {

          val result = CapitalStructureSql.parse(getRes._3)

          //股本结构 TableOne

          val stockLimitSql = mysqlconnection.prepareStatement("INSERT INTO stock_limit " +
            "(stock_code,case_name, value, percent)" +
            " VALUES (?,?,?,?)")

          result._1.foreach {

            info => {

              val stock_code = x
              val case_name = info.head
              var value = 0.0

              if (info(1).nonEmpty) {
                value = info(1).toDouble
              }

              val percent = info(2)

              var res = 0.toDouble

              if (percent.nonEmpty) {

                if (percent.contains("%")) {
                  res = (percent.split("%")(0).toDouble / 100).formatted("%.4f").toDouble
                }

              }

              DbConnection.insert(stockLimitSql, stock_code, case_name, value, res)

            }

          }


          //Table two
          val stockCirculatingSql = mysqlconnection.prepareStatement("INSERT INTO stock_circulating " +
            "(stock_code,case_name, value, percent)" +
            " VALUES (?,?,?,?)")

          result._2.foreach {

            info => {

              val stock_code = x
              val stock_name = info.head
              var value = 0.0

              if (info(1).nonEmpty) {
                value = info(1).toDouble
              }

              var percent = info(2)

              // 百分比要转换成小数
              var res = 0.toDouble

              if (percent.nonEmpty) {

                if (percent.contains("%")) {

                  res = (percent.split("%")(0).toDouble / 100).formatted("%.4f").toDouble
                }

              }

              if (!stock_name.startsWith("其它已流通股份")) {
                DbConnection.insert(stockCirculatingSql, stock_code, stock_name, value, res)
              }

            }

          }

          //Table three
          val calendarStockSql = mysqlconnection.prepareStatement("INSERT INTO calendar_year_stock " +
            "(stock_code,date,general_capital, state_backing, limit_share," +
            "state_backing_limit, float_share, listed_share)" +
            " VALUES (?,?,?,?,?,?,?,?)")

          result._3.foreach {

            info => {

              val stock_code = x
              var date = ""

              if (info.nonEmpty) {
                date = info.last
              }

              var general_capital = 0.0
              var limit_share = 0.0

              var float_share = 0.0

              var listed_share = 0.0

              var state_backing = 0.0

              var state_backing_limit = 0.0

              for (j <- info.indices) {

                if (info(j).startsWith("总股本")) {

                  general_capital = info(j + 1).toString.toDouble

                } else if (info(j).startsWith("流通受限股份")) {

                  if (info(j + 1).toString.nonEmpty) {
                    limit_share = info(j + 1).toString.toDouble
                  }

                } else if (info(j).startsWith("已流通股份")) {

                  if (info(j + 1).toString.nonEmpty) {
                    float_share = info(j + 1).toString.toDouble
                  }

                } else if (info(j).startsWith("已上市流通A股")) {

                  if (info(j + 1).toString.nonEmpty) {
                    listed_share = info(j + 1).toString.toDouble
                  }
                } else if (info(j).startsWith("国家持股")) {

                  if (info(j + 1).toString.nonEmpty) {
                    state_backing = info(j + 1).toString.toDouble
                  }
                } else if (info(j).startsWith("国家持股(受限)")) {

                  if (info(j + 1).toString.nonEmpty) {
                    state_backing_limit = info(j + 1).toString.toDouble
                  }
                }

              }

              DbConnection.insert(calendarStockSql, stock_code, date, general_capital, state_backing, limit_share,
                state_backing_limit, float_share, listed_share)

            }

          }

        }

        //第四部分
        if (null != getRes._4) {

          // two tables:float_stockholder   and stockholder
          val result = stockHolderSql.parse(getRes._4)

          val floatSql = mysqlconnection.prepareStatement("INSERT INTO top10_float_stockholder (stock_code, date, rank, stockholder_name,stockholder_nature, share_type, shares_number, total_ratio, change_share,change_ratio)" +
            " VALUES (?,?,?,?,?,?,?,?,?,?)")

          val holderSql = mysqlconnection.prepareStatement("INSERT INTO top10_stockholder (stock_code, date, rank, stockholder_name, share_type, shares_number, total_ratio, change_share,change_ratio) VALUES (?,?,?,?,?,?,?,?,?)")

          val floatData = result._1

          val nonfloatData = result._2

          //十大流通股东
          for (i <- floatData.indices) {

            val topData = floatData(i)

            for (j <- topData.indices) {

              val midData = topData(j)
              var stock_code = x
              var date = ""
              var rank = 0
              var stockholder_name = ""
              var stockholder_nature = ""
              var share_type = ""
              var share_number = ""
              var total_ratio = ""
              var change_share = ""
              var change_ratio = ""

              for (k <- midData.indices) {

                //这一层是指一行的数据stockholder_nature, share_type, share_number, total_ratio, change_share,change_ratio

                date = midData.head

                if (midData(7).nonEmpty) {
                  rank = midData(7).toInt
                }

                stockholder_name = midData(3)
                stockholder_nature = midData(4)
                share_type = midData(1)
                share_number = midData(5)
                total_ratio = midData(2)
                change_share = midData(6)

              }

              DbConnection.insert(floatSql, stock_code, date, rank, stockholder_name, stockholder_nature, share_type, share_number, total_ratio, change_share, change_ratio)

            }

          }

          //十大股东
          for (i <- nonfloatData.indices) {

            val topData = nonfloatData(i)

            for (j <- topData.indices) {

              val midData = topData(j)
              var stock_code = x
              var date = ""
              var rank = 0
              var stockholder_name = ""
              var share_type = ""
              var share_number = ""
              var total_ratio = ""
              var change_share = ""
              var change_ratio = ""


              for (k <- midData.indices) {

                //(2015-06-30, 流通A股,限售流通A股, 3.65%, 58,563,387, 157,487, 4, 0.27%)
                //这一层是指一行的数据stockholder_name, share_type, share_number, total_ratio, change_share,change_ratio

                date = midData.head

                if (midData(6).nonEmpty) {
                  rank = midData(6).toInt
                }

                stockholder_name = midData(3)
                share_type = midData(1)
                share_number = midData(4)
                total_ratio = midData(2)
                change_share = midData(5)

              }

              DbConnection.insert(holderSql, stock_code, date, rank, stockholder_name, share_type, share_number, total_ratio, change_share, change_ratio)

            }

          }

        }

      }

    }

    println("Done!")

    hbaseConnection.close()

    mysqlconnection.close()
  }

  /**
    * 获取股票代码
    *
    * @param path 读取文件的路径
    * @return 所有股票代码的集合
    */
  def getCode(path: String): ListBuffer[String] = {

    var result = ListBuffer[String]()

    val file = Source.fromFile(path).mkString

    file.foreach {

      x => {
        result += x.toString
      }

    }

    result
  }


}

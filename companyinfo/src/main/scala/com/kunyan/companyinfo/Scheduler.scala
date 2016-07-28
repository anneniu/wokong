package com.kunyan.companyinfo

import java.io.{File, PrintWriter}
import java.sql.{PreparedStatement, DriverManager}
import com.kunyan.companyinfo.db.DbConnection
import com.kunyan.companyinfo.parser._
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

    val configFile = XML.loadFile(ClassLoader.getSystemResource("configFile.xml").toString.split("file:/")(1))

    //    //hbase 连接信息
    val hbaseConf = HBaseConfiguration.create
    hbaseConf.set("hbase.rootdir", (configFile \ "hbase" \ "rootDir").text)
    hbaseConf.set("hbase.zookeeper.quorum", (configFile \ "hbase" \ "ip").text)
    val hbaseConnection = org.apache.hadoop.hbase.client.ConnectionFactory.createConnection(hbaseConf)
    val familyname = List[String](DbConnection.COLUMN_FAMILY_NAME)
    val hbaseTable = hbaseConnection.getTable(TableName.valueOf(DbConnection.TABLE_NAME))
    Class.forName("com.mysql.jdbc.Driver")
    val mysqlConnection = DriverManager.getConnection((configFile \ "mysql" \ "url").text,
      (configFile \ "mysql" \ "username").text, (configFile \ "mysql" \ "password").text)
    mysqlConnection.setAutoCommit(true)

    //    股票代码
    val file = Source.fromFile(ClassLoader.getSystemResource("StockCode.txt").toString.split("file:/")(1)).getLines()

    file.foreach {

      x => {

        val getRes = DbConnection.query(DbConnection.TABLE_NAME, x, hbaseConnection)
        //第一部分
        if (null != getRes._1) {

          val result = CpnyInstructureSql.parse(getRes._1)
          val stockCode = x
          val companyName = result.head
          val companyEngName = result(1)
          val usedName = result(2)
          val AStockcode = result(3)
          val AShort = result(4)
          val BStockcode = result(5)
          val BShort = result(6)
          val HStockcode = result(7)
          val HShort = result(8)
          val securityType = result(9)
          val industryInvolved = result(10)
          val ceo = result(11)
          val lawPerson = result(12)
          val secretary = result(13)
          val chairman = result(14)
          val securityAgent = result(15)
          val independentDirector = result(16)
          val companyTel = result(17)
          val companyEmail = result(18)
          val companyFax = result(19)
          val companyWebsite = result(20)
          val businessAddress = result(21)
          val regAddress = result(22)
          val area = result(23)
          val postCode = result(24)
          val regCaptical = result(25)
          val businessRegistration = result(26)
          val employeeNum = result(27)
          val adminNum = result(28)
          val lawFirm = result(29)
          val accountingFirm = result(30)
          val companyIntro = result(31)
          val businessScope = result(32)

          val companyProfileSql = mysqlConnection.prepareStatement("INSERT INTO company_profile (stock_code,company_name, company_eng_name, used_name, A_stockcode,A_short,B_stockcode,B_short, H_stockcode,H_short,security_type,industry_involved,ceo, law_person,secretary,chairman,security_agent,independent_director, company_tel,company_email, company_fax, company_website, business_address, reg_address, area, post_code, reg_captial, business_registration, employee_num, admin_num, law_firm, accounting_firm, company_intro, business_scope) VALUES(?,?,?,?,?,?,?,?,?,?, ?,?,?,?,?,?,?,?,?,?, ?,?,?,?,?,?,?,?,?,?, ?,?,?,?)")

          DbConnection.insert(companyProfileSql, stockCode, companyName, companyEngName, usedName,
            AStockcode, AShort, BStockcode, BShort,
            HStockcode, HShort, securityType, industryInvolved, ceo,
            lawPerson, secretary, chairman, securityAgent, independentDirector,
            companyTel, companyEmail, companyFax, companyWebsite,
            businessAddress, regAddress, area, postCode, regCaptical,
            businessRegistration, employeeNum, adminNum, lawFirm,
            accountingFirm, companyIntro, businessScope)

        }

        //第二部分
        if (null != getRes._2) {

          val result = CpnyExecutiveSql.parse(getRes._2)

          val stockExecutiveSql = mysqlConnection.prepareStatement("INSERT INTO stock_executive " +
            "(stock_code,number,name,sex,age,education,duty)" +
            " VALUES (?,?,?,?,?,?,?)")
          val executivesProfileSql = mysqlConnection.prepareStatement("INSERT INTO executives_profile " +
            "(stock_code,name,sex,education,position,brief_intro)" +
            " VALUES (?,?,?,?,?,?)")
          //Table one
          result._1.foreach {

            info => {

              if (info != null && info.toString() != "") {

                val stockCode = x
                val number = info(0).toInt
                val name = info(1)
                val sex = info(4)
                var age = 0

                if (info(2) != "") {
                  age = info(2).toInt
                }

                val education = info(3)
                val duty = info(5)

                DbConnection.insert(stockExecutiveSql, stockCode, number, name, sex, age, education, duty)

              }

            }

          }


          //Table two
          result._2.foreach {

            info => {

              if (info != null) {

                val stockCode = x
                val name = info.head
                val sex = info(1)
                val education = info(2)
                var position = ""

                if (info.size >= 4) {

                  if (info(3).nonEmpty) {
                    position = info(3)
                  }

                }

                var briefIntro = ""

                if (info.size >= 5) {

                  if (info(4).nonEmpty) {
                    briefIntro = info(4)
                  }

                }

                DbConnection.insert(executivesProfileSql, stockCode, name, sex, education, position, briefIntro)

              }

            }

          }

        }

        //第三部分
        if (null != getRes._3) {

          val result = CapitalStructureSql.parse(getRes._3)

          //股本结构 TableOne

          val stockLimitSql = mysqlConnection.prepareStatement("INSERT INTO stock_limit " +
            "(stock_code,case_name, value, percent)" +
            " VALUES (?,?,?,?)")

          result._1.foreach {

            info => {

              val stockCode = x
              val caseName = info.head
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

              DbConnection.insert(stockLimitSql, stockCode, caseName, value, res)

            }

          }


          //Table two
          val stockCirculatingSql = mysqlConnection.prepareStatement("INSERT INTO stock_circulating " +
            "(stock_code,case_name, value, percent)" +
            " VALUES (?,?,?,?)")

          result._2.foreach {

            info => {

              val stockCode = x
              val stockName = info.head
              var value = 0.0

              if (info(1).nonEmpty) {
                value = info(1).toDouble
              }

              val percent = info(2)
              // 百分比要转换成小数
              var res = 0.toDouble

              if (percent.nonEmpty) {

                if (percent.contains("%")) {
                  res = (percent.split("%")(0).toDouble / 100).formatted("%.4f").toDouble
                }

              }

              if (!stockName.startsWith("其它已流通股份")) {
                DbConnection.insert(stockCirculatingSql, stockCode, stockName, value, res)
              }

            }

          }

          //Table three
          val calendarStockSql = mysqlConnection.prepareStatement("INSERT INTO calendar_year_stock " +
            "(stock_code,date,general_capital, state_backing, limit_share," +
            "state_backing_limit, float_share, listed_share)" +
            " VALUES (?,?,?,?,?,?,?,?)")

          result._3.foreach {

            info => {

              val stockCode = x
              var date = ""

              if (info.nonEmpty) {
                date = info.last
              }

              var generalCapital = 0.0
              var limitShare = 0.0
              var floatShare = 0.0
              var listedShare = 0.0
              var stateBacking = 0.0
              var stateBackingLimit = 0.0

              for (j <- info.indices) {

                if (info(j).startsWith("总股本")) {

                  generalCapital = info(j + 1).toString.toDouble

                } else if (info(j).startsWith("流通受限股份")) {

                  if (info(j + 1).toString.nonEmpty) {
                    limitShare = info(j + 1).toString.toDouble
                  }

                } else if (info(j).startsWith("已流通股份")) {

                  if (info(j + 1).toString.nonEmpty) {
                    floatShare = info(j + 1).toString.toDouble
                  }

                } else if (info(j).startsWith("已上市流通A股")) {

                  if (info(j + 1).toString.nonEmpty) {
                    listedShare = info(j + 1).toString.toDouble
                  }

                } else if (info(j).startsWith("国家持股")) {

                  if (info(j + 1).toString.nonEmpty) {
                    stateBacking = info(j + 1).toString.toDouble
                  }

                } else if (info(j).startsWith("国家持股(受限)")) {

                  if (info(j + 1).toString.nonEmpty) {
                    stateBackingLimit = info(j + 1).toString.toDouble
                  }

                }

              }

              DbConnection.insert(calendarStockSql, stockCode, date, generalCapital, stateBacking, limitShare,
                stateBackingLimit, floatShare, listedShare)

            }

          }

        }

        //第四部分
        if (null != getRes._4) {

          // two tables:float_stockholder   and stockholder
          val result = stockHolderSql.parse(getRes._4)
          val floatSql = mysqlConnection.prepareStatement("INSERT INTO top10_float_stockholder (stock_code, date, rank, stockholder_name,stockholder_nature, share_type, shares_number, total_ratio, change_share,change_ratio)" +
            " VALUES (?,?,?,?,?,?,?,?,?,?)")
          val holderSql = mysqlConnection.prepareStatement("INSERT INTO top10_stockholder (stock_code, date, rank, stockholder_name, share_type, shares_number, total_ratio, change_share,change_ratio) VALUES (?,?,?,?,?,?,?,?,?)")
          val floatData = result._1
          val nonfloatData = result._2

          //十大流通股东
          for (i <- floatData.indices) {

            val topData = floatData(i)

            for (j <- topData.indices) {

              val midData = topData(j)
              val stockCode = x
              var date = ""
              var rank = 0
              var stockholderName = ""
              var stockholderNature = ""
              var shareType = ""
              var shareNumber = ""
              var totalRatio = ""
              var changeShare = ""
              var changeRatio = ""

              for (k <- midData.indices) {

                //这一层是指一行的数据stockholder_nature, share_type, share_number, total_ratio, change_share,change_ratio

                date = midData.head

                if (midData(7).nonEmpty) {
                  rank = midData(7).toInt
                }

                stockholderName = midData(3)
                stockholderNature = midData(4)
                shareType = midData(1)
                shareNumber = midData(5)
                totalRatio = midData(2)
                changeShare = midData(6)
                changeRatio = midData(8)

              }

              DbConnection.insert(floatSql, stockCode, date, rank, stockholderName, stockholderNature, shareType, shareNumber, totalRatio, changeShare, changeRatio)

            }

          }

          //十大股东
          for (i <- nonfloatData.indices) {

            val topData = nonfloatData(i)

            for (j <- topData.indices) {

              val midData = topData(j)
              val stockCode = x
              var date = ""
              var rank = 0
              var stockholderName = ""
              var shareType = ""
              var shareNumber = ""
              var totalRatio = ""
              var changeShare = ""
              var changeRatio = ""

              for (k <- midData.indices) {

                //(2015-06-30, 流通A股,限售流通A股, 3.65%, 58,563,387, 157,487, 4, 0.27%)
                //这一层是指一行的数据stockholder_name, share_type, share_number, total_ratio, change_share,change_ratio

                date = midData.head

                if (midData(6).nonEmpty) {
                  rank = midData(6).toInt
                }

                stockholderName = midData(3)
                shareType = midData(1)
                shareNumber = midData(4)
                totalRatio = midData(2)
                changeShare = midData(5)
                changeRatio = midData(7)

              }

              DbConnection.insert(holderSql, stockCode, date, rank, stockholderName, shareType, shareNumber, totalRatio, changeShare, changeRatio)

            }

          }

        }

      }

    }

    println("Done!")
    hbaseConnection.close()
    mysqlConnection.close()
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

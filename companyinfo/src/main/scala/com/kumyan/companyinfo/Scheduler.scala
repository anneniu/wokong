package com.kumyan.companyinfo

import java.io.{File, PrintWriter}
import java.sql.{PreparedStatement, DriverManager}

import com.kumyan.companyinfo.db.{DbUtil, DbConnection}
import com.kumyan.companyinfo.parser._
import org.apache.hadoop.hbase.{TableName, HBaseConfiguration}

import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.xml.XML

/**
  * Created by niujiaojiao on 2016/7/21.
  * 主程序入口
  */
object Scheduler {


  def main(args: Array[String]): Unit = {

    val configFile = XML.loadFile("D:/company/wokong/companyinfo/src/main/resources/configFile.xml")
    //hbase 连接信息
    val hbaseConf = HBaseConfiguration.create
    hbaseConf.set("hbase.rootdir", (configFile \ "hbase" \ "rootDir").text)
    hbaseConf.set("hbase.zookeeper.quorum", (configFile \ "hbase" \ "ip").text)

    val hbaseConnection = org.apache.hadoop.hbase.client.ConnectionFactory.createConnection(hbaseConf)

    sys.addShutdownHook {
      hbaseConnection.close()
    }

    //    val familyname = List[String](DbConnection.COLUMN_FAMILY_NAME)
    //
    //    val hbaseTable = hbaseConnection.getTable(TableName.valueOf(DbConnection.TABLE_NAME))
    //
    //
    //    DbConnection.createHbaseTable(TableName.valueOf(DbConnection.TABLE_NAME), familyname, hbaseConnection)

    //    mysql 连接信息
    Class.forName("com.mysql.jdbc.Driver")

    val mysqlconnection = DriverManager.getConnection((configFile \ "mysql" \ "url").text,
      (configFile \ "mysql" \ "username").text, (configFile \ "mysql" \ "password").text)

    //    val writer = new PrintWriter(new File("E:/EastForthJson.txt"), "utf-8")

    //    val file = Source.fromFile("E:/wokong/companyinfo/src/main/resources/StockCode.txt").getLines()

    //    file.foreach {
    //
    //      x => {
    //
    //        val firstJson = CpnyInstructure.parseCpnyInstructure(x)
    //
    //        val secondJson = CpnyExecutives.parseCpnyExecutives(x)
    //
    //        val thirdJson = CapitalStructure.parseCapitalStructure(x)
    //
    //        val fourthJson = StockHolders.parseStockHolders(x)
    //
    //        //        writer.write( x + "\n" + "第四模块\n" + fourthJson )
    //        //        + "\n" + "第二模块\n" + secondJson + "\n" + "第三模块\n" + thirdJson
    //        //          + "\n" + "第四模块\n" )
    //        DbConnection.putResultToTable(hbaseTable, x.toString, ListBuffer[String](firstJson, secondJson, thirdJson, fourthJson))
    //
    //      }
    //
    //    }

    val getRes = DbConnection.query(DbConnection.TABLE_NAME, "sh166105", hbaseConnection)

    if (null != getRes._1) {
      val result = CpnyInstructureSql.parse(getRes._1)

      val company_profileSql = mysqlconnection.prepareStatement("INSERT INTO company_profile " +
        "(stock_code,company_name, company_eng_name, used_name," +
        "A_stockcode,A_short,B_stockcode,B_short" +
        "H_stockcodeH_stockcode,H_short,security_type,industry_involved,ceo," +
        "law_person,secretary,chairman,security_agent,independent_director," +
        "company_tel,company_email, company_fax, company_website," +
        "business_address, reg_address, area, post_code, reg_captical," +
        "business_registration, employee_num, admin_num, law_firm," +
        "accounting_firm, company_intro, business_scope" +
        ")" +

        " VALUES (?,?,?,?,?,?,?,?,?,?," +
        "?,?,?,?,?,?,?,?,?,?," +
        "?,?,?,?,?,?,?,?,?,?," +
        "?,?,?,?)")



      //操作入库
      val insertData = insertDataToSql(company_profileSql, result)


    }

    if (null != getRes._2) {
      val result = CpnyExecutiveSql.parse(getRes._2)

      val stockExecutiveSql = mysqlconnection.prepareStatement("INSERT INTO stock_executive " +
        "(stock_code,number,name,sex,age,education,duty)" +
        " VALUES (?,?,?,?,?,?,?)")

      val executivesProfileSql = mysqlconnection.prepareStatement("INSERT INTO executives_profile " +
        "(stock_code,name,sex,education,position,brief_intro)" +
        " VALUES (?,?,?,?,?,?)")


    }

    if (null != getRes._3) {

      val result = CapitalStructureSql.parse(getRes._3)

      //股本结构 TableOne
      val stockLimitSql = mysqlconnection.prepareStatement("INSERT INTO stock_limit " +
        "(stock_code,stock_name, value, percent)" +
        " VALUES (?,?,?,?)")

      result._1.foreach {
        info => {
          val stock_code = ""
          val stock_name = info.head
          val value = info(1).toDouble
          val percent = info(2)

          var res = 0.toDouble

          if(percent.contains("%")){

            res = (percent.split("%")(0).toDouble/100).formatted("%.4f").toDouble
          }

          DbConnection.insert(stockLimitSql, stock_code, stock_name, value, res)

        }

      }


      //股本结构 TableTwo

      val stockCirculatingSql = mysqlconnection.prepareStatement("INSERT INTO stock_circulating " +
        "(stock_code,case_name, value, percent)" +
        " VALUES (?,?,?,?)")

      result._2.foreach {

        info => {

          val stock_code = ""
          val stock_name = info.head
          val value = info(1).toDouble
          var percent = info(2)

          // 百分比要转换成小数
          var res = 0.toDouble

          if(percent.contains("%")){

            res = (percent.split("%")(0).toDouble/100).formatted("%.4f").toDouble
          }

          if (!stock_name.startsWith("其它已流通股份")) {
            DbConnection.insert(stockCirculatingSql, stock_code, stock_name, value, res)
          }

        }

      }

      //历年股本构成

      val calendarStockSql = mysqlconnection.prepareStatement("INSERT INTO calendar_year_stock " +
        "(stock_code,date,general_capital, state_backing, limit_share," +
        "state_backing_limit, float_share, listed_share)" +
        " VALUES (?,?,?,?,?,?,?,?)")
      result._3.foreach {

        info => {

          val stock_code = ""
          val date = info.head
          val general_capital = info(1).toDouble
          val state_backing = 0.toDouble
          val limit_share = info(2).toDouble
          val state_backing_limit = 0.toDouble
          val float_share = info(5).toDouble

          val listed_share = info(6).toDouble

            DbConnection.insert(calendarStockSql, stock_code, date,general_capital, state_backing, limit_share,
              state_backing_limit, float_share, listed_share)

        }

      }

    }

    if (null != getRes._4) {
      // two tables:float_stockholder   and stockholder

      val result = stockHolderSql.parse(getRes._4)

      val floatSql = mysqlconnection.prepareStatement("INSERT INTO top10_float_stockholder (stock_code, date, rank, stockholder_name,stockholder_nature, share_type, share_number, total_ratio, change_share,change_ratio)" +
        " VALUES (?,?,?,?,?,?,?,?,?,?)")


      val holderSql = mysqlconnection.prepareStatement("INSERT INTO top10_stockholder (stock_code, date, rank, stockholder_name, share_type, share_number, total_ratio, change_share,change_ratio)" +
        " VALUES (?,?,?,?,?,?,?,?,?)")



      val floatData = result._1

      val nonfloatData = result._2

      //十大流通股东
      for (i <- floatData.indices) {

        val topData = floatData(i)

        for (j <- topData.indices) {

          val midData = topData(j)

          for (k <- midData.indices) {

            //这一层是指一行的数据stockholder_nature, share_type, share_number, total_ratio, change_share,change_ratio

            val stock_code = ""
            val date = midData.head
            val rank = midData(1).toInt
            val stockholder_name = midData(2)
            val stockholder_nature = midData(3)
            val share_type = midData(4)
            val share_number = midData(5)
            val total_ratio = midData(6)
            val change_share = midData(7)
            val change_ratio = midData(8)

            DbConnection.insert(floatSql, stock_code, date, rank, stockholder_name, stockholder_nature, share_type, share_number, total_ratio, change_share, change_ratio)

          }
        }

      }

      //十大股东
      for (i <- nonfloatData.indices) {

        val topData = nonfloatData(i)

        for (j <- topData.indices) {

          val midData = topData(j)

          for (k <- midData.indices) {

            //这一层是指一行的数据stockholder_name, share_type, share_number, total_ratio, change_share,change_ratio

            val stock_code = ""
            val date = midData.head
            val rank = midData(1).toInt
            val stockholder_name = midData(2)
            val share_type = midData(3)
            val share_number = midData(4)
            val total_ratio = midData(5)
            val change_share = midData(6)
            val change_ratio = midData(7)

            DbConnection.insert(floatSql, stock_code, date, rank, stockholder_name, share_type, share_number, total_ratio, change_share, change_ratio)

          }
        }

      }


    }

    //    printl

    println("over")


    //    hbaseConnection.close()
    // sql close
  }

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


  def insertDataToSql(prep: PreparedStatement, list: ListBuffer[String]) = {


  }

}

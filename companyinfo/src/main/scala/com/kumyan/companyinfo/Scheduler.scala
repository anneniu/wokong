package com.kumyan.companyinfo

import java.io.{File, PrintWriter}
import java.sql.DriverManager

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

    val getRes = DbConnection.query(DbConnection.TABLE_NAME, "sh166105",hbaseConnection)

    if(null != getRes._1){

    }

    if(null != getRes._2){

    }

    if(null != getRes._3){
      //股本结构 TableOne
      val

      //股本结构 TableTwo
      //历年股本构成

    }

    if(null != getRes._4){
      // two tables:float_stockholder   and stockholder

      val floatSql = mysqlconnection.prepareStatement ("INSERT INTO top10_float_stockholder (stock_code, date, rank, stockholder_name,stockholder_nature, share_type, share_number, total_ratio, change_share,change_ratio)" +
        " VALUES (?,?,?,?,?,?,?,?,?,?)")


      val holderSql = mysqlconnection.prepareStatement ("INSERT INTO top10_stockholder (stock_code, date, rank, stockholder_name, share_type, share_number, total_ratio, change_share,change_ratio)" +
        " VALUES (?,?,?,?,?,?,?,?,?)")

      val result = stockHolderSql.parse(getRes._4)

      val floatData = result._1

      val nonfloatData = result._2

      //十大流通股东
      for(i<- floatData.indices){

        val topData = floatData(i)

        for(j<-topData.indices){

         val midData = topData(j)

          for(k<- midData.indices){

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

            DbConnection.insert(floatSql,stock_code, date, rank, stockholder_name,stockholder_nature, share_type, share_number, total_ratio, change_share,change_ratio )

          }
        }

      }

      //十大股东
      for(i<- nonfloatData.indices){

        val topData =nonfloatData(i)

        for(j<-topData.indices){

          val midData = topData(j)

          for(k<- midData.indices){

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

            DbConnection.insert(floatSql,stock_code, date, rank, stockholder_name, share_type, share_number, total_ratio, change_share,change_ratio )

          }
        }

      }


    }

//    printl

    println("over")


//    hbaseConnection.close()
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




}

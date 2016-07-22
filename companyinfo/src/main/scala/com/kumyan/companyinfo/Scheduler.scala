package com.kumyan.companyinfo

import java.io.{File, PrintWriter}
import java.sql.DriverManager

import com.kumyan.companyinfo.parser.{CapitalStructure, CpnyExecutives, CpnyInstructure}
import org.apache.hadoop.hbase.HBaseConfiguration

import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.xml.XML

/**
  * Created by niujiaojiao on 2016/7/21.
  */
object Scheduler {


  def main(args: Array[String]): Unit = {

    val configFile = XML.loadFile("/wokong/companyinfo/src/main/resources/configFile.xml")
    //hbase 连接信息
    val hbaseConf = HBaseConfiguration.create
    hbaseConf.set("hbase.rootdir", (configFile \ "hbase" \ "rootDir").text)
    hbaseConf.set("hbase.zookeeper.quorum", (configFile \ "hbase" \ "ip").text)

    val hbaseConnection = org.apache.hadoop.hbase.client.ConnectionFactory.createConnection(hbaseConf)

    sys.addShutdownHook {
      hbaseConnection.close()
    }

    //mysql 连接信息
    Class.forName("com.mysql.jdbc.Driver")

    val Mysqlconnection = DriverManager.getConnection((configFile \ "mysql" \ "url").text,
      (configFile \ "mysql" \ "username").text, (configFile \ "mysql" \ "password").text)

    val writer = new PrintWriter(new File("E:/getJsonTest.txt"), "gb2312")

    val file = Source.fromFile("E:/wokong/companyinfo/src/main/resources/StockCode.txt").getLines()

    file.foreach {

      x => {

        val firstJson = CpnyInstructure.parseCpnyInstructure(x)

        val secondJson = CpnyExecutives.parseCpnyExecutives(x)

        val thirdJson = CapitalStructure.parseCapitalStructure(x)

        writer.write(x + "\n" + "第一模块\n" + firstJson + "\n" + "第二模块\n" + secondJson + "\n" + "第三模块\n" + thirdJson)

        writer.write("*********************************************************************")
      }

    }

    println("over")

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

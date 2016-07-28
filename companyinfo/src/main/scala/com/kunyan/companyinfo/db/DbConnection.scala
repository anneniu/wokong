package com.kunyan.companyinfo.db

import java.sql.{PreparedStatement, DriverManager}

import com.ibm.icu.text.CharsetDetector
import org.apache.hadoop.hbase.client.{Get, Connection, Put, Table}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}

import scala.collection.mutable.ListBuffer

/**
  * Created by niujiaojiao on 2016/7/21.
  */
object DbConnection {

  val TABLE_NAME = "company_info_latest_second"

  val COLUMN_FAMILY_NAME = "company"

  val FAMILY_NAME = ListBuffer("CompanyStructure", "CapitalStructure", "CompanyExecutives", "StockHolders")

  /**
    * 创建hbase表
    *
    * @param tableName 表名
    * @param families  列族名：company
    * @param hbaseConn 服务器连接配置信息
    */
  def createHbaseTable(tableName: TableName, families: List[String], hbaseConn: Connection): Unit = {

    val admin = hbaseConn.getAdmin
    val htd = new HTableDescriptor(tableName)

    for (family <- families) {
      val hcd = new HColumnDescriptor(family)
      htd.addFamily(hcd.setMaxVersions(3))
    }

    admin.createTable(htd)
    admin.close()
  }

  /**
    * 结果写入hbase表中
    *
    * @param table   表名
    * @param stockID 表的rowkey
    * @param content 将要写入的内容
    */
  def putResultToTable(table: Table, stockID: String, content: ListBuffer[String]): Unit = {

    assert(content.size == FAMILY_NAME.size)

    val resultPut = new Put(Bytes.toBytes(stockID))

    for (i <- FAMILY_NAME.indices) {

      resultPut.addColumn(Bytes.toBytes(COLUMN_FAMILY_NAME), Bytes.toBytes(FAMILY_NAME(i)), Bytes.toBytes(content(i)))

    }

    table.put(resultPut)
  }

  /**
    * 清空table 里面数据
    *
    * @param tableName 要清空的hbase表名的字符串
    * @param hbaseConn hbase的服务端的连接配置信息
    */
  def emptyHbaseTable(tableName: TableName, hbaseConn: Connection): Unit = {

    val admin = hbaseConn.getAdmin
    admin.disableTable(tableName)
    admin.deleteTable(tableName)
    admin.close()

  }

  def insert( prep: PreparedStatement, params: Any*): Unit = {

    try {

      for (i <- params.indices) {

        val param = params(i)

        param match {

          case param: String =>
            prep.setString(i + 1, param)
          case param: Int =>
            prep.setInt(i + 1, param)
          case param: Boolean =>
            prep.setBoolean(i + 1, param)
          case param: Long =>
            prep.setLong(i + 1, param)
          case param: Double =>
            prep.setDouble(i + 1, param)
          case _ =>
           println("Unknown Type")
        }

      }

      prep.executeUpdate

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

  }

  /**
    * 读取hbase 表四列的信息
    * @param tableName  表名
    * @param rowkey  行 键值
    * @param hbaseConn  hbase 连接
    * @return 表中四列的信息
    */
  def query(tableName: String, rowkey: String, hbaseConn:Connection ): (String, String,String,String) = {

    val table = hbaseConn.getTable(TableName.valueOf(tableName))

    val get = new Get(rowkey.getBytes)

    try {

      val companyInStructure= table.get(get).getValue(Bytes.toBytes(COLUMN_FAMILY_NAME), Bytes.toBytes(FAMILY_NAME(0)))

      val companyExecutives = table.get(get).getValue(Bytes.toBytes(COLUMN_FAMILY_NAME), Bytes.toBytes(FAMILY_NAME(1)))

      val CaptalStructure = table.get(get).getValue(Bytes.toBytes(COLUMN_FAMILY_NAME), Bytes.toBytes(FAMILY_NAME(2)))

      val stockHolders = table.get(get).getValue(Bytes.toBytes(COLUMN_FAMILY_NAME), Bytes.toBytes(FAMILY_NAME(3)))


      val encodingOne = new CharsetDetector().setText(companyInStructure).detect().getName

      val encodingTwo = new CharsetDetector().setText(companyExecutives).detect().getName

      val encodingThree = new CharsetDetector().setText(CaptalStructure).detect().getName

      val encodingFour = new CharsetDetector().setText(stockHolders).detect().getName

      (new String(companyInStructure, encodingOne),
        new String(companyExecutives,encodingTwo),
        new String(CaptalStructure,encodingThree),
        new String(stockHolders,encodingFour))

    } catch {

      case e: Exception =>
        null

    }

  }

}

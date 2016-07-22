package com.kumyan.companyinfo.db

import java.sql.DriverManager

import org.apache.hadoop.hbase.client.{Connection, Put, Table}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}

import scala.collection.mutable.ListBuffer

/**
  * Created by niujiaojiao on 2016/7/21.
  */
object Connections {

  val TABLE_NAME = "company_info"

  val COLUMN_FAMILY_NAME = "company"

  val FAMILY_NAME = ListBuffer("CompanyStructure", "CapitalStructure", "CompanyExecutives", "StockHolders")

  /**
    * 创建hbase表
    *
    * @param tableName 表名
    * @param families  列簇名
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

    for (i <- 0 until FAMILY_NAME.size) {

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


}

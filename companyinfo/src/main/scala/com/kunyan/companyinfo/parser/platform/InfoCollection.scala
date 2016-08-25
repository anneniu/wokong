package com.kunyan.companyinfo.parser.platform

import java.sql.DriverManager

import com.kunyan.companyinfo.db.HbaseConnection
import org.jsoup.Jsoup

import scala.util.parsing.json.JSON
import scala.xml.XML

/**
  * Created by niujiaojiao on 2016/8/25.
  *
  * 网页爬取数据解析数据写入数据库表(定时更新）company_db数据库（sh_sz_stock数据表->复制到test数据库STOCK_INFO表)
  */
object InfoCollection extends App{

  val configFile = XML.loadFile(ClassLoader.getSystemResource("configFile.xml").toString.split("file:/")(1))

  val mysqlConnection = DriverManager.getConnection((configFile \ "mysql" \ "url").text,
    (configFile \ "mysql" \ "username").text, (configFile \ "mysql" \ "password").text)
  mysqlConnection.setAutoCommit(true)

  val stockSql = mysqlConnection.prepareStatement("INSERT INTO sh_sz_stock (stock_code, stock_name, date," +
    "holder_count,compare_change, float_stock_num, top_float_stock_num," +
    "top_float_stock_ratio,top_stock_num,top_stock_ratio,institution_stock_num,institution_stock_ratio)"+
    " VALUES (?,?,?,?,?,?,?,?,?,?,?,?)")


  //yon
  val dateList = List("2015-12-31","2015-9-30","2015-6-30",
    "2015-3-31","2014-12-31","2014-9-30","2014-6-30","2014-3-31",
    "2013-12-31","2013-9-30","2013-6-30","2013-3-31","2012-12-31","2012-9-30",
  "2012-6-30","2012-3-31","2011-12-31"
  )

  for(index <- dateList.indices){

    val indexDate = dateList(index)
    val doc = Jsoup.connect("http://datainterface.eastmoney.com/EM_DataCenter/JS.aspx?type=GG&sty=GDRS&st=2&sr=-1&p=1&ps=3000&js=var%20FFkbXtZn={pages:(pc),data:[(x)]}&mkt=1&fd="+indexDate.toString).timeout(30000).get()
    val body = doc.select("body").toString
    val text = body.split("FFkbXtZn=")(1).split("</body>")(0).split("data:")(1).split("}")(0)
    val json = JSON.parseFull(text)

    if (json.isEmpty) {
      println("\"JSON parse value is empty,please have a check!\"")
    } else {
      json match {
        case Some(mapInfo) =>
          val list = mapInfo.asInstanceOf[List[String]]
          for (i <- list.indices) {
            val singleData = list(i)
            val rs = singleData.split(",")
            println(i)
            HbaseConnection.insert(stockSql,rs(0), rs(1), rs(14), rs(2), rs(3), rs(4),rs(5),rs(6),rs(7),rs(8),rs(9),rs(10))
          }
        case None => println("Parsing failed")
      }
    }
  }

  println("over")
  mysqlConnection.close()

}

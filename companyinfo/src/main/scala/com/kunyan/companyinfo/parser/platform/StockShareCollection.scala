package com.kunyan.companyinfo.parser.platform

import java.sql.DriverManager

import com.kunyan.companyinfo.db.HbaseConnection
import org.jsoup.Jsoup

import scala.xml.XML

/**
  * Created by niujiaojiao on 2016/8/25.
  *
logo
//全景网 > 数据中心 > 高管增减持 高管增减持排行榜

  //表名  company_db数据库（overload_unload_share数据表）->test数据库
  //具体解释见表 增减持不同的字段
  */
object StockShareCollection extends App {

  val configFile = XML.loadFile(ClassLoader.getSystemResource("configFile.xml").toString.split("file:/")(1))

  val mysqlConnection = DriverManager.getConnection((configFile \ "mysql" \ "url").text,
    (configFile \ "mysql" \ "username").text, (configFile \ "mysql" \ "password").text)
  mysqlConnection.setAutoCommit(true)

  val stockSql = mysqlConnection.prepareStatement("INSERT INTO overload_unload_share (identifier,stock_code, stock_name, shares_price, shares_number,float_number, total_number, ratio)"+ " VALUES (?,?,?,?,?,?,?,?)")

  try{

    for(i<- 1 until 32){

      println("This is page:  "+ i.toString)
//      http://data.p5w.net/executive/ranklist.php?a=out&m=12&s=s2&sd=1&page=1
//      val doc = Jsoup.connect("http://data.p5w.net/executive/ranklist.php?a=in&m=12&s=s2&sd=1&page="+i.toString).timeout(20000).get
      val doc = Jsoup.connect("http://data.p5w.net/executive/ranklist.php?a=out&m=12&s=s2&sd=1&page="+i.toString).timeout(20000).get
      val tbody = doc.select("tbody tr")

      for(j<- 1 until tbody.size){

        val values = tbody.get(j).getElementsByTag("td")
        val stockCode = values.get(0).text()
        val stockName = values.get(1).text()
        val sharesPrice = values.get(3).text().replace(",","")
        val sharesNumber= values.get(4).text().replace(",","")
        val floatNumber = values.get(5).text().replace(",","")
        val totalNumber = values.get(6).text().replace(",","")
        val ratio = values.get(7).text().replace(",","")
        
        HbaseConnection.insert(stockSql,0, stockCode, stockName, sharesPrice, sharesNumber, floatNumber, totalNumber,ratio)
      }

    }

  }catch {
    case e:Exception =>
      e.printStackTrace()
  }finally {

    mysqlConnection.close()
  }


}

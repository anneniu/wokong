import org.jsoup.Jsoup

/**
  * Created by niujiaojiao on 2016/7/22.
  */
object test extends App{

  val doc = Jsoup.connect("http://f10.eastmoney.com/f10_v2/CompanySurvey.aspx?code=sh166105").get


  val tableBot = doc.getElementById("fxxg").nextElementSibling().getElementsByTag("table").select("tbody")
  println(tableBot)

}

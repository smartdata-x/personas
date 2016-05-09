package lingying


import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by pc on 2016/5/4.
  * 解析 linkedin 用户信息
  * @param source 数据读取地址
  * @param sc  SparkContext对象
  * @param path 文件保存地址
  * @author  zhangruibo
  */
object InforRule {

  def main(args: Array[String]): Unit = {
    val source = args(0)
    val path = args(1)
    val list = List[String](
       "\"fullname\":\"",
      "\"firstTopCurrentPosition\"",
      "\"industry_highlight\"",
      "\"addresses\":[{\"address\":",
      "\"phones\":[{\"number\":",
      "\"emails\":[{\"email\":",
       "\"IMs\":[{\"type\":"
    )
    val conf = new SparkConf().setAppName("GetData")
    val sc = new SparkContext(conf)
    val rule1 = sc.textFile(source)
      .filter(x => x.contains("\"fullname\":\"") || x.contains("\"firstTopCurrentPosition\"") ||
        x.contains("\"industry_highlight\"") || x.contains("\"addresses\":[{\"address\":") ||
        x.contains("\"phones\":[{\"number\":") || x.contains("\"emails\":[{\"email\":") ||
        x.contains("\"IMs\":[{\"type\":"))
    val rule2 = sc.broadcast(rule1)
    val data = list.map(x => rule(x, rule2))
      .map(x => {
      if (x._1.contains("\"fullname\":\"")) getName(x._2)
      else if (x._1.contains("\"firstTopCurrentPosition\"")) getProfession(x._2)
      else if (x._1.contains("\"industry_highlight\"")) getIndustry(x._2)
      else if (x._1.contains("\"addresses\":[{\"address\":")) getAdresses(x._2)
      else if (x._1.contains("\"phones\":[{\"number\":"))  getPhones(x._2)
      else if (x._1.contains("\"emails\":[{\"email\":")) getEmail(x._2)
      else if (x._1.contains("\"emails\":[{\"email\":") || x._1.contains("\"IMs\":[{\"type\":"))
        getQqFromOut(getIMS(x._2),getEmail(x._2))
        })
    val distData = sc.parallelize(data)
    distData.repartition(1).saveAsTextFile(path)
  }

  // get name
  def getName(result: String): (String, String) = {
    val start = result.indexOf("\"fullname\":\"") + 12
    val name = result.substring(start, result.length - 1).split("\"")(0)
    ("name", name)
  }

  //get profession
  def getProfession(result: String): (String, String) = {
    val start = result.indexOf("\"firstTopCurrentPosition\":{\"") + 28
    val profession = result.substring(start, result.length - 1).split("\"")(6)
    ("profession", profession)
  }

  //get industry
  def getIndustry(result: String): (String, String) = {
    val start = result.indexOf("\"industry_highlight\":\"") + 22
    val industry = result.substring(start, result.length - 1).split("\"")(0)
    ("industry", industry)
  }

  //get adresses
  def getAdresses(result: String): (String, String) = {
    val start = result.indexOf("{\"addresses\":[{\"address\":\"") + 26
    val adresses = result.substring(start, result.length - 1).split("\"")(0)
    ("adresses", adresses)
  }

  //get phones
  def getPhones(result: String): (String, String) = {
    val start = result.indexOf("\"phones\":[{\"number\":") + 21
    val end = start + 11
    val phones = result.substring(start, end)
    ("phones", phones)
  }

  //get email
  def getEmail(result: String): (String, String) = {
    var email = ""
    if (result.contains("\"emails\":[{\"email\":")) {
      val start = result.indexOf("\"emails\":[{\"email\":") + 20
      email = result.substring(start, result.length - 1).split("\"")(0)
    }
    ("email", email) 
  }

  //get IMS
  def getIMS(result: String): (String, String) = {
    var ImsType = ""
    var userName = ""
    if (result.contains("\"IMs\":[{\"type\":")) {
	
      val start = result.indexOf("\"IMs\":[{\"type\":")
      val content = result.substring(start, result.length - 1)
      if (content.contains("qq")) {
	  
        ImsType = "qq"
        val qqStart = content.indexOf("qq") + 16
        val qqEnd = content.indexOf("}")
        userName = content.substring(qqStart, qqEnd - 1)
		
      }
	  
    }
    (ImsType, userName)
  }

  //get qq from Ims or email
  var qq = ""
  def getQqFromOut(ims: (String, String), email: (String, String)): (String, String) = {

    if (ims._1.equals("qq")) {
      qq = ims._2
    }
    else {
      if (email._2.contains("qq")) {
        qq = email._2.split("@")(0)
      }	  
    }
    ("qq", qq)
  }
  
  var str = ""
  
  //get data what can get information 
  def rule(result: String, ruleOne: Broadcast[RDD[String]]): (String, String) = {
    var flag = 1
    for (i <- ruleOne.value) {
	
      while (i.contains(result) && i != "" && flag == 1) {
        str = i
        flag = 0
      }
	  
    }
    (result, str)
  }
}


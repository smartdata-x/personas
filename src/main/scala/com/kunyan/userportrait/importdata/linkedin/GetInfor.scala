package lingying

import java.io.{OutputStreamWriter, FileOutputStream, File}
import com.sun.jersey.api.client.Client
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.http.util.EntityUtils
import org.apache.log4j.PropertyConfigurator

/**
  * Created by zhangruibo on 2016/5/3.
  * 获取linkedin用户信息页面源码
  * @param file  文件保存地址
  * @author  zhangruibo
  */
object GetInfor {
  val url = "https://www.linkedin.com/in/%E7%91%9E%E5%8B%83-%E5%BC%A0-85844111b?trk=nav_responsive_tab_profile"
  val userAgent = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.130 Safari/537.36"
  val encoding = "utf8"
  val cookie = "JSESSIONID=ajax:2736566739207119687;bcookie=v=2&6414dcf4-47d8-4357-8bb4-c12099b3c90d;bscookie=v=1&2016042509123923b9a58e-0714-43bd-83ab-efb5f5fe55e3AQH5TuuEkCo8Vk8PQf7j1zz82_eBNYmD;visit=v=1&M;liap=true;li_at=AQEDAR3Jb4QAEqHRAAABVEyx97YAAAFUYBW8w00AGDx5OvHBED-Pj-m4VEObafmGzW_tY_nrMxo2VcJU9MBzpq8XYCtyqxH0go2iqpGHQcIPLoIdrZIlfOoh65CZzwmDiWpWgd8qoQ_feK33Z42DM0Z7;sl=v=1&tpWBb;_lipt=0_z5oydY9vgyJ58osUL9cbKH6WuhiuqbCtt2rCi3cmCv9OBKkjIhmY7Vc4QTYO_rH9fD6UuRdNZKdWU3p8tC6PKbPVSTe317NINaRUjJUtDJI_wRtz93mg9hjIJJ9v4EfV;lidc=b=VB48:g=518:u=2:i=1461895730:t=1461980117:s=AQH2esOjtUFx6ZihcKNvgMa8nKRLJ3tz;lang=v=2&lang=zh-cn;oz_props_fetch_size1_undefined=undefined; wutan=Y9tBrjEZfGKAyUw4UgRLMjPCovR+1/hHg9acdBd4pO8=;share_setting=PUBLIC;L1c=6950417a;sdsc=1%3A1SZM1shxDNbLt36wZwCgPgvN58iw%3D;L1e=1b606881;_ga=GA1.2.808431156.1461894478"
  def main(args :Array[String]): Unit = {
    val log4jConfPath = "E:/hadoop-2.7.1/etc/hadoop/log4j.properties"
    PropertyConfigurator.configure(log4jConfPath)
//  BasicConfigurator.configure()
    val httpClient = new DefaultHttpClient()
    //val client = new Client()
    val httpGet = new HttpGet(url)
    httpGet.setHeader("Referer", "https://www.linkedin.com")
    httpGet.setHeader("Cookie", cookie)
    httpGet.setHeader("User-Agent", userAgent)
    val response = httpClient.execute(httpGet)
    val entity = response.getEntity()
    val result = EntityUtils.toString(entity, encoding)
    val file = args(0)
      //new File("f:/linying3.txt")
    val fileOne = new FileOutputStream(file)
    val writer = new OutputStreamWriter(fileOne, encoding)
    for (x <- result) {
      writer.write(x)
    }
    writer.close()

  }

}

import com.kunyan.userportrait.Extractor
import com.kunyan.userportrait.config.{PlatformConfig, FileFormatConfig}
import com.kunyan.userportrait.data.Analysis
import com.kunyan.userportrait.platform._
import org.scalatest.{Matchers, FlatSpec}

/**
  * Created by C.J.YOU on 2016/2/25.
  */
class AnalysisTest  extends FlatSpec with Matchers{

//  it should "work" in{
//    Analysis.loadData(Extractor.sc,"F:\\datatest\\data\\000095_0",FileFormatConfig.tableName)
//    Analysis.getAdAndUaAndUrl(PlatformConfig.PLATFORM_SUNING).foreach(SuNing.extract(_))
//    SuNing.urlListBuffer.foreach(x => println("suning:"+x))
//  }

//  it should "work " in {
//    Analysis.loadData(Extractor.sc,"F:\\datatest\\data\\000136_0",FileFormatConfig.tableName)
//    Analysis.getAdAndUa(PlatformConfig.PLATFORM_SUNING)
//  }

//  it should "work" in{
//    Analysis.loadData(Extractor.sc,"F:\\datatest\\data\\000095_0",FileFormatConfig.tableName)
//    Analysis.getAdAndUaAndUrl(PlatformConfig.PLATFORM_ELEME).foreach(Eleme.extract(_))
//    Eleme.urlListBuffer.foreach(x => println("ELEME:"+x))
//  }

  it should "work" in{
    Analysis.loadData(Extractor.sc,"F:\\datatest\\data\\000095_0",FileFormatConfig.tableName)
    Analysis.getAdAndUaAndUrl(PlatformConfig.PLATFORM_QZONE).foreach(Qzone.QQzone(_))
    Qzone.urlListBuffer.distinct.foreach(x => println("Qzone URL:"+x))
    // Qzone.QQListBuffer.distinct.foreach(x => println("Qzone QQ:"+x))
  }
//
//  it should "work" in{
//    Analysis.loadData(Extractor.sc,"F:\\datatest\\data\\000095_0",FileFormatConfig.tableName)
//    Analysis.getAdAndUaAndUrl(PlatformConfig.PLATFORM_WEIBO).foreach(WeiBo.extract(_))
//    WeiBo.urlListBuffer.foreach(x => println("WeiBo:"+x))
//  }

//  it should "work" in{
//    Analysis.loadData(Extractor.sc,"F:\\datatest\\data\\000095_0",FileFormatConfig.tableName)
//    Analysis.getAdAndUaAndUrl(PlatformConfig.PLATFORM_ZHIHU).foreach(ZhiHu.extract(_))
//    ZhiHu.urlListBuffer.foreach(x => println("ZhiHu:"+x))
//  }
}
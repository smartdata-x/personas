import com.kunyan.userportrait.Extractor
import com.kunyan.userportrait.config.{PlatformConfig, FileFormatConfig}
import com.kunyan.userportrait.data.Analysis
import com.kunyan.userportrait.platform.{SuNing, ZhiHu, Eleme}
import org.scalatest.{Matchers, FlatSpec}

/**
  * Created by C.J.YOU on 2016/2/25.
  */
class AnalysisTest  extends FlatSpec with Matchers{
  it should "work" in{
    Analysis.loadData(Extractor.sc,"F:\\datatest\\data\\000095_0",FileFormatConfig.tableName)
    Analysis.getAdAndUaAndUrl(PlatformConfig.PLATFORM_SUNING).foreach(SuNing.extract(_))
    SuNing.urlListBuffer.foreach(x => println("url:"+x))
  }
//  it should "work " in {
//    Analysis.loadData(Extractor.sc,"F:\\datatest\\data\\000136_0",FileFormatConfig.tableName)
//    Analysis.getAdAndUa(PlatformConfig.PLATFORM_SUNING)
//  }
}

import com.kunyan.userportrait.Extractor
import com.kunyan.userportrait.config.FileFormatConfig
import com.kunyan.userportrait.data.Analysis
import org.scalatest.{Matchers, FlatSpec}

/**
  * Created by C.J.YOU on 2016/2/25.
  */
class AnalysisTest  extends FlatSpec with Matchers{
  it should "work" in{
    Analysis.loadData(Extractor.sc,"F:\\datatest\\data\\000095_0",FileFormatConfig.tableName)
    Analysis.getAdAndUa
  }

}

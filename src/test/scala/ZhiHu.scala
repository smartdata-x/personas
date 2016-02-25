
import com.kunyan.userportrait.platform.ZhiHu
import org.scalatest.{Matchers, FlatSpec}


/**
  * Created by C.J.YOU on 2016/2/24.
  */
class ZhiHu extends FlatSpec with Matchers{
  it should "work" in {
    ZhiHu.extractQuestionAndAnwer("http://www.zhihu.com/question/")
  }
}


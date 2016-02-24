
import com.kunyan.userportrait.platform.ZhiHu
import org.scalatest.{Matchers, FlatSpec}


/**
  * Created by C.J.YOU on 2016/2/24.
  */
class ZHTest extends FlatSpec with Matchers{
  it should "work" in{
        ZhiHu.questionAndAnwer("http://www.zhihu.com/question/3938282/answer/8218559?from=timeline&isappinstalled=0&code=00197f60f5aae2989366030f81b7762D&state=c76efd1cde53dba06e28e403241cc4f7")
      }
}


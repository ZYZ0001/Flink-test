import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import com.alibaba.fastjson.JSON
import org.json4s.native.Serialization

object Test {
  def main(args: Array[String]): Unit = {
    implicit val formats = org.json4s.DefaultFormats
    val userInfoJsonString = Serialization.write(UserInfo("1001", "xiaoming", 25))
    println(userInfoJsonString)

    val userInfo: UserInfo = JSON.parseObject(userInfoJsonString, classOf[UserInfo])
    println(userInfo)
  }
}
case class UserInfo(id: String, name: String, age: Int)

object Test1 {
  def main(args: Array[String]): Unit = {
    println(LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")))
  }
}
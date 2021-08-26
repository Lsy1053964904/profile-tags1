package cn.itcast.tags.test.regex

import scala.util.matching.Regex

object RegexUrlTest {
	
	def main(args: Array[String]): Unit = {
		// 访问url
		val locUrl = "http://www.eshop.com/product/10781.html?ebi=ref-i5-main-1-7"
		
		// 正则表达式
		val regex: Regex = "^.+\\/product\\/(\\d+)\\.html.+$".r
		
		// 正则匹配
		val optionMatch: Option[Regex.Match] = regex.findFirstMatchIn(locUrl)
		// 获取匹配的值
		val productId = optionMatch match {
			case None => println("没有匹配成功"); null
			case Some(matchValue) => matchValue.group(1)
		}
		println(s"productId = $productId")
	}
	
}
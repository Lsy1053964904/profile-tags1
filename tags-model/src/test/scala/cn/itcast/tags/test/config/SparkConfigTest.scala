package cn.itcast.tags.test.config

import java.util
import java.util.Map

import com.typesafe.config.{Config, ConfigFactory, ConfigValue}

object SparkConfigTest{
	
	def main(args: Array[String]): Unit = {
		// a、使用ConfigFactory加载spark.conf
		val config: Config = ConfigFactory.load("spark.conf")
		
		// 获取加载配置信息
		val entrySet: util.Set[Map.Entry[String, ConfigValue]] = config.entrySet()
		
		// 遍历
		import scala.collection.JavaConverters._
		for(entry <- entrySet.asScala){
			// 获取属性来源的文件名称
			val resource = entry.getValue.origin().resource()
			if("spark.conf".equals(resource)){
				println(entry.getKey + ": " + entry.getValue.unwrapped().toString)
			}
		}
		
	}
	
}
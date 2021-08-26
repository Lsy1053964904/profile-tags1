package cn.itcast.tags.test.meta

import cn.itcast.tags.meta.MetaParse
import org.apache.spark.sql.{DataFrame, SparkSession}

object MySQLMetaTest {
	
	def main(args: Array[String]): Unit = {
		
		val spark = SparkSession
			.builder()
			.appName(this.getClass.getSimpleName.stripSuffix("$"))
			.master("local[4]")
			.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
			.getOrCreate()
		import org.apache.spark.sql.functions._
		import spark.implicits._
		
		// 1. 参数Map集合
		val paramsMap: Map[String, String] = Map(
			"inType"-> "mysql",
			"driver"-> "com.mysql.jdbc.Driver",
			"url"-> "jdbc:mysql://bigdata-cdh01.itcast.cn:3306/?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC",
			"user"-> "root",
			"password"-> "123456",
			"sql"-> "SELECT id, gender FROM tags_dat.tbl_users"
		)
		
		// 2. 加载数据
		val dataframe: DataFrame = MetaParse.parseMetaToData(spark, paramsMap)
		
		dataframe.printSchema()
		dataframe.show(20, truncate = false)
		
		spark.stop()
	}
	
}

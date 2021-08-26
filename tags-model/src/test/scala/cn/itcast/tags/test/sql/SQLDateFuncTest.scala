package cn.itcast.tags.test.sql

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 日期函数使用，范例代码
 */
object SQLDateFuncTest {
	
	def main(args: Array[String]): Unit = {
		
		val spark = SparkSession.builder()
			.appName(this.getClass.getSimpleName.stripSuffix("$"))
			.master("local[4]")
			.getOrCreate()
		import org.apache.spark.sql.functions._
		import spark.implicits._
		
		// 模拟数据
		val dataframe: DataFrame = Seq(
			"1594569600" // 2020-07-13 00:00:00
		).toDF("finishtime")
		
		val df: DataFrame = dataframe
			.select(
				// a. 将Long类型转换日期时间类型
				from_unixtime($"finishtime").as("finish_time"), //
				// b. 获取当前日期时间
				current_timestamp().as("now_time")
			)
			// c. 计算天数
			.select(
				$"finish_time", $"now_time", //
				datediff($"now_time", $"finish_time").as("days")
			)
		df.show(10, truncate = false)
		
		println("====================================================")
		dataframe
			.select(
				// a. 将Long类型转换日期类型
				from_unixtime($"finishtime", "yyyy-MM-dd").as("finish_date"), //
				// b. 获取当前日期时间
				current_date().as("now_date")
			)
			// c. 计算天数
			.select(
				$"finish_date", $"now_date", //
				datediff($"now_date", $"finish_date").as("days")
			)
			.show(10, truncate = false)
		
		// 应用结束，关闭资源
		spark.stop()
	}
	
}

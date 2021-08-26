package cn.itcast.tags.test.meta

import cn.itcast.tags.meta.MetaParse
import org.apache.spark.sql.{DataFrame, SparkSession}

object HdfsMetaTest {
	
	def main(args: Array[String]): Unit = {
		
		val spark = SparkSession
			.builder()
			.appName(this.getClass.getSimpleName.stripSuffix("$"))
			.master("local[4]")
			.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
			.getOrCreate()
		
		// 1. 参数Map集合
		val paramsMap: Map[String, String] = Map(
			"inType"-> "hdfs",
			"inPath"-> "hdfs://bigdata-cdh01.itcast.cn:8020/apps/datas/tbl_tag_logs.tsv",
			"sperator" -> "\t",
			"selectFieldNames" -> "global_user_id,loc_url,log_time"
		)
		
		// 2. 加载数据
		val dataframe: DataFrame = MetaParse.parseMetaToData(spark, paramsMap)
		
		dataframe.printSchema()
		dataframe.show(20, truncate = false)
		
		spark.stop()
	}
}
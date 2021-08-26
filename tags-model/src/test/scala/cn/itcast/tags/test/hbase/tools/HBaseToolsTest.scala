package cn.itcast.tags.test.hbase.tools

import cn.itcast.tags.tools.HBaseTools
import org.apache.spark.sql.{DataFrame, SparkSession}

object HBaseToolsTest {
	
	def main(args: Array[String]): Unit = {
		
		val spark = SparkSession.builder()
			.appName(this.getClass.getSimpleName.stripSuffix("$"))
			.master("local[4]")
			.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
			.getOrCreate()
		import spark.implicits._
		/*
			zkHosts=bigdata-cdh01.itcast.cn
			zkPort=2181
			hbaseTable=tbl_tag_users
			family=detail
			selectFieldNames=id,gender
		 */
		val df: DataFrame = HBaseTools.read(
			spark, "bigdata-cdh01.itcast.cn", "2181",
			"tbl_tag_users", "detail", Seq("id", "gender")
		)
		println(s"count = ${df.count()}")
		df.printSchema()
		df.show(100, truncate = false)
		
		HBaseTools.write(
			df, "bigdata-cdh01.itcast.cn", "2181",
			"tbl_users", "info", "id"
		)
		
		
		spark.stop()
	}
}

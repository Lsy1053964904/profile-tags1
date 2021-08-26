package cn.itcast.tags.test.hbase.sql

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
 * 测试自定义外部数据源实现从HBase表读写数据接口
 */
object HBaseSQLTest {
	
	def main(args: Array[String]): Unit = {
		
		val spark = SparkSession.builder()
			.appName(this.getClass.getSimpleName.stripSuffix("$"))
			.master("local[4]")
			.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
			.getOrCreate()
		
		// 读取数据
		val usersDF: DataFrame = spark.read
			.format("hbase")
			.option("zkHosts", "bigdata-cdh01.itcast.cn")
			.option("zkPort", "2181")
			.option("hbaseTable", "tbl_tag_users")
			.option("family", "detail")
			.option("selectFields", "id,gender")
			.load()
		
		//usersDF.printSchema()
		//usersDF.show(100, truncate = false)
		
		// 保存数据
		usersDF.write
			.mode(SaveMode.Overwrite)
			.format("hbase")
			.option("zkHosts", "bigdata-cdh01.itcast.cn")
			.option("zkPort", "2181")
			.option("hbaseTable", "tbl_users")
			.option("family", "info")
			.option("rowKeyColumn", "id")
			.save()
		
		spark.stop()
	}
}

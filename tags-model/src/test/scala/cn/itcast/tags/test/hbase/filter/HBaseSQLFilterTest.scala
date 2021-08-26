package cn.itcast.tags.test.hbase.filter

import org.apache.spark.sql.{DataFrame, SparkSession}

object HBaseSQLFilterTest {
	
	def main(args: Array[String]): Unit = {
		
		val spark: SparkSession = SparkSession
			.builder()
			.appName(this.getClass.getSimpleName.stripSuffix("$"))
			.master("local[4]")
			.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
			.getOrCreate()
		
		// 读取HBase表数据，不设置条件
		
		// TODO: 未设置任何过滤条件filterConditions
		val ordersDF: DataFrame = spark.read
        	.format("hbase")
			.option("zkHosts", "bigdata-cdh01.itcast.cn")
			.option("zkPort", "2181")
			.option("hbaseTable", "tbl_tag_orders")
			.option("family", "detail")
			.option("selectFields", "id,memberid,orderamount")
			.load()
		//ordersDF.printSchema()
		//ordersDF.show(50, truncate = false)
		println(s"count = ${ordersDF.count()}")
		
		// TODO：读取HBase表数据，设置条件: 2019-09-01
		val dataframe: DataFrame = spark.read
        	.format("hbase")
			.option("zkHosts", "bigdata-cdh01.itcast.cn")
			.option("zkPort", "2181")
			.option("hbaseTable", "tbl_tag_orders")
			.option("family", "detail")
			.option("selectFields", "id,memberid,orderamount")
			.option("filterConditions", "modified[lt]2019-09-01")
			.load()
		//dataframe.show(50, truncate = false)
		println(s"count = ${dataframe.count()}")
		
		spark.stop()
	}
	
}
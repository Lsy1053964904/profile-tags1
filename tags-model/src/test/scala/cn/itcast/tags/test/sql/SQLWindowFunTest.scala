package cn.itcast.tags.test.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window

/**
 * SparkSQL中开窗函数DSL编程
 */
object SQLWindowFunTest {
	
	def main(args: Array[String]): Unit = {
		
		val spark = SparkSession.builder()
			.appName(this.getClass.getSimpleName.stripSuffix("$"))
			.master("local[4]")
			.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
			// 设置Shuffle分区数目
			.config("spark.sql.shuffle.partitions", "4")
			.enableHiveSupport()
			// 设置与Hive集成: 读取Hive元数据MetaStore服务
			.config("hive.metastore.uris", "thrift://bigdata-cdh01.itcast.cn:9083")
			// 设置数据仓库目录
			.config("spark.sql.warehouse.dir", "hdfs://bigdata-cdh01.itcast.cn:8020/user/hive/warehouse")
			.getOrCreate()
		import org.apache.spark.sql.functions._
		import spark.implicits._
		
		// TODO: 获取各个部分薪资最高人的基本信息 -> deptno   sal
		// 使用SQL完成
		spark.sql(
			"""
			  |WITH tmp AS (
			  |   SELECT
			  |     ename, sal, deptno,
			  |     ROW_NUMBER() OVER(PARTITION BY deptno ORDER BY sal DESC) AS rnk
			  |   FROM
			  |     db_hive.emp
			  |)
			  |SELECT t.ename, t.sal, t.deptno FROM tmp t WHERE t.rnk = 1
			  |""".stripMargin)
    			.show(10, truncate = false)
		
		println("====================================================")
		
		// 使用DSL完成
		spark.read
			.table("db_hive.emp")
			// 窗口分析函数，添加一列
			.withColumn(
				"rnk", //
				row_number().over(
					Window.partitionBy($"deptno").orderBy($"sal".desc)
				)
			)
			// where条件
			.where($"rnk" === 1)
			.show(10, truncate = false)
		
		// 应用结束，关闭资源
		spark.stop()
	}
	
}

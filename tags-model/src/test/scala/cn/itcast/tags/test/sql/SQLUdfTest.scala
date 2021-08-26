package cn.itcast.tags.test.sql

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

object SQLUdfTest {
	
	def main(args: Array[String]): Unit = {
		
		val spark = SparkSession.builder()
			.appName(this.getClass.getSimpleName.stripSuffix("$"))
			.master("local[4]")
			.getOrCreate()
		import spark.implicits._
		
		// 0. 自定UDF函数，解析分解属性标签的规则rule： 19500101-19591231
		val rule_to_tuple: UserDefinedFunction = udf(
			(rule: String) => {
				val Array(start, end) = rule.split("-").map(_.toInt)
				// 返回二元组
				(start, end)
			}
		)
		
		val datasDF: DataFrame = Seq(
			"19500101-19591231",
			"19600101-19691231",
			"19700101-19791231",
			"19800101-19891231",
			"19900101-19991231",
			"20000101-20091231",
			"20100101-20191231",
			"20200101-20291231"
		).toDF("rule")
		
		val ruleDF: DataFrame = datasDF
			.select(
                rule_to_tuple($"rule").as("rules")
            )
			.select(
                $"rules._1".as("start"), 
                $"rules._2".as("end")
            )

		/*
		root
		 |-- start: integer (nullable = true)
		 |-- end: integer (nullable = true)
		
		+--------+--------+
		|   start|     end|
		+--------+--------+
		|19500101|19591231|
		|19600101|19691231|
		|19700101|19791231|
		|19800101|19891231|
		|19900101|19991231|
		|20000101|20091231|
		|20100101|20191231|
		|20200101|20291231|
		+--------+--------+
		 */
		ruleDF.printSchema()
		ruleDF.show()
	}
}
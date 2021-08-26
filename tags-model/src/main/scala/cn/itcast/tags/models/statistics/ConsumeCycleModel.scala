package cn.itcast.tags.models.statistics

import cn.itcast.tags.models.{AbstractModel, ModelType}
import cn.itcast.tags.tools.TagTools
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * 统计类型标签模型开发：消费周期标签模型
 */
class ConsumeCycleModel extends AbstractModel("消费周期标签", ModelType.STATISTICS){
	/*
	347	消费周期
		348	近7天		0-7
		349	近2周		8-14
		350	近1月		15-30
		351	近2月		31-60
		352	近3月		61-90
		353	近4月		91-120
		354	近5月		121-150
		355	近半年		151-180
	 */
	override def doTag(businessDF: DataFrame, tagDF: DataFrame): DataFrame = {
		import businessDF.sparkSession.implicits._
		
		/*
		root
		 |-- memberid: string (nullable = true)
		 |-- finishtime: string (nullable = true)
		 */
		businessDF.printSchema()
		businessDF.show(10,truncate = false)

		tagDF.printSchema()
		tagDF.show(10,truncate = false)
		/*
			+---------+----------+
			|memberid |finishtime|
			+---------+----------+
			|13823431 |1564415022|
			|4035167  |1565687310|
			|4035291  |1564681801|
			|4035041  |1565799378|
			|13823285 |1565062072|
			|4034219  |1563601306|
			|138230939|1565509622|
			|4035083  |1565731851|
			|138230935|1565382991|
			|13823231 |1565677650|
			+---------+----------+
		 */
		//businessDF.show(10, truncate = false)
		
		/*
		root
		 |-- id: long (nullable = false)
		 |-- name: string (nullable = true)
		 |-- rule: string (nullable = true)
		 |-- level: integer (nullable = true)
		 */
		//tagDF.printSchema()
		/*
			+---+----+-------+-----+
			|id |name|rule   |level|
			+---+----+-------+-----+
			|348|近7天 |0-7    |5    |
			|349|近2周 |8-14   |5    |
			|350|近1月 |15-30  |5    |
			|351|近2月 |31-60  |5    |
			|352|近3月 |61-90  |5    |
			|353|近4月 |91-120 |5    |
			|354|近5月 |121-150|5    |
			|355|近半年 |151-180|5    |
			+---+----+-------+-----+
		 */
		//tagDF.filter($"level" === 5).show(10, truncate = false)
		
		// 1. 对业务数据进行计算操作
		val consumerDaysDF: DataFrame = businessDF
			// a. 按照用户ID分组，获取最大订单完成时间
    		.groupBy($"memberid")
    		.agg(max($"finishtime").as("max_finishtime"))
			// b. 日期时间转换，和获取当前日期时间
			.select(
				$"memberid", //
				// 将Long类型转换日期时间类型
				from_unixtime($"max_finishtime").as("finish_time"), //
				//  获取当前日期时间
				current_timestamp().as("now_time")
			)
			// c. 计算天数
			.select(
				$"memberid".as("id"), //
				datediff($"now_time", $"finish_time").as("consumer_days")
			)
		//consumerDaysDF.printSchema()
		//consumerDaysDF.show(100, truncate = false)
		
		// 2. 提取属性标签规则数据
		val attrTagRuleDF: DataFrame = TagTools.convertTuple(tagDF)
		//attrTagRuleDF.show(10, truncate = false)
		
		// 3. 关联DataFrame，指定条件Where语句
		val modelDF: DataFrame = consumerDaysDF
			.join(attrTagRuleDF)
			// 设置条件，使用WHERE语句
			.where(
				$"consumer_days".between($"start", $"end")
			)
			// 选取字段值
			.select(
				$"id".as("userId"), //
				$"name".as("consumercycle") //
			)
		//modelDF.show(500, truncate = false)
		
		// 返回画像标签数据
		modelDF
		null
	}
}

object ConsumeCycleModel{
	def main(args: Array[String]): Unit = {
		val tagModel = new ConsumeCycleModel()
		tagModel.executeModel(348L)
	}
}
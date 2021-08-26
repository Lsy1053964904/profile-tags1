package cn.itcast.tags.models.statistics

import cn.itcast.tags.models.{AbstractModel, ModelType}
import cn.itcast.tags.tools.TagTools
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._


/**
 * 统计类型标签模型开发：支付方式标签模型
 */
class PayTypeModel extends AbstractModel("支付方式标签", ModelType.STATISTICS){
	/*
	356	支付方式
		357	支付宝		alipay
		358	微信支付		wxpay
		359	银联支付		chinapay
		360	货到付款		cod
	 */
	override def doTag(businessDF: DataFrame, tagDF: DataFrame): DataFrame = {
		import businessDF.sparkSession.implicits._
		
		/*
		root
		 |-- memberid: string (nullable = true)
		 |-- paymentcode: string (nullable = true)
		 */
		//businessDF.printSchema()
		//businessDF.show(10, truncate = false)
		
		/*
		root
		 |-- id: long (nullable = false)
		 |-- name: string (nullable = true)
		 |-- rule: string (nullable = true)
		 |-- level: integer (nullable = true)
		 */
		//tagDF.printSchema()
		//tagDF.filter($"level" === 5).show(10, truncate = false)
		
		// 1. 从业务数据中计算获取每个用户使用最多支付方式
		val paymentDF: DataFrame = businessDF
			// a. 按照先用户ID分组，再按支付编码paymentcode分组，使用count函数，统计次数
    		.groupBy($"memberid", $"paymentcode")
    		.count() // 使用count函数列名称就是：count
			// b. 使用窗口分析函数，获取最多次数
			.withColumn(
				"rnk", //
				row_number().over(
					Window.partitionBy($"memberid").orderBy($"count".desc)
				)
			)
			.where($"rnk" === 1)
			// c. 选取字段
    		.select($"memberid".as("id"), $"paymentcode".as("payment"))
		//paymentDF.printSchema()
		/*
			+---+-----------+
			|id |payment|
			+---+-----------+
			|1  |alipay     |
			|102|alipay     |
			|107|alipay     |
			|110|alipay     |
			|111|alipay     |
			|120|alipay     |
			|130|alipay     |
			|135|alipay     |
			|137|alipay     |
			|139|alipay     |
			+---+-----------+
		 */
		//paymentDF.show(10, truncate = false)
		
		// 2. 使用属性标签规则数据，调用工具类方法，给用户打标签
		val modelDF: DataFrame = TagTools.ruleMatchTag(
			paymentDF, "payment", tagDF
		)
		modelDF.show(100, truncate = false)
		
		// 返回画像标签户数
		modelDF
		null
	}
}

object PayTypeModel{
	def main(args: Array[String]): Unit = {
		val tagModel = new PayTypeModel()
		tagModel.executeModel(357L)
	}
}
package cn.itcast.tags.spark.sql

import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

/**
 * 默认数据源提供Relation对象，分别为加载数据和保存提供Relation对象
 */
class DefaultSource extends RelationProvider
		with CreatableRelationProvider with DataSourceRegister{
	
	val HBASE_TABLE_SELECT_FIELDS: String = "selectFields"
	val SPERATOR: String = ","
	
	/**
	 * 从数据源加载数据时，使用简称：hbase，不需要在写包名称
	 */
	override def shortName(): String = "hbase"
	
	/**
	 * 从数据源加载读取数据时，创建Relation对象，此Relation实现BaseRelation和TableScan
	 * @param sqlContext SparkSession实例对象
	 * @param parameters 表示连接数据源时参数，通过option设置
	 */
	override def createRelation(
		                           sqlContext: SQLContext,
		                           parameters: Map[String, String]
	                           ): BaseRelation = {
		// TODO： 从参数中获取查询字段名称
		val fields: Array[String] = parameters(HBASE_TABLE_SELECT_FIELDS).split(",")
		
		// 1. 自定义Schema信息
		val userSchema: StructType = new StructType(
			fields.map{field =>
				StructField(field, StringType, nullable = true)
			}
		)
		
		// 2. 创建HBaseRelation对象，传递参数
		val relation = new HBaseRelation(sqlContext, parameters, userSchema)
		// 3. 返回Relation对象
		relation
	}
	
	/**
	 * 将数据集保存至数据源时，创建Relation对象，此Relation对象实现BaseRelation和InsertableRelation
	 * @param sqlContext SparkSession实例对象
	 * @param mode 保存模式
	 * @param parameters 表示连接数据源时参数，通过option设置
	 * @param data 保存数据数据集
	 * @return
	 */
	override def createRelation(
		                           sqlContext: SQLContext,
		                           mode: SaveMode,
		                           parameters: Map[String, String],
		                           data: DataFrame
	                           ): BaseRelation = {
		// 1. 创建HBaseRelation对象
		val relation = new HBaseRelation(sqlContext, parameters, data.schema)
		// 2. 保存数据
		relation.insert(data, true)
		// 3. 返回Relation对象
		relation
	}
	
}

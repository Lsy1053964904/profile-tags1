package cn.itcast.tags.spark.sql

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Put, Result, Scan}
import org.apache.hadoop.hbase.filter.{FilterList, SingleColumnValueFilter}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, InsertableRelation, TableScan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

/**
 * 自定义外部数据源：从HBase表加载数据和保存数据至HBase表的Relation实现
 */
class HBaseRelation(
	                   context: SQLContext,
	                   params: Map[String, String],
	                   userSchema: StructType
                   ) extends BaseRelation
		with TableScan with InsertableRelation with Serializable {
	
	// 连接HBase数据库的属性名称
	val HBASE_ZK_QUORUM_KEY: String = "hbase.zookeeper.quorum"
	val HBASE_ZK_QUORUM_VALUE: String = "zkHosts"
	val HBASE_ZK_PORT_KEY: String = "hbase.zookeeper.property.clientPort"
	val HBASE_ZK_PORT_VALUE: String = "zkPort"
	val HBASE_TABLE: String = "hbaseTable"
	val HBASE_TABLE_FAMILY: String = "family"
	val SPERATOR: String = ","
	val HBASE_TABLE_SELECT_FIELDS: String = "selectFields"
	val HBASE_TABLE_ROWKEY_NAME: String = "rowKeyColumn"
	
	// filterConditions：modified[GE]20190601,modified[LE]20191201
	val HBASE_TABLE_FILTER_CONDITIONS: String = "filterConditions"
	
	/**
	 * 表示SparkSQL加载数据和保存程序入口，相当于SparkSession
	 */
	override def sqlContext: SQLContext = context
	
	/**
	 * 在SparkSQL中数据封装在DataFrame或者Dataset中Schema信息
	 */
	override def schema: StructType = userSchema
	
	/**
	 * 从数据源加载数据，封装至RDD中，每条数据在Row中，结合schema信息，转换为DataFrame
	 */
	override def buildScan(): RDD[Row] = {
		// 1. 读取配置信息，加载HBaseClient配置（主要ZK地址和端口号）
		val conf: Configuration = HBaseConfiguration.create()
		conf.set(HBASE_ZK_QUORUM_KEY, params(HBASE_ZK_QUORUM_VALUE))
		conf.set(HBASE_ZK_PORT_KEY, params(HBASE_ZK_PORT_VALUE))
		// 2. 设置表的名称
		conf.set(TableInputFormat.INPUT_TABLE, params(HBASE_TABLE))
		// TODO: 设置读取列簇和列名称
		val scan: Scan = new Scan()
		// 设置列簇
		val cfBytes: Array[Byte] = Bytes.toBytes(params(HBASE_TABLE_FAMILY))
		scan.addFamily(cfBytes)
		// 设置列
		val fields: Array[String] = params(HBASE_TABLE_SELECT_FIELDS).split(",")
		fields.foreach{field =>
			scan.addColumn(cfBytes, Bytes.toBytes(field))
		}
		
		// ==================================================================================
		// ================   此处设置HBase Filter过滤器   ================
		// a. 从option参数中获取过滤条件的值，可能没有设置，给以null
		val filterConditions: String = params.getOrElse(HBASE_TABLE_FILTER_CONDITIONS, null)
		// b. 判断过滤条件是否有值，如果有值，再创建过滤器，进行过滤数据
		val filterList: FilterList = new FilterList()
		if(null != filterConditions){
			// filterConditions：modified[GE]20190601,modified[LE]20191201
			filterConditions
				.split(",") // 多个过滤条件使用逗号隔开的，所以使用逗号分割
				// 将每个过滤条件构建SingleColumnValueFilter对象
    			.foreach{filterCondition =>
				    // modified[LE]20191201 创建Filter对象
				    // step1. 解析过滤条件，封装样例类CaseClass中
				    val condition: Condition = Condition.parseCondition(filterCondition)
				   // step2.
				    val filter: SingleColumnValueFilter = new SingleColumnValueFilter(
					    cfBytes,
					    Bytes.toBytes(condition.field), //
					    condition.compare,
					    Bytes.toBytes(condition.value) //
				    )
				    // step3. 将Filter放入列表中
				    filterList.addFilter(filter)
				    // step4. TODO: 必须获取过滤列的值
				    scan.addColumn(cfBytes, Bytes.toBytes(condition.field))
			    }
			
			// c. 针对扫描器Scan设置过滤器
			scan.setFilter(filterList)
		}
		// ==================================================================================
		
		// 设置Scan过滤数据: 将Scan对象转换为String
		conf.set(
			TableInputFormat.SCAN, //
			Base64.encodeBytes(ProtobufUtil.toScan(scan).toByteArray) //
		)
		// 3. 从HBase表加载数据
		val datasRDD: RDD[(ImmutableBytesWritable, Result)] = sqlContext.sparkContext
			.newAPIHadoopRDD(
				conf, //
				classOf[TableInputFormat], //
				classOf[ImmutableBytesWritable], //
				classOf[Result]
			)
		
		// 4. 解析获取HBase表每行数据Result，封装至Row对象中
		val rowsRDD: RDD[Row] = datasRDD.map{case (_, result) =>
			// 基于列名称获取对应的值
			val values: Seq[String] = fields.map{ field =>
				// 传递列名称和列簇获取value值
				val value: Array[Byte] = result.getValue(cfBytes, Bytes.toBytes(field))
				// 转换为字符串
				Bytes.toString(value)
			}
			// 将Seq序列转换为Row对象
			Row.fromSeq(values)
		}
		
		// 5. 返回RDD[Row]
		rowsRDD
	}
	
	/**
	 * 将DataFrame数据保存至数据源
	 * @param data 数据集
	 * @param overwrite 是否覆写
	 */
	override def insert(data: DataFrame, overwrite: Boolean): Unit = {
		// 1. 设置HBase依赖Zookeeper相关配置信息
		val conf: Configuration = HBaseConfiguration.create()
		conf.set(HBASE_ZK_QUORUM_KEY, params(HBASE_ZK_QUORUM_VALUE))
		conf.set(HBASE_ZK_PORT_KEY, params(HBASE_ZK_PORT_VALUE))
		// 2. 数据写入表的名称
		conf.set(TableOutputFormat.OUTPUT_TABLE, params(HBASE_TABLE))
		
		// 3. 将DataFrame中数据转换为RDD[(RowKey, Put)]
		val cfBytes: Array[Byte] = Bytes.toBytes(params(HBASE_TABLE_FAMILY))
		val columns: Array[String] = data.columns // 从DataFrame中获取列名称
		val datasRDD: RDD[(ImmutableBytesWritable, Put)] = data.rdd.map{ row =>
			// TODO: row 每行数据 转换为 二元组(RowKey, Put)
			// a. 获取RowKey值
			val rowKey: String = row.getAs[String](params(HBASE_TABLE_ROWKEY_NAME))
			val rkBytes: Array[Byte] = Bytes.toBytes(rowKey)
			// b. 构建Put对象
			val put: Put = new Put(rkBytes)
			// c. 设置列值
			columns.foreach{column =>
				val value = row.getAs[String](column)
				put.addColumn(cfBytes, Bytes.toBytes(column), Bytes.toBytes(value))
			}
			// d. 返回二元组
			(new ImmutableBytesWritable(rkBytes), put)
		}
		
		// 4. 保存RDD数据至HBase表中
		datasRDD.saveAsNewAPIHadoopFile(
			s"datas/hbase/output-${System.nanoTime()}", //
			classOf[ImmutableBytesWritable], //
			classOf[Put], //
			classOf[TableOutputFormat[ImmutableBytesWritable]], //
			conf
		)
	}
}

package cn.itcast.tags.tools

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Put, Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableMapReduceUtil, TableOutputFormat}
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
 * 工具类，提供2个方法，分别从HBase表读取数据和向HBase表写入数据
 */
object HBaseTools {
	
	/**
	 * 依据指定表名称、列簇及列名称从HBase表中读取数据
	 * @param spark SparkSession 实例对象
	 * @param zks Zookerper 集群地址
	 * @param port Zookeeper端口号
	 * @param table HBase表的名称
	 * @param family 列簇名称
	 * @param fields 列名称
	 */
	def read(
		        spark: SparkSession,
		        zks: String,
		        port: String,
		        table: String,
		        family: String,
		        fields: Seq[String]
	        ): DataFrame = {
		/*
		 def newAPIHadoopRDD[K, V, F <: NewInputFormat[K, V]](
		      conf: Configuration = hadoopConfiguration,
		      fClass: Class[F],
		      kClass: Class[K],
		      vClass: Class[V]
		  ): RDD[(K, V)]
		 */
		val sc: SparkContext = spark.sparkContext
		
		// 1. 读取配置信息，加载HBaseClient配置（主要ZK地址和端口号）
		val conf: Configuration = HBaseConfiguration.create()
		conf.set("hbase.zookeeper.quorum", zks)
		conf.set("hbase.zookeeper.property.clientPort", port)
		// 2. 设置表的名称
		conf.set(TableInputFormat.INPUT_TABLE, table)
		// TODO: 设置读取列簇和列名称
		val scan: Scan = new Scan()
		// 设置列簇
		val cfBytes: Array[Byte] = Bytes.toBytes(family)
		scan.addFamily(cfBytes)
		// 设置列
		fields.foreach{field =>
			scan.addColumn(cfBytes, Bytes.toBytes(field))
		}
		// 设置Scan过滤数据: 将Scan对象转换为String
		conf.set(
			TableInputFormat.SCAN, //
			Base64.encodeBytes(ProtobufUtil.toScan(scan).toByteArray) //通过查看源码,改写,转换为String
		)

		// 3. 从HBase表加载数据
		val datasRDD: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(
			conf, //
			classOf[TableInputFormat], //
			classOf[ImmutableBytesWritable], //
			classOf[Result]
		)
		
		//===============================================
		//  DataFrame = RDD[Row] + Schema
		//===============================================
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
		
		// 5. 自定义Schema
		val schema: StructType = StructType(
			fields.map{field =>
				StructField(field, StringType, nullable = true)
			}
		)
		// 6. 使用SparkSession将RowsRDD和Schema组成为DataFrame
		spark.createDataFrame(rowsRDD, schema)
	}
	
	
	/**
	 * 将DataFrame数据保存到HBase表中
	 *
	 * @param dataframe 数据集DataFrame
	 * @param zks Zk地址
	 * @param port 端口号
	 * @param table 表的名称
	 * @param family 列簇名称
	 * @param rowKeyColumn RowKey字段名称
	 */
	def write(
		         dataframe: DataFrame,
		         zks: String,
		         port: String,
		         table: String,
		         family: String,
		         rowKeyColumn: String
	         ): Unit = {
		/*
		  def saveAsNewAPIHadoopFile(
			  path: String,
			  keyClass: Class[_],
			  valueClass: Class[_],
			  outputFormatClass: Class[_ <: NewOutputFormat[_, _]],
			  conf: Configuration = self.context.hadoopConfiguration): Unit
		 */
		// 1. 设置HBase依赖Zookeeper相关配置信息
		val conf: Configuration = HBaseConfiguration.create()
		conf.set("hbase.zookeeper.quorum", zks)
		conf.set("hbase.zookeeper.property.clientPort", port)
		// 2. 数据写入表的名称
		conf.set(TableOutputFormat.OUTPUT_TABLE, table)
		
		// 3. 将DataFrame中数据转换为RDD[(RowKey, Put)]
		val cfBytes: Array[Byte] = Bytes.toBytes(family)
		val columns: Array[String] = dataframe.columns // 从DataFrame中获取列名称
		val datasRDD: RDD[(ImmutableBytesWritable, Put)] = dataframe.rdd.map{ row =>
			// TODO: row 每行数据 转换为 二元组(RowKey, Put)
			// a. 获取RowKey值
			val rowKey: String = row.getAs[String](rowKeyColumn)
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

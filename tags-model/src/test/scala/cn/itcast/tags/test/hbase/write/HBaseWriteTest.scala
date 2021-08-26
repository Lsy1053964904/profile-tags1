package cn.itcast.tags.test.hbase.write

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 将画像标签数据保存至HBase表：htb_tags
 */
object HBaseWriteTest {
	
	def main(args: Array[String]): Unit = {
		
		// a. 构建SparkContext实例对象
		val sparkConf = new SparkConf()
			.setAppName("SparkHBaseWrite")
			.setMaster("local[4]")
			// 设置使用Kryo序列
			.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
			// 注册哪些类型使用Kryo序列化, 最好注册RDD中类型
			.registerKryoClasses(
				Array(classOf[ImmutableBytesWritable], classOf[Put])
			)
		val sc: SparkContext = new SparkContext(sparkConf)
		
		// b. 模拟数据集
		val tagsRDD: RDD[(String, String)] = sc.parallelize(
			List(
				("1001", "gender:男,job:教师"), ("1002", "gender:女,job:工人"),  //
				("1003", "gender:男,job:学生"), ("1004", "gender:男,job:工人") //
			),
			numSlices = 2 //
		)
		
		// TODO：将RDD数据保存到HBase表中，要求RDD数据类型为二元组，Key：ImmutableBytesWritable， Value：Put
		/*
			HBase表：htb_tags
				RowKey：userId
				CF：user
				Column：tagName
			create 'htb_tags', 'user'
		 */
		val datasRDD: RDD[(ImmutableBytesWritable, Put)] = tagsRDD.map{case(userId, tags) =>
			// a. 构建RowKey
			val rowKey: Array[Byte] = Bytes.toBytes(userId)
			// b. 构建put对象
			val put = new Put(rowKey)
			// 设置列
			put.addColumn(
				Bytes.toBytes("user"), Bytes.toBytes("userId"), Bytes.toBytes(userId)
			)
			tags.split(",").foreach{tag =>
				val Array(field, value) = tag.split(":")
				put.addColumn(
					Bytes.toBytes("user"), Bytes.toBytes(field), Bytes.toBytes(value)
				)
			}
			
			// 返回二元组
			(new ImmutableBytesWritable(rowKey), put)
		}
		
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
		conf.set("hbase.zookeeper.quorum", "bigdata-cdh01.itcast.cn")
		conf.set("hbase.zookeeper.property.clientPort", "2181")
		conf.set("zookeeper.znode.parent", "/hbase")
		// 2. 数据写入表的名称
		conf.set(TableOutputFormat.OUTPUT_TABLE, "htb_tags")
		//
		datasRDD.saveAsNewAPIHadoopFile(
			s"datas/hbase/output-${System.nanoTime()}", //
			classOf[ImmutableBytesWritable], //
			classOf[Put], //
			classOf[TableOutputFormat[ImmutableBytesWritable]], //
			conf
		)
		
		// 应用结束，关闭资源
		sc.stop()
	}
	
}
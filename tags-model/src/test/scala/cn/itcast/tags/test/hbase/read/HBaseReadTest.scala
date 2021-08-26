package cn.itcast.tags.test.hbase.read

import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration}
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 演示：SparkCore如何从HBase表读取数据
 */
object HBaseReadTest {
	
	def main(args: Array[String]): Unit = {
		
		// 创建SparkContext实例对象
		val sparkConf = new SparkConf()
			.setMaster("local[4]")
			.setAppName("HBaseReadTest")
			// 设置使用Kryo序列
			.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
			// 注册哪些类型使用Kryo序列化, 最好注册RDD中类型
			.registerKryoClasses(Array(classOf[ImmutableBytesWritable], classOf[Result]))
		val sc: SparkContext = SparkContext.getOrCreate(sparkConf)
		
		// 读取数据
		/*
		  def newAPIHadoopRDD[K, V, F <: NewInputFormat[K, V]](
		      conf: Configuration = hadoopConfiguration,
		      fClass: Class[F],
		      kClass: Class[K],
		      vClass: Class[V]
		  ): RDD[(K, V)]
		 */
		// 1. 读取配置信息，加载HBaseClient配置（主要ZK地址和端口号）
		val conf = HBaseConfiguration.create()
		conf.set("hbase.zookeeper.quorum", "bigdata-cdh01.itcast.cn")
		conf.set("hbase.zookeeper.property.clientPort", "2181")
		conf.set("zookeeper.znode.parent", "/hbase")
		// 2. 设置表的名称
		conf.set(TableInputFormat.INPUT_TABLE, "tbl_tag_users")
		// 3. 从HBase表加载数据
		val hbaseRDD: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(
			conf, //
			classOf[TableInputFormat], //
			classOf[ImmutableBytesWritable], //
			classOf[Result]
		)
		println(s"count = ${hbaseRDD.count()}")
		
		hbaseRDD.take(2).foreach{ case(_, result) =>
			println(s"RowKey = ${Bytes.toString(result.getRow)}")
			for(cell <- result.rawCells()){
				// 列簇CF
				val cf = Bytes.toString(CellUtil.cloneFamily(cell))
				// 列名称
				val column = Bytes.toString(CellUtil.cloneQualifier(cell))
				// 列的值
				val value = Bytes.toString(CellUtil.cloneValue(cell))
				println(s"\t ${cf}:${column} = ${value}, version -> ${cell.getTimestamp}")
			}
		}
		
		// 应用结束，关闭资源
		sc.stop()
	}
	
}
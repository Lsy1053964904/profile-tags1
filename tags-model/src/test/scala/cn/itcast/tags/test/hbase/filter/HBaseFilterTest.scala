package cn.itcast.tags.test.hbase.filter

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, ResultScanner, Scan, Table}
import org.apache.hadoop.hbase.filter.{CompareFilter, SingleColumnValueFilter}
import org.apache.hadoop.hbase.util.Bytes

/**
 * 从HBase表中读取数据，设置过滤条件：modified < '2019-09-01'
 */
object HBaseFilterTest {
	
	def main(args: Array[String]): Unit = {
		
		//  1. 设置HBase中Zookeeper集群信息
		val conf: Configuration = new Configuration()
		conf.set("hbase.zookeeper.quorum", "bigdata-cdh01.itcast.cn")
		conf.set("hbase.zookeeper.property.clientPort", "2181")
		
		// 2. 获取Connection连接对象
		val conn: Connection = ConnectionFactory.createConnection(conf)
		
		// 3. 获取Table句柄
		val table: Table = conn.getTable(TableName.valueOf("tbl_tag_orders"))
		
		// 4. 创建扫描器，设置过滤条件
		val scan: Scan = new Scan()
		// 过滤条件
		// SELECT COUNT(1) AS total FROM tbl_tag_orders WHERE modified < '2019-09-01' ;
		val filter: SingleColumnValueFilter = new SingleColumnValueFilter(
			Bytes.toBytes("detail"),
			Bytes.toBytes("modified"), //
			CompareFilter.CompareOp.LESS,
			Bytes.toBytes("2019-09-01") //
		)
		scan.setFilter(filter)
		// 必须添加过滤条件的列， TODO： 先获取列的值，再按照过滤条件过滤
		scan.addColumn(Bytes.toBytes("detail"), Bytes.toBytes("modified"))
		
		// 设置获取列名称
		scan.addColumn(Bytes.toBytes("detail"), Bytes.toBytes("memberid"))
		scan.addColumn(Bytes.toBytes("detail"), Bytes.toBytes("paymentcode"))
		
		
		// 5. 查询扫描数据
		val resultScanner: ResultScanner = table.getScanner(scan)
		import scala.collection.JavaConverters._

		
		// 6. 关闭资源
		resultScanner.close()
		table.close()
		conn.close()
	}
	
}
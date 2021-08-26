package cn.itcast.tags.test

import cn.itcast.tags.up.OozieParam
import cn.itcast.tags.up.OozieUtils.{genProperties, start, store, uploadConfig}

object OozieUtilsTest {
	/**
	 * 调用方式展示
	 */
	def main(args: Array[String]): Unit = {

		val param = OozieParam(
			0,
			"org.apache.spark.examples.SparkPi",
			"hdfs://bigdata-cdh01.itcast.cn:8020/apps/tags/models/tag_0/lib/model.jar",
			"",
			"2021-08-17T18:21+0800",
			"2021-08-17T18:27+0800"
		)
		val prop = genProperties(param)
		uploadConfig(param.modelId)
		store(param.modelId, prop)
		start(prop)
	}
}

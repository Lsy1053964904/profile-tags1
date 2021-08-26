package cn.itcast.tags.tools

import cn.itcast.tags.config.ModelConfig
import cn.itcast.tags.utils.HdfsUtils
import org.apache.hadoop.conf.Configuration
import org.apache.spark.internal.Logging
import org.apache.spark.ml.{Model, Pipeline, PipelineModel}
import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, DecisionTreeClassifier}
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, MulticlassClassificationEvaluator}
import org.apache.spark.ml.feature.{VectorAssembler, VectorIndexer}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.tuning.{CrossValidator, CrossValidatorModel, ParamGridBuilder, TrainValidationSplit, TrainValidationSplitModel}
import org.apache.spark.sql.DataFrame
import org.apache.spark.storage.StorageLevel

/**
 * 算法模型工具类：专门依据数据集训练算法模型，保存及加载
 */
object MLModelTools extends Logging {
	
	/**
	 * 加载模型，如果模型不存在，使用算法训练模型
	 * @param dataframe 训练数据集
	 * @param mlType 表示标签模型名称
	 * @return Model 模型
	 */
	def loadModel(dataframe: DataFrame, mlType: String, clazz: Class[_]): Model[_] = {
		// 获取算法模型保存路径
		val modelPath = s"${ModelConfig.MODEL_BASE_PATH}/${clazz.getSimpleName.stripSuffix("$")}"
		
		// 1. 判断模型是否存在，存在直接加载
		val conf: Configuration = dataframe.sparkSession.sparkContext.hadoopConfiguration
		if(HdfsUtils.exists(conf, modelPath)){
			logWarning(s"正在从【$modelPath】加载模型.................")
			
			mlType.toLowerCase match {     //转换为小写,以防大小写匹配错误
				case "rfm" => KMeansModel.load(modelPath)  // 加载返回
				case "rfe" => KMeansModel.load(modelPath)  // 加载返回
				case "psm" => KMeansModel.load(modelPath)  // 加载返回
				case "usg" => PipelineModel.load(modelPath)
			}
		}else{
			// 2. 如果模型不存在训练模型，获取最佳模型及保存模型
			logWarning(s"正在训练模型.................")
			val bestModel = mlType.toLowerCase match {
				case "rfm" => trainBestKMeansModel(dataframe, kClusters = 5)
				case "rfe" => trainBestKMeansModel(dataframe, kClusters = 4)
				case "psm" => trainBestKMeansModel(dataframe, kClusters = 5)
				case "usg" => trainBestPipelineModel(dataframe)
			}
			
			// 保存模型
			logWarning(s"保存最佳模型.................")
			bestModel.save(modelPath)
			
			// 返回模型
			bestModel
		}
	}
	
	/**
	 * 调整算法超参数，获取最佳模型
	 * @param dataframe 数据集
	 * @return
	 */
	def trainBestKMeansModel(dataframe: DataFrame, kClusters: Int): KMeansModel = {
		// TODO：模型调优方式二：调整算法超参数 -> MaxIter 最大迭代次数, 使用训练验证模式完成
		// 1.设置超参数的值
		val maxIters: Array[Int] = Array(5, 10, 20)
		// 2.不同超参数的值，训练模型
		dataframe.persist(StorageLevel.MEMORY_AND_DISK)
		val models: Array[(Double, KMeansModel, Int)] = maxIters.map{ maxIter =>
			// a. 使用KMeans算法应用数据训练模式
			val kMeans: KMeans = new KMeans()
				.setFeaturesCol("features")
				.setPredictionCol("prediction")
				.setK(kClusters) // 设置聚类的类簇个数
				.setMaxIter(maxIter)
				.setSeed(31) // 实际项目中，需要设置值
			// b. 训练模式
			val model: KMeansModel = kMeans.fit(dataframe)
			// c. 模型评估指标WSSSE
			val ssse = model.computeCost(dataframe)
			// d. 返回三元组(评估指标, 模型, 超参数的值)
			(ssse, model, maxIter)
		}
		dataframe.unpersist()
		models.foreach(println)
		// 3.获取最佳模型
		val (_, bestModel, _) = models.minBy(tuple => tuple._1)
		// 4.返回最佳模型
		bestModel
	}
	
	
	/**
	 * 采用K-Fold交叉验证方式，调整超参数获取最佳PipelineModel模型
	 * @param dataframe 数据集
	 * @return
	 */
	def trainBestPipelineModel(dataframe: DataFrame): PipelineModel = {
		// a. 特征向量化
		val assembler: VectorAssembler = new VectorAssembler()
			.setInputCols(Array("color", "product"))
			.setOutputCol("raw_features")
		
		// b. 类别特征进行索引
		val vectorIndexer: VectorIndexer = new VectorIndexer()
			.setInputCol("raw_features")
			.setOutputCol("features")
			.setMaxCategories(30)
		
		// c. 构建决策树分类器
		val dtc: DecisionTreeClassifier = new DecisionTreeClassifier()
			.setFeaturesCol("features")
			.setLabelCol("label")
			.setPredictionCol("prediction")
		
		// 构建Pipeline管道对象，组合模型学习器（算法）和转换器（模型）
		val pipeline: Pipeline = new Pipeline()
			.setStages(Array(assembler, vectorIndexer, dtc))
		
		// TODO: 创建网格参数对象实例，设置算法中超参数的值
		val paramGrid: Array[ParamMap] = new ParamGridBuilder()
			.addGrid(dtc.impurity, Array("gini", "entropy"))
			.addGrid(dtc.maxDepth, Array(5, 10))
			.addGrid(dtc.maxBins, Array(32, 64))
			.build()
		
		// f. 多分类评估器
		val evaluator = new MulticlassClassificationEvaluator()
			.setLabelCol("label")
			.setPredictionCol("prediction")
			// 指标名称，支持：f1、weightedPrecision、weightedRecall、accuracy
			.setMetricName("accuracy")
		
		// TODO: 创建交叉验证实例对象，设置算法、评估器和数据集占比
		val cv: CrossValidator = new CrossValidator()
			.setEstimator(pipeline) // 设置算法，此处为管道
			.setEvaluator(evaluator) // 设置模型评估器
			.setEstimatorParamMaps(paramGrid) // 设置算法超参数
			// TODO: 将数据集划分为K份，其中1份为验证数据集，其余K-1份为训练收集，通常K>=3
			.setNumFolds(3)
		
		// 传递数据集，训练模型
		val cvModel: CrossValidatorModel = cv.fit(dataframe)
		
		// TODO： 获取最佳模型
		val pipelineModel: PipelineModel = cvModel.bestModel.asInstanceOf[PipelineModel]
		
		// 返回获取最佳模型
		pipelineModel
	}
	
	
}

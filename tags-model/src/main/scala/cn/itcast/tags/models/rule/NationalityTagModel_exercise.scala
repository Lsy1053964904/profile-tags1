package cn.itcast.tags.models.rule

import cn.itcast.tags.models.{AbstractModel, ModelType}
import cn.itcast.tags.tools.TagTools
import cn.itcast.tags.utils.SparkUtils
import org.apache.spark.sql.DataFrame


class NationalityTagModel_exercise extends AbstractModel("国籍标签", ModelType.MATCH) {
  override def doTag(businessDF: DataFrame, tagDF: DataFrame): DataFrame = {

    import businessDF.sparkSession.implicits._
    //    businessDF.printSchema()
    //    businessDF.show(10,truncate = false)
    //
    //    tagDF.printSchema()
    //    tagDF.filter($"level" === 5).show(10,truncate = false)
    val modelDF: DataFrame = TagTools.ruleMatchTag(businessDF, "nationality", tagDF)
    modelDF.show(10,truncate = false)
    modelDF
//    null
  }
}


object NationalityTagModel_exercise {
  def main(args: Array[String]): Unit = {
    import scala.collection.JavaConverters._
    val model_exercise = new NationalityTagModel_exercise
    model_exercise.executeModel(333L)
  }
}

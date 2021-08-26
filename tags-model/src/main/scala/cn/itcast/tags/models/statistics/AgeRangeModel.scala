package cn.itcast.tags.models.statistics

import cn.itcast.tags.models.{AbstractModel, ModelType}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

class AgeRangeModel extends AbstractModel("年龄段标签", ModelType.STATISTICS) {
  /*
  338	年龄段
    339	50后		19500101-19591231
    340	60后		19600101-19691231
    341	70后		19700101-19791231
    342	80后		19800101-19891231
    343	90后		19900101-19991231
    344	00后		20000101-20091231
    345	10后		20100101-20191231
    346	20后		20200101-20291231
   */
  override def doTag(businessDF: DataFrame, tagDF: DataFrame): DataFrame = {
    import businessDF.sparkSession.implicits._

    /*
      root
       |-- id: string (nullable = true)
       |-- birthday: string (nullable = true)
     */
    //businessDF.printSchema()
    /*
    +---+----------+
    |id |birthday  |
    +---+----------+
    |1  |1992-05-31|
    |10 |1980-10-13|
    |100|1993-10-28|
    |101|1996-08-18|
     */
    //businessDF.show(10, truncate = false)

    /*
    root
     |-- id: long (nullable = false)
     |-- name: string (nullable = true)
     |-- rule: string (nullable = true)
     |-- level: integer (nullable = true)
     */
    //tagDF.printSchema()
    /*
      +---+----+-----------------+-----+
      |id |name|rule             |level|
      +---+----+-----------------+-----+
      |339|50后 |19500101-19591231|5    |
      |340|60后 |19600101-19691231|5    |
      |341|70后 |19700101-19791231|5    |
      |342|80后 |19800101-19891231|5    |
      |343|90后 |19900101-19991231|5    |
      |344|00后 |20000101-20091231|5    |
      |345|10后 |20100101-20191231|5    |
      |346|20后 |20200101-20291231|5    |
      +---+----+-----------------+-----+
     */
    //tagDF.filter($"level" === 5).show(10, truncate = false)

    // 1. 自定义UDF函数，解析属性标签规则rule
    val rule_to_tuple: UserDefinedFunction = udf(
      (rule: String) => {
        val Array(start, end) = rule.trim.split("-").map(_.toInt)
        (start, end)
      }
    )

    // 2. 针对属性标签数据中规则rule使用UDF函数，提取start和end
    val attrTagRuleDF: DataFrame = tagDF
      .filter($"level" === 5) // 5级标签
      .select(
        $"name", rule_to_tuple($"rule").as("rules")
      )
      .select(
        $"name", $"rules._1".as("start"), $"rules._2".as("end")
      )
    attrTagRuleDF.printSchema()
    attrTagRuleDF.show(10, truncate = false)

    // 3. 使用业务数据字段：birthday 与 属性标签规则数据进行JOIN关联
    /*
      attrTagRuleDF： t2
      businessDF: t1
      SELECT t2.userId, t1.name FROM business t1 JOIN  attrTagRuleDF t2
      WHERE t2.start <= t1.birthday AND t2.end >= t1.birthday ;
     */
    val modelDF: DataFrame = businessDF
      // a. 使用正则函数，转换日期
      .select(
        $"id", //
        regexp_replace($"birthday", "-", "")
          .cast(IntegerType).as("bornDate")
      )
      // b. 关联属性标签规则数据
      .join(attrTagRuleDF)
      // c. 设置条件，使用WHERE语句
      .where(
        $"bornDate".between($"start", $"end")
      )
      // d. 选取字段值
      .select(
        $"id".as("userId"), //
        $"name".as("agerange") //
      )
//    modelDF.printSchema()
//    modelDF.show(100, truncate = false)

    // 返回画像标签数据
    		modelDF
//    null
  }
}

object AgeRangeModel {
  def main(args: Array[String]): Unit = {
    val tagModel = new AgeRangeModel()
    tagModel.executeModel(339L)
  }
}
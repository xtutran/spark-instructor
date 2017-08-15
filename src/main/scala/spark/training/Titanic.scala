package spark.training

import org.apache.spark.SparkContext
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.RFormula
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType}

/**
  * Created by tran.xuantu on 12/13/2016.
  */
object Titanic extends MainTrait {
  override def createJob(args: Array[String], sc: SparkContext): Unit = {
    sc.getConf.setAppName("Titanic Prediction")

    val sqlContext = new SQLContext(sc)

    val df = readCsv(sqlContext, "data/titanic/train.csv", header = true)

    val avgAge = df.select(avg("Age")).first()
    val fillAge =  when(col("Age").notEqual(0), col("Age")).otherwise(avgAge(0))
    val fillEmbarked = when(col("Embarked").notEqual(""), col("Embarked")).otherwise("S")

    val df2 = df.withColumn("Age", fillAge)
      .withColumn("Embarked", fillEmbarked)
      .withColumn("Pclass", col("Pclass").cast(StringType))
    df2.show()
    val trans = new RFormula()
      .setFormula("Survived ~ Sex + Pclass + SibSp  + Embarked")
      .setFeaturesCol("features")
      .setLabelCol("label")

    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.01)

    val pipeline = new Pipeline().setStages(Array(trans, lr))

    // We use a ParamGridBuilder to construct a grid of parameters to search over.
    // With 3 values for hashingTF.numFeatures and 2 values for lr.regParam,
    // this grid will have 3 x 2 = 6 parameter settings for CrossValidator to choose from.
    val paramGrid = new ParamGridBuilder()
      .addGrid(lr.regParam, Array(0.1, 0.01))
      .build()

    // We now treat the Pipeline as an Estimator, wrapping it in a CrossValidator instance.
    // This will allow us to jointly choose parameters for all Pipeline stages.
    // A CrossValidator requires an Estimator, a set of Estimator ParamMaps, and an Evaluator.
    // Note that the evaluator here is a BinaryClassificationEvaluator and its default metric
    // is areaUnderROC.
    val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(new BinaryClassificationEvaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(10) // Use 3+ in practice

    val cvModel = cv.fit(df2)

    // Read test data
    var df3 = readCsv(sqlContext, "data/titanic/test.csv", header = true)
    df3 = df3.withColumn("Age", fillAge)
      .withColumn("Embarked", fillEmbarked)
      .withColumn("Pclass", col("Pclass").cast(StringType))

    writeCsv(cvModel
      .transform(df3)
      .select(col("PassengerId"), col("prediction").cast(IntegerType).alias("Survived")),
      path = "mydata.csv")
  }
}

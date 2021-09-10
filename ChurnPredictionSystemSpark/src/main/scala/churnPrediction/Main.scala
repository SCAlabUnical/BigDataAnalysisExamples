package churnPrediction

import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object Main {
  val schema: StructType = StructType(Array(
    StructField("State", StringType, nullable = true),
    StructField("AccountLength", IntegerType, nullable = true),
    StructField("AreaCode", StringType, nullable = true),
    StructField("InternationalPlan", StringType, nullable = true),
    StructField("VoiceMailPlan", StringType, nullable = true),
    StructField("NumberVmailMessages", DoubleType, nullable = true),
    StructField("TotalDayMinutes", DoubleType, nullable = true),
    StructField("TotalDayCalls", DoubleType, nullable = true),
    StructField("TotalDayCharge", DoubleType, nullable = true),
    StructField("TotalEveMinutes", DoubleType, nullable = true),
    StructField("TotalEveCalls", DoubleType, nullable = true),
    StructField("TotalEveCharge", DoubleType, nullable = true),
    StructField("TotalNightMinutes", DoubleType, nullable = true),
    StructField("TotalNightCalls", DoubleType, nullable = true),
    StructField("TotalNightCharge", DoubleType, nullable = true),
    StructField("TotalIntlMinutes", DoubleType, nullable = true),
    StructField("TotalIntlCalls", DoubleType, nullable = true),
    StructField("TotalIntlCharge", DoubleType, nullable = true),
    StructField("CustomerServiceCalls", DoubleType, nullable = true),
    StructField("Churn", StringType, nullable = true)
  ))

  case class User(State: String, AccountLength: Integer, AreaCode: String,
                  InternationalPlan: String, VoiceMailPlan: String, NumberVmailMessages: Double,
                  TotalDayMinutes: Double, TotalDayCalls: Double, TotalDayCharge: Double,
                  TotalEveMinutes: Double, TotalEveCalls: Double, TotalEveCharge: Double,
                  TotalNightMinutes: Double, TotalNightCalls: Double, TotalNightCharge: Double,
                  TotalIntlMinutes: Double, TotalIntlCalls: Double, TotalIntlCharge: Double,
                  CustomerServiceCalls: Double, Churn: String)

  def main(args: Array[String]): Unit = {

    /** Fetch data from file or from a data warehouse */

    /** From file */
    //    val spark: SparkSession = SparkSession.builder().appName("churn").config("spark.master", "local[*]").getOrCreate()
    //    import spark.implicits._

    //    val filepath: String = "churn-bigml-80.csv"
    //    val textRDD = spark.sparkContext.textFile("file://"+filepath, 8)
    //    val header = textRDD.first()
    //    val data: RDD[User] = textRDD.filter(line => line != header) map {
    //      line =>
    //        val col = line.split(",")
    //        User(col(0), col(1).toInt, col(2), col(3), col(4), col(5).toDouble, col(6).toDouble, col(7).toDouble, col(8).toDouble, col(9).toDouble, col(10).toDouble, col(11).toDouble, col(12).toDouble, col(13).toDouble,
    //          col(14).toDouble, col(15).toDouble, col(16).toDouble, col(17).toDouble, col(18).toDouble, col(19))
    //    }

    /** From Hive datawarehouse */
    // Build and configure the SparkSession
    val spark: SparkSession = SparkSession.builder().appName("churn").config("spark.master", "local[*]").enableHiveSupport().getOrCreate()
    import spark.sql

    sql("CREATE TABLE IF NOT EXISTS Users" +
      "(State STRING, AccountLength INT, AreaCode INT, InternationalPlan STRING, VoiceMailPlan STRING ,NumberVmailMessages INT," +
      "TotalDayMinutes DOUBLE, TotalDayCalls DOUBLE, TotalDayCharge DOUBLE, TotalEveMinutes DOUBLE, TotalEveCalls DOUBLE, TotalEveCharge DOUBLE," +
      "TotalNightMinutes DOUBLE, TotalNightCalls DOUBLE, TotalNightCharge DOUBLE, TotalIntlMinutes DOUBLE, TotalIntlCalls DOUBLE, TotalIntlCharge DOUBLE," +
      "CustomerServiceCalls INT, Churn STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE")
    sql("LOAD DATA LOCAL INPATH '/home/user/IdeaProjects/Churn_Prediction_System_Spark/src/main/resources/churn-bigml-80.csv' INTO TABLE Users")

    val data: RDD[User] = sql("SELECT * FROM Users").rdd.map(x => x(0).toString.split(",")).map(col => User(col(0), col(1).toInt, col(2), col(3), col(4), col(5).toDouble,
      col(6).toDouble, col(7).toDouble, col(8).toDouble, col(9).toDouble, col(10).toDouble, col(11).toDouble, col(12).toDouble, col(13).toDouble, col(14).toDouble, col(15).toDouble, col(16).toDouble, col(17).toDouble, col(18).toDouble, col(19)))

    // Cache data for improving performance
    data.cache()

    /** Preprocessing data */

    def numericalFeature: String => Double = (categoricalFeature: String) => {
      val encodeMap: Map[String, Double] = Map("Yes" -> 1.0, "No" -> 0.0, "True" -> 1.0, "False" -> 0.0)
      encodeMap(categoricalFeature)
    }

    val points: RDD[LabeledPoint] = data.map(x =>
      LabeledPoint(numericalFeature(x.Churn),
        Vectors.dense(Array(x.AreaCode.toDouble, x.AccountLength.toDouble, x.CustomerServiceCalls, numericalFeature(x.InternationalPlan), x.NumberVmailMessages, x.TotalDayCalls, x.TotalDayCharge,
          x.TotalDayMinutes, x.TotalEveCalls, x.TotalEveCharge, x.TotalEveMinutes, x.TotalIntlCalls, x.TotalIntlCharge, x.TotalIntlMinutes, x.TotalNightCalls,
          x.TotalNightCharge, x.TotalNightMinutes, numericalFeature(x.VoiceMailPlan))))
    )
    val scaler = new StandardScaler(withMean = true, withStd = true).fit(points.map(x => x.features))
    val preprocessedData = points.map(x => LabeledPoint(x.label, scaler.transform(Vectors.dense(x.features.toArray))))


    /** Setup the parameters of the decision tree for the training phase */
    // Split the data into training and test sets (30% held out for testing)
    val splits = preprocessedData.randomSplit(Array(0.7, 0.3))
    val (training_set, test_set) = (splits(0), splits(1))

    //  Empty categoricalFeaturesInfo indicates all features are continuous
    val numClasses = 2
    val categoricalFeaturesInfo = Map[Int, Int]()
    val impurity = "gini"
    val maxDepth = 5
    val maxBins = 32

    val model = DecisionTree.trainClassifier(training_set, numClasses, categoricalFeaturesInfo,
      impurity, maxDepth, maxBins)

    model.save(spark.sparkContext, "DTModel")

    /** Evaluate model on test set */
    val predictionAndLabels = test_set.map { case LabeledPoint(label, features) =>
      val prediction = model.predict(features)
      (prediction, label)
    }

    val testErr = predictionAndLabels.filter(r => r._1 != r._2).count().toDouble / test_set.count()
    val metrics = new BinaryClassificationMetrics(predictionAndLabels)
    val precision = metrics.precisionByThreshold
    val recall = metrics.recallByThreshold

    /** Classify unclassified instances */

    // Load data of unclassified customers from a data warehouse

    // val warehouseLocation = new File("spark-warehouse").getAbsolutePath
    //    val spark: SparkSession = SparkSession.builder().appName("ChurnPredictionSystemDeployment").config("spark.sql.warehouse.dir", warehouseLocation)
    //      .enableHiveSupport().getOrCreate()
    //    import spark.sql

    val unclassCustomers: RDD[User] = sql("SELECT * FROM Users").rdd
      .map(x => x(0).toString.split(","))
      .map(col => User(col(0), col(1).toInt, col(2), col(3), col(4), col(5).toDouble,
        col(6).toDouble, col(7).toDouble, col(8).toDouble, col(9).toDouble, col(10).toDouble,
        col(11).toDouble, col(12).toDouble, col(13).toDouble, col(14).toDouble, col(15).toDouble,
        col(16).toDouble, col(17).toDouble, col(18).toDouble, col(19)))

    // Load the trained model from disk
    val modelTrained = DecisionTreeModel.load(spark.sparkContext, "DTModel")

    // Process real data as for training
    val pointsUnclass: RDD[LabeledPoint] = unclassCustomers.map(x =>
      LabeledPoint(numericalFeature(x.Churn),
        Vectors.dense(Array(x.AreaCode.toDouble, x.AccountLength.toDouble, x.CustomerServiceCalls, numericalFeature(x.InternationalPlan), x.NumberVmailMessages, x.TotalDayCalls, x.TotalDayCharge,
          x.TotalDayMinutes, x.TotalEveCalls, x.TotalEveCharge, x.TotalEveMinutes, x.TotalIntlCalls, x.TotalIntlCharge, x.TotalIntlMinutes, x.TotalNightCalls,
          x.TotalNightCharge, x.TotalNightMinutes, numericalFeature(x.VoiceMailPlan))))
    )

    val preprocessedUnclassCustomers = pointsUnclass.map(x => LabeledPoint(x.label, scaler.transform(Vectors.dense(x.features.toArray))))

    // Run model on real instances and find out if a customer will churn or not
    val predictions = preprocessedUnclassCustomers.map(x => modelTrained.predict(x.features))
  }
}



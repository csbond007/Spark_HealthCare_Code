
import com.google.common.collect.ImmutableMap;


//// Basic Spark Library
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

//// Spark-Cassandra Connector Library
import com.datastax.spark.connector._

//// Spark-SQL Library
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.udf

//// MLLib (Spark Machine Learning Libraries)
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.mllib.tree.GradientBoostedTrees
import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import org.apache.spark.mllib.tree.model.GradientBoostedTreesModel

object Main {
	def main(args: Array[String]) {

		val conf = new SparkConf(true)
				.set("spark.cassandra.connection.host", "10.10.40.138")
				.setAppName(this.getClass.getSimpleName)
				//	.setMaster("spark://10.10.40.138:7077")
				// .setMaster("mesos://zk://10.10.40.138:2181/mesos")

				val sc =  new SparkContext(conf)

				// Spark-SQL Context
				val sqlCtx = new SQLContext(sc)

				/**
				val emr_labscorepopulated_rdd = sc.cassandraTable("emrbots_data", "emr_labscorepopulated")

				println("/////////////////////////////////////// " + emr_labscorepopulated_rdd.count())

				// Total number of records in this table = 107535277
				emr_labscorepopulated_rdd.take(200000).foreach(println)
				 **/

				val df = sqlCtx.read
				.format("org.apache.spark.sql.cassandra")
				.options(Map( "table" -> "protocol_subject_109", "keyspace" -> "remote_monitoring_uci"))
				.load()

				// Remove Time_Stamp Column here as it is non-important attribute
				val df_minus_timestamp = df.drop("time_stamp")

				/**
				 * Imputation Code has to go here
				 */

				// HACK - Replaced value NA with 141
				//val df1 = df_minus_timestamp.distinct()
				
				
				val df1 = df_minus_timestamp.na.replace("heart_rate", ImmutableMap.of("NA", "0"));
        val df_imputed = df1.filter("heart_rate != 0")
		 
        println("&&&&&&&&&&&&&&&&&&&&&&&&&  =  " + df_imputed.count())
		//df_imputed.
		
		def encodeLabel=udf((activity_id: Double) => {
			activity_id match {
			 case 1 => 0.0 
			case 2 => 1.0
			case 3 => 2.0
			case 4 => 3.0
			case 5 => 4.0
			case 6 => 5.0
			case 7 => 6.0
			case 9 => 7.0
			case 10 => 8.0
			case 11 => 9.0
			case 12 => 10.0
			case 13 => 11.0
			case 16 => 12.0
			case 17 => 13.0
			case 18 => 14.0
			case 19 => 15.0
			case 20 => 16.0
			case 24 => 17.0
			//case 0 => 18.0
			case _ => 18.0
			}})

				def toVec4 =udf((heart_rate: Double,
						IMU_hand_temperature: Double) => {
							Vectors.dense(heart_rate,
									IMU_hand_temperature)})


				val df_mod = df_imputed.withColumn(
						"features",
						toVec4(
								df_imputed("heart_rate"),
								df_imputed("IMU_hand_temperature"))
						).withColumn("label", encodeLabel(df_imputed("activity_id")))
				.select("features", "label")

				df_mod.show(10)   

				val splits = df_mod.randomSplit(Array(0.7, 0.3))

				val (training_split, test_split) = (splits(0), splits(1))

				val trainingData = training_split.rdd.map(row => LabeledPoint(
						row.getAs[Double]("label"),
						row.getAs[org.apache.spark.mllib.linalg.Vector]("features")
						))

				println("+++++++++++++++++++++++++++++++++++++++++++++++++++++" + trainingData.count())	

				val testData = test_split.rdd.map(row => LabeledPoint(
						row.getAs[Double]("label"),
						row.getAs[org.apache.spark.mllib.linalg.Vector]("features")
						))

			//	trainingData.cache()
			//	testData.cache()

				// Train a DecisionTree model.
				//  Empty categoricalFeaturesInfo indicates all features are continuous.
				val numClasses = 19
				val categoricalFeaturesInfo = Map[Int, Int]()
				val impurity = "entropy"
				val maxDepth = 3
				val maxBins = 5

				// Random Forest Stuff
				val numTrees = 100 // Use more in practice.
				val featureSubsetStrategy = "auto" // Let the algorithm choose.

				 val model = RandomForest.trainClassifier(trainingData, numClasses, 
				 categoricalFeaturesInfo,numTrees, 
				 featureSubsetStrategy, impurity, maxDepth, maxBins)

				// val model = DecisionTree.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo,
						// impurity, maxDepth, maxBins)

				// Train a GradientBoostedTrees model.
				// The defaultParams for Classification use LogLoss by default.
				val boostingStrategy = BoostingStrategy.defaultParams("Classification")
				boostingStrategy.numIterations = 3 // Note: Use more iterations in practice.
				boostingStrategy.treeStrategy.numClasses = 3
				boostingStrategy.treeStrategy.maxDepth = 3
				// Empty categoricalFeaturesInfo indicates all features are continuous.
				boostingStrategy.treeStrategy.categoricalFeaturesInfo = Map[Int, Int]()

				// val model = GradientBoostedTrees.train(trainingData, boostingStrategy)

				val labelAndPreds = testData.map { point =>
				val prediction = model.predict(point.features)
				(point.label, prediction)
		}

		val testErr = labelAndPreds.filter(r => r._1 != r._2).count().toDouble / testData.count()
		println("Prediction Accuracy ////////////////////////////////////////////// = " + (100-testErr*100) + "%")
			//	println("Test Error //////////////////////////////////////////////= " + testErr)
				//println("Learned classification tree model/////////////////////////:\n" + model.toDebugString)

				//////////////////////////////////////////////////////////////////////////////////////////////////////////////

				// Working 107535277
				// emr_labscorepopulated_data.take(50000).foreach(println)

				sc.stop()
		}

	}


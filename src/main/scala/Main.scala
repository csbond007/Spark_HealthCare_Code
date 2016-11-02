
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

				val emr_labscorepopulated_rdd = sc.cassandraTable("emrbots_data", "emr_labscorepopulated")
				
				println("/////////////////////////////////////// " + emr_labscorepopulated_rdd.count())
				
				// Total number of records in this table = 107535277
				emr_labscorepopulated_rdd.take(200000).foreach(println)

				/**
		  // Spark-SQL Context
				val sqlCtx = new SQLContext(sc)

				val df = sqlCtx.read
				.format("org.apache.spark.sql.cassandra")
				.options(Map( "table" -> "data", "keyspace" -> "iris"))
				.load()

				def encodeLabel=udf((flower: String) => {
					flower match {
					case "Iris-setosa" => 0.0
					case "Iris-versicolor" => 1.0
					case "Iris-virginica" => 2.0
					//case _ => 0.0
					}})

				def toVec4 =udf((sepal_length: Double, sepal_width: Double,petal_length: Double,petal_width: Double) => {
					Vectors.dense(sepal_length, sepal_width, petal_length,petal_width)
				})

				val df_mod = df.withColumn(
						"features",
						toVec4(
								df("sepal_length"),
								df("sepal_width"),
								df("petal_length"),
								df("petal_width")
								)
						).withColumn("label", encodeLabel(df("flower"))).select("features", "label")

						df_mod.show()

				val splits = df_mod.randomSplit(Array(0.01, 0.99))

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

				trainingData.cache()
				testData.cache()

				// Train a DecisionTree model.
				//  Empty categoricalFeaturesInfo indicates all features are continuous.
				val numClasses = 3
				val categoricalFeaturesInfo = Map[Int, Int]()
				val impurity = "entropy"
				val maxDepth = 3
				val maxBins = 5

				// Random Forest Stuff
				val numTrees = 1 // Use more in practice.
        val featureSubsetStrategy = "auto" // Let the algorithm choose.

       val model = RandomForest.trainClassifier(trainingData, numClasses, 
                              categoricalFeaturesInfo,numTrees, 
                               featureSubsetStrategy, impurity, maxDepth, maxBins)

			//	val model = DecisionTree.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo,
				//		impurity, maxDepth, maxBins)

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
				println("Test Error //////////////////////////////////////////////= " + testErr)
				println("Learned classification tree model/////////////////////////:\n" + model.toDebugString)

				//////////////////////////////////////////////////////////////////////////////////////////////////////////////
				 */
				// Working 107535277
				// emr_labscorepopulated_data.take(50000).foreach(println)

				sc.stop()
	}

}


package main

import utils.AppConfiguration
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import utils.GenerateReport.generateReport
import utils.Tools._


object Main {

  def main(args: Array[String]): Unit = {

        if (args.length != 1) {
          throw new IllegalArgumentException("You have to pass the following arguments: Path to config file")
        }

    val sparkSession = org.apache.spark.sql.SparkSession.builder
      .appName("sparkSessionDataScienceAPP").master("local[*]")
      .getOrCreate
    sparkSession.conf.set("spark.sql.session.timeZone", "GMT")
    sparkSession.sparkContext.setLogLevel("ERROR")

    import sparkSession.implicits._

    AppConfiguration.initializeAppConfig(args(0))

    ////////////////////////////////////////////////////////////////////////
    println(
      """
        |    //////////////////////////////////
        |    //// IMPORT AND PREPROCESSING ////
        |    //////////////////////////////////
      """.stripMargin)

    val Database = importData(sparkSession)

    // Separate Numeric and no Numeric features
    val NoNumFeaturesColNames = mutable.ArrayBuffer[String]()
    val NumFeaturesColNames = mutable.ArrayBuffer[String]()
    Database.schema.map(col => {
      if(col.dataType == StringType){
        NoNumFeaturesColNames += (col.name.toString)
      } else {
        NumFeaturesColNames += (col.name.toString)
      }
    })

    // Define Features names
    val idxdNoNumFeaturesColNames = NoNumFeaturesColNames.map(_ + "Indexed")
    val allFeaturesColNames = NumFeaturesColNames ++ NoNumFeaturesColNames
    val allIdxdFeaturesColNames = NumFeaturesColNames ++ idxdNoNumFeaturesColNames
    val features = "features"
    val allData = Database.select(allFeaturesColNames.map(col): _*)

    // Encode from string to int Features
    val stringIndexers = NoNumFeaturesColNames.map { colName =>
      new StringIndexer()
        .setInputCol(colName)
        .setOutputCol(colName + "Indexed")
    }

    // Vector assembler
    val assembler = new VectorAssembler()
      .setInputCols(Array(allIdxdFeaturesColNames: _*))
      .setOutputCol(features)

    ////////////////////////////////////////////////////////////////////////
    println(
      """
        |    /////////////////////////////////////////////
        |    //// SILHOUETTE METHOD TO FIND OPTIMAL K ////
        |    ////////////////////////////////////////////
      """.stripMargin)

    // Pipeline
    val pipeline = new Pipeline().setStages((stringIndexers :+ assembler).toArray)
    val processData = pipeline.fit(allData).transform(allData).persist()

    // Evaluate clustering by computing Silhouette score
    val evaluator = new ClusteringEvaluator()

    // Create Result class to store scores for each K
    case class Result(k : Int, silhouette : Double) extends Serializable
    val resSilhouette = new ListBuffer[Result]()

    // Loop to test each K and compute associated Silhouette score
    //    for (i <- 2 to AppConfiguration.maxKtoCompute.toInt) {
    for (i <- 2 to AppConfiguration.maxKtoCompute.toInt) {
      val km = new KMeans()
        .setK(i)
        .setFeaturesCol("features")
        .setPredictionCol("prediction")
        .setSeed(1L)
      val model = km.fit(processData)
      val predictionResult = model.transform(processData)
      val silhouette = evaluator.evaluate(predictionResult)
      resSilhouette. +=(new Result(i, silhouette))
    }

    // Extract optimal clustering K
    val optimalK = resSilhouette.map(x => {(x.k,x.silhouette)})
      .toDF("K","Score")
      .select(bround(col("Score"),2).as("Score"), col("K"))
      .withColumn("BestScore", max("Score").over(Window.partitionBy()))
      .filter(col("Score")===col("BestScore"))
      .withColumn("K", min(col("K")).over(Window.partitionBy())).select("K")
      .take(1)(0)(0).asInstanceOf[Int]

    ////////////////////////////////////////////////////////////////////////
    println(
      """
        |    ////////////////////////////////////////////////////
        |    //// APPLY KMEANS CLUSTERING ALG WITH OPTIMAL K ////
        |    ////////////////////////////////////////////////////
      """.stripMargin)
    // Apply clustering
    val optimalKmeans = new KMeans()
      .setK(optimalK)
      .setFeaturesCol("features")
      .setPredictionCol("prediction")
      .setSeed(1L)
    val model = optimalKmeans.fit(processData)
    val resultClustering = model.transform(processData).drop("features").drop(idxdNoNumFeaturesColNames: _*)
      .withColumnRenamed("prediction","Cluster").persist()

    // Save output
        resultClustering.repartition(1).write.format("csv").option("header","true").save(AppConfiguration.outputPath + "/ClusteringResult")

    processData.unpersist()

    println(
      """
        |    /////////////////////////
        |    //// GENERATE REPORT ////
        |    /////////////////////////
      """.stripMargin)

    // Clustering report
    val reportingDataframe = generateReport(resultClustering, NoNumFeaturesColNames , NumFeaturesColNames)

    // Save output
        reportingDataframe.repartition(1).write.format("csv").option("header","true").save(AppConfiguration.outputPath + "/ReportResult")

  }
}
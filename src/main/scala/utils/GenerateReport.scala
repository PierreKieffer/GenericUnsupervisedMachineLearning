package utils

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{callUDF, col, count, lit}

import scala.collection.mutable.ArrayBuffer

object GenerateReport {
  def generateReport(resultClustering : DataFrame, NoNumFeaturesColNames : ArrayBuffer[String], NumFeaturesColNames : ArrayBuffer[String]) : DataFrame ={

    val resultClusteringForReport = resultClustering.withColumn("ClusterNum",col("Cluster"))

    ////////////////////////////////////////////
    //// Generate report for no numeric features
    val NoNumDataFrame = resultClusteringForReport.select("Cluster", NoNumFeaturesColNames: _*)

    val columnsModifyNoNumDataFrame = NoNumDataFrame.columns.map(col).map(colName => {
      val w = Window.partitionBy("Cluster",colName.toString())
      val Name = colName.toString() +"_count"
      count(colName.toString()).over(w) as(s"$Name")
    })

    val columnsInitialNoNumDataFrame = NoNumDataFrame.columns.map(col).map(colName => {
      col(colName.toString) as(s"$colName")
    })

    val colsreportNoNumDataFrame = columnsInitialNoNumDataFrame ++ columnsModifyNoNumDataFrame

    ////////////////////////////////////////////
    //// Generate report for numeric features
    val NumDataFrame = resultClusteringForReport.select("ClusterNum",NumFeaturesColNames: _*)
    val columnsModifyNumDataFrame = NumDataFrame.columns.map(col).map(colName => {
      val w = Window.partitionBy("ClusterNum")
      val Name = colName.toString() +"_median"
      callUDF("percentile_approx", col(colName.toString()), lit(0.5)).over(w) as(s"$Name")
    })

    val columnsInitialNumDataFrame = NumDataFrame.columns.map(col).map(colName => {
      col(colName.toString) as(s"$colName")
    })

    ////////////////////////////////////////////
    //// Combine results
    val colsreportNumDataFrame = columnsInitialNumDataFrame ++ columnsModifyNumDataFrame

    val reportCols = colsreportNoNumDataFrame ++ colsreportNumDataFrame
    val reportDataFrameInterm = resultClusteringForReport.select(reportCols: _*)

    ////////////////////////////////////////////
    //// Pourcentage for no numeric features
    val countColumnsNames = NoNumFeaturesColNames.map(_ + "_count")
    val pourcentageColumns = reportDataFrameInterm.select("Cluster_count", countColumnsNames: _*)
      .columns.map(col).map(colName => {
      val Name = colName.toString() +"PourcentageCluster"
      colName*lit(100) /col("Cluster_count") as(s"$Name")
    })

    val columnsReportDataFrameInterm = reportDataFrameInterm.columns.map(col).map(colName => {
      col(colName.toString) as(s"$colName")
    })

    val finalReportColumns = pourcentageColumns ++ columnsReportDataFrameInterm

    val reportDataFrame =  reportDataFrameInterm.select(finalReportColumns: _*)
      .drop("ClusterNum","ClusterNum_median","Cluster_countPourcentageCluster")
      .withColumn("TotalRecordsInRun", count("Cluster").over(Window.partitionBy()))
      .withColumn("ClusterPourcentage", Tools.pourcentageUDF(col("Cluster_count"),col("TotalRecordsInRun")))
      .drop(NumFeaturesColNames :_*)
      .distinct()


    reportDataFrame

  }

}

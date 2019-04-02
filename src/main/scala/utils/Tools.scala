package utils

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf

object Tools {

  def importData(sparkSession: SparkSession): DataFrame ={

    val inputFormat = AppConfiguration.inputFormat
    val inputPath = AppConfiguration.inputPath + "/*." + inputFormat

    if (inputFormat == "csv") {
      val Database = sparkSession.read.format("csv").option("header","true").load(inputPath)
      Database
    } else {
      val Database = sparkSession.read.parquet(inputPath)
      Database
    }
  }

  def pourcentage(value : Long, total : Long) : Long ={
    return (value*100)/total
  }

  val pourcentageUDF = udf(pourcentage _)

}

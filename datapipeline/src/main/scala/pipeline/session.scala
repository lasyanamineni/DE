package scala.pipeline

import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import org.apache.spark.sql.SparkSession

object session {
  //Creates spark session
  lazy val spark = getSpark()
  def getSpark(): SparkSession = {
    SparkSession.builder().getOrCreate()
  }
}

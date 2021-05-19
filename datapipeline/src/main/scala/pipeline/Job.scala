package scala.pipeline

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext

trait Job {
  val logger = Logger.getLogger(this.getClass)
  val spark:SparkSession = session.getSpark()
  val sc:SparkContext =  spark.sparkContext
  def start(): Unit = {
    run()
  }
//run would be defined for all the classes that extends Job
  def run()

}

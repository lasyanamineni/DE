package scala.pipeline
import scala.io.{reader, writer}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, lit, unix_timestamp}
import org.apache.spark.graphx.{Edge, Graph}


object graph_processing extends Job{

  def run(): Unit = {
    val df = loadDataFrame
    writer(df:DataFrame,"swr_rpt", "test", "delta", "overwrite")
  }

  def transformNodes(ss: DataFrame): DataFrame = {
    //assign unique id to each name
    import spark.implicits._
    ss.distinct
      .rdd.map(_.getAs[String]("NAME"))
      .zipWithUniqueId().toDF("NAME", "ID")
  }

  def transformEdges(ss: DataFrame, nodes: DataFrame): DataFrame = {
    // recalculate the edges with unique Id from nodes
    ss.join(nodes.withColumnRenamed("ID", "ID1")
      .join(nodes.withColumnRenamed("ID", "ID2")
      .select("ID1", "ID2")
  }

  def createGraph(nodes: DataFrame, edges: DataFrame): Graph[String, Int] = {
    //returns a graph with nodes, edges
    val n = nodes.rdd.map { r => (r.getAs[Long]("ID"), r.getAs[String]("Name")) }.coalesce(1).cache
    val e = edges.rdd.map { r => Edge(r.getAs[Long]("ID1"), r.getAs[Long]("ID2"), 1) }.coalesce(1).cache
    Graph(n, e, "")
  }

  def FindConnected(df1: DataFrame,df2:DataFrame):DataFrame= {
    import spark.implicits._
    val nodes = transformNodes(df1).cache //get distinct nodes
    val edges = transformEdges(df2, nodes).cache
    val g = createGraph(nodes, edges)
    val c = g.connectedComponents.vertices.toDF("ID1", "ID2") //finds the connected components
    c.join(nodes, col("ID1")===col("ID")).drop("ID").withColumnRenamed("Name","Name1")
      .join(nodes, col("ID2")===col("ID")).drop("ID").withColumnRenamed("Name","Name2")
  }

  def loadDataFrame: DataFrame = {
    val nodesDf = reader("nodesdb")
    val edgesDf = reader("edgesdb")
    val connected_components = FindConnected(nodesDf,edgesDf)

    connected_components
  }
}

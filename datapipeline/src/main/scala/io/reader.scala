package scala.io
import scala.pipeline.session
import org.apache.spark.sql.DataFrame
object reader {

  //reads the data from given hive table
  protected def load(tableName: String): DataFrame = {
    val df = session.getSpark.table(tableName)
    df
  }
  def apply(tableName: String): DataFrame =
    load(tableName)

}

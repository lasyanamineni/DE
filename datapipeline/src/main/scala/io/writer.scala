package scala.io
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Row}

object writer {
  //writes data to given path and creates an external table
  protected def save(df: DataFrame, db: String, tbl: String, mode: String = "overwrite", format: String = "parquet", partitionCol: String = ""): Unit = {
    if (partitionCol.isEmpty) {
      df.write.mode(mode).
        format(format).
        saveAsTable(s"$db.$tbl")
    }
    else {
      df.write.mode(mode).
        format(format).
        partitionBy(partitionCol).
        saveAsTable(s"$db.$tbl")
    }
    df.write.mode(mode).
      format(format).
      partitionBy(partitionCol).
      saveAsTable(s"$db.$tbl")
  }
  def apply(df: DataFrame, db: String, tbl: String, format: String, mode: String): Unit =
    save(df, db, tbl, format, mode)
}
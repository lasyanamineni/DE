package scala.pipeline
import org.apache.log4j.{Level, Logger}

object driver extends Serializable {
  def main(args: Array[String]): Unit = {
    //Get the pipeline name to run the required job
    val logger = Logger.getLogger(driver.getClass)
    if (args.length == 0) {
      throw new Exception("no job name")
    }
    else{
      this._jobSubmit(logger,args(0))
    }
  }
  private def _jobSubmit(logger: Logger,sJobName: String): Unit = try {
    logger.log(Level.INFO, "Pipeline Initiated for (%s)".format(sJobName))
    Class.forName(sJobName).newInstance().asInstanceOf[Job].start()
    logger.log(Level.INFO, "Pipeline Completed for Job (%s)".format(sJobName))
  }
  catch {
    case e: Exception =>
      logger.log(Level.ERROR, " (%s) job failed with error - (%s)".format(sJobName, e.getMessage))
      throw e
  }
}

# DE
* DataPipeline has a framework to run spark jobs.
* graph_processing.scala takes nodes, edges of a graph as input and gives connected components as output using spark graphx api. 
* /src/main/scala/pipeline/session.scala initiates spark session
* /src/main/scala/io/reader.scala reads data from data lake
* /src/main/scala/io/writer.scala writes data to data lake
* /src/main/scala/pipeline/driver.scala takes the job name as an argument , runs spark submit



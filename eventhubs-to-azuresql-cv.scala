// Databricks notebook source
dbutils.widgets.text("eventhub-consumergroup", "azuresql", "Event Hubs consumer group")
dbutils.widgets.text("eventhub-maxEventsPerTrigger", "1000", "Event Hubs max events per trigger")
dbutils.widgets.text("azuresql-servername", "servername")
dbutils.widgets.text("azuresql-stagingtable", "[dbo].[staging_table]")
dbutils.widgets.text("azuresql-finaltable", "[dbo].[rawdata]")
dbutils.widgets.text("azuresql-etlstoredproc", "[dbo].[stp_WriteDataBatch]")

// COMMAND ----------

import org.apache.spark.eventhubs._

val eventHubsConf = EventHubsConf(dbutils.secrets.get(scope = "MAIN", key = "event-hubs-read-connection-string"))
  .setConsumerGroup(dbutils.widgets.get("eventhub-consumergroup"))
  .setStartingPosition(EventPosition.fromStartOfStream)
  .setMaxEventsPerTrigger(dbutils.widgets.get("eventhub-maxEventsPerTrigger").toLong)

val eventhubs = spark.readStream
  .format("eventhubs")
  .options(eventHubsConf.toMap)
  .load()

// COMMAND ----------

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

val schema = StructType(
  StructField("eventId", StringType, false) ::
  //StructField("complexData", StructType((0 to 22).map(i => StructField(s"moreData$i", DoubleType, false)))) ::
  StructField("complexData", StringType, false) ::
  StructField("value", StringType, false) ::
  StructField("type", StringType, false) ::
  StructField("deviceId", StringType, false) ::
  StructField("deviceSequenceNumber", LongType, false) ::
  StructField("createdAt", TimestampType, false) :: Nil)

val dataToWrite = eventhubs
  .select(from_json(decode($"body", "UTF-8"), schema).as("eventData"), $"*")
  .select($"eventData.*", $"enqueuedTime".as("enqueuedAt"))
  .select('eventId.as("EventId"), 'Type, 'DeviceId, 'DeviceSequenceNumber, 'CreatedAt, 'Value, 'ComplexData, 'EnqueuedAt)

// COMMAND ----------

// Helper method to retry an operation up to n times with exponential backoff
@annotation.tailrec
final def retry[T](n: Int, backoff: Int)(fn: => T): T = {
  Thread.sleep(((scala.math.pow(2, backoff) - 1) * 1000).toLong)
  util.Try { fn } match {
    case util.Success(x) => x
    case _ if n > 1 => retry(n - 1, backoff + 1)(fn)
    case util.Failure(e) => throw e
  }
}

// COMMAND ----------

val serverName = dbutils.widgets.get("azuresql-servername")
val stagingTable =dbutils.widgets.get("azuresql-stagingtable")
val destinationTable = dbutils.widgets.get("azuresql-finaltable")
val etlStoredProc = dbutils.widgets.get("azuresql-etlstoredproc")

val jdbcUrl = s"jdbc:sqlserver://$serverName.database.windows.net;database=streaming"
val connectionProperties = new java.util.Properties()
connectionProperties.put("user", "serveradmin")
connectionProperties.put("password", dbutils.secrets.get(scope = "MAIN", key = "azuresql-pass"))
connectionProperties.setProperty("Driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")

val numPartitions = retry (6, 0) {
  val conn = java.sql.DriverManager.getConnection(jdbcUrl, connectionProperties)
  // This stored procedure merges data in batch from staging_table to the final table
  // ensuring deduplication.
  val getPartitionCount = conn.prepareStatement("select count(*) from sys.dm_db_partition_stats where object_id = object_id(?) and index_id < 1")
  getPartitionCount.setString(1, destinationTable)
  val getPartitionCountRs = getPartitionCount.executeQuery()
  getPartitionCountRs.next
  val numPartitions = getPartitionCountRs.getInt(1)
  getPartitionCountRs.close
  getPartitionCount.close
  conn.close
  if (numPartitions>1) numPartitions else 1
}

// COMMAND ----------

import java.util.UUID.randomUUID
import org.apache.spark.sql._
import org.apache.spark.sql.DataFrame
import java.time.Instant
import java.sql.Timestamp

val generateUUID = udf(() => randomUUID().toString)

var writeDataBatch : java.sql.PreparedStatement = null

def upsertToDelta(microBatchOutputDF: DataFrame, batchId: Long) {
  
      microBatchOutputDF
     .withColumn("PartitionId", pmod(hash('DeviceId), lit(numPartitions)))
     .withColumn("ProcessedAt", lit(Timestamp.from(Instant.now)))
     .select("EventId", "Type", "DeviceId", "DeviceSequenceNumber", "CreatedAt", "Value","ComplexData", "EnqueuedAt", "ProcessedAt", "PartitionId")
     .write.mode("append")
     .jdbc(jdbcUrl, "RawIOTData", connectionProperties)
}

val WriteToSQLQuery  = dataToWrite
  .writeStream
  .option("checkpointLocation", "dbfs:/streaming_at_scale/checkpoints/eventhubs-to-azuresql")
  .foreachBatch(upsertToDelta _)
  .outputMode("update")

var streamingQuery = WriteToSQLQuery.start()

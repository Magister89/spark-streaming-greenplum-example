import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{from_json, to_date}
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.types.{StringType, StructType}

object App extends App {
  val spark = SparkSession
    .builder
    .appName("streamingOmnichannelChat")
    .config("spark.master", "local[*]")
    .getOrCreate()

  val timeInterval = "1 minutes"
  val kafkaInstanceOrigin = "10.52.3.72:9092"
  val topicOrigin = "ing.omnichannel.chat"

  import spark.implicits._

  val df = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", kafkaInstanceOrigin)
    .option("subscribe", topicOrigin)
    .option("startingOffsets", "earliest")
    .load()

  val schemaJson = (new StructType)
    .add("header", (new StructType)
      .add("eventId", StringType)
      .add("sourceSystem", StringType)
      .add("eventNotificationTime", StringType)
      .add("sourceSystemTime", StringType)
      .add("eventType", StringType)
      .add("entityToBeUpdated", StringType)
      .add("entityId", StringType)
      .add("ver", StringType))
    .add("data", (new StructType)
      .add("id", StringType)
      .add("rifCliente", StringType)
      .add("ambito", StringType)
      .add("dataInizioEvento", StringType)
      .add("dataFineEvento", StringType))

  val jsonDf = df.select(from_json($"value".cast("string"), schemaJson) as "value")

  val tableDf = jsonDf
    .select($"value.header.eventId" as "event_id",
      $"value.header.sourceSystem" as "source_system",
      $"value.header.eventNotificationTime" as "event_notification_time",
      $"value.header.sourceSystemTime" as "source_system_time",
      $"value.header.eventType" as "event_type",
      $"value.header.entityToBeUpdated" as "entity_to_be_updated",
      $"value.header.entityId" as "entity_id",
      $"value.header.ver" as "ver",
      $"value.data.id" as "id",
      $"value.data.rifCliente" as "rif_cliente",
      $"value.data.ambito" as "ambito",
      $"value.data.dataInizioEvento" as "data_inizio_evento",
      $"value.data.dataFineEvento" as "data_fine_evento",
      to_date($"value.header.eventNotificationTime", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'") as "partition_date")

  val output = tableDf
    .writeStream.foreachBatch { (batchDF: DataFrame, batchId: Long) =>
    val portNumber = (batchId + 50000).toString
    val gscWriteOptionMap = Map(
      "url" -> "jdbc:postgresql://10.52.4.60:5432/test",
      "user" -> "gpadmin",
      "password" -> "Changeme007!",
      "dbschema" -> "test_gpkafka",
      "dbtable" -> "test_spark_chat",
      "server.port" -> portNumber
    )
    batchDF.write.format("greenplum")
      .options(gscWriteOptionMap)
      .mode(SaveMode.Append).save()
  }
    .trigger(Trigger.ProcessingTime(timeInterval))
    .start()


  output.awaitTermination()
}

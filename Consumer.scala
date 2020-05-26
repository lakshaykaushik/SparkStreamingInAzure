val TOPIC = "ingestevents"
val kafkaBrokers = "testsparkstreaming.servicebus.windows.net:9093"
val connection_string = "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$ConnectionString\" password=\"Endpoint=sb://testsparkstreaming.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=z1JAKV+wRRt7KtY+rGAgCkcH5ez5ZuqD9WJmfVkAl9s=\";"


import kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule
// READ STREAM USING SPARK's KAFKA CONNECTOR
val kafka = spark.readStream
    .format("kafka")
    .option("subscribe", TOPIC)
    .option("kafka.bootstrap.servers", kafkaBrokers)
    .option("kafka.sasl.mechanism", "PLAIN")
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.jaas.config", connection_string)
    .option("kafka.request.timeout.ms", "60000")
    .option("kafka.session.timeout.ms", "60000")
    .option("failOnDataLoss", "false")
    .option("startingOffsets", "latest")
    .option("kafka.group.id", "$Default")
    .load()

    import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{min, max}
import org.apache.spark.sql.Row

val dataSchema = StructType(Seq(StructField("metric",StringType,true)))

val kafkaData = kafka
    .selectExpr("CAST(value as string)")
    .select(from_json($"value", dataSchema).as("json"))
    .selectExpr("json.metric")
    .select($"metric".cast(IntegerType).as("int_col"))
    

kafkaData.agg(max("int_col")).writeStream.outputMode("update").format("console").option("truncate", false).start().awaitTermination()

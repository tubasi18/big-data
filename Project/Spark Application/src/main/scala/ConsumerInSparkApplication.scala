import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.BasicConfigurator
import org.apache.log4j.varia.NullAppender
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.KafkaUtils.createDirectStream

object ConsumerInSparkApplication {

  def main(args: Array[String]): Unit = {

    val nullAppender = new NullAppender
    BasicConfigurator.configure(nullAppender)

    val conf = new SparkConf().setMaster("local[2]").setAppName("KafkaConsumer")
    val ssc = new StreamingContext(conf, Seconds(2))

    //Kafka Configuration
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",  // Server to connect with producer
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],// to know how to Deserialization the array [byte]  as string
      "group.id" -> "use_a_separate_group_id_for_each_stream", // if we have topic receives data and more than producer write
      // in same topic we can make a group of consumer and give it the same key
      "auto.offset.reset" -> "latest", //  The latest you write, read it (the latest data is arrived read it )
      "enable.auto.commit" -> (false: java.lang.Boolean),
    )

    val topics = Array("twitter") // name of topic to read form it
    val stream = createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    ) // scc is the reader  , PreferConsistent distribute partitions evenly across all executors in a consistent manner
    // Subscribe  Connect the consumer with Kafka and start pulling data form topic

    val spark = SparkSession.builder.getOrCreate()

    import spark.implicits._


    //The code takes streaming data from Kafka, converts it into a structured DataFrame,
    // performs data cleaning, and adds the cleaned records to a local MongoDB collection named "bigdata.tweets."
    stream.foreachRDD(rdd =>
      if (!rdd.isEmpty()) {
        val splitRdd = rdd.map(_.value().split(","))
          .map(x => (x(0), x(1), x(2), x(3), System.currentTimeMillis()))


        val df = splitRdd.toDF("id","date","user","text" ,"producedTweetTime" )
        df.show(10)
        sys.exit(0)
        val cleanedDF = cleanDataFrame(df)

//       cleanedDF.show(10)

        cleanedDF.write
          .format("mongo")
          .mode("append")
          .option("uri", "mongodb://localhost:27017/bigdata.tweets")
          .save()
      }
    )
    ssc.start()
    ssc.awaitTermination()
    //so it stays up and keeps running  the Kafka every 2 seconds
  }

  private def cleanDataFrame(df: DataFrame): DataFrame = {
    df
      .withColumn("id", extractAndCast("id", "'id': '(\\d+)'", "int"))
      .withColumn("date", extractAndCast("date", "'date': '(.*?)'", "string"))
      .withColumn("user", extractAndCast("user", "'user': '(\\w+)'", "string"))
      .withColumn("text", cleanText("text", "'text': '|'}$"))
  }

  private def extractAndCast(columnName: String, pattern: String, dataType: String): Column =
    regexp_extract(col(columnName), pattern, 1).cast(dataType)

  private def cleanText(columnName: String, pattern: String): Column =
    regexp_replace(col(columnName), pattern, "")

}
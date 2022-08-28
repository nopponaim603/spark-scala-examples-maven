import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object kafkar {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    println("program started")

    val conf = new SparkConf().setMaster("local[4]").setAppName("kafkar")
    val ssc = new StreamingContext(conf, Seconds(2))

    // my kafka topic
    val kafkaStream = KafkaUtils.createStream(ssc,"localhost:2181","spark-streaming-consumer-group", Map("mytest" -> 5))

    kafkaStream.print()
    ssc.start
    ssc.awitTermination()

  }
}

//https://www.youtube.com/watch?v=2z6scTH_C4c&t=1230s&ab_channel=Intellipaat

import org.apache.log4j.BasicConfigurator
import org.apache.log4j.varia.NullAppender
import org.apache.spark.{SparkConf, SparkContext}

object main {
  def main(args: Array[String]): Unit = {
    BasicConfigurator.configure(new NullAppender())
    val conf = new SparkConf()
      .setAppName("FirstScalaSpark")
    //      .setMaster("local[*]") // You can specify your Spark cluster URL here
    val sc = new SparkContext(conf)



    val rdd = sc.textFile(args(0));
    val newRdd = rdd.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey((word1, word2) => word1 + word2)
    newRdd.collect().foreach(println);
  }
}

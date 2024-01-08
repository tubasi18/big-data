package home1

import org.apache.log4j.BasicConfigurator
import org.apache.log4j.varia.NullAppender
import org.apache.spark.{SparkConf, SparkContext}


object MAIN {
  def main(args: Array[String]): Unit = {
    BasicConfigurator.configure(new NullAppender)
    val conf = new SparkConf().setAppName("WordCount").setMaster("local")
    val sc = new SparkContext(conf)
    val input = sc.textFile("src/main/scala/home1/elonmusk.csv")
    val header = input.first()
    val dataWithOutHeader = input.filter(row => row != header)


    print("Enter keyword : ")
    val keywordInput = scala.io.StdIn.readLine()
    val keywords = keywordInput.split(",").map(x=>x.trim());

    val data = dataWithOutHeader
      .map(line => clearLine(line))
        .map(line=> (line.split(",")(1).split(" ")(1),line.split(",")(2)))
      .flatMap(line => line._2.split(" ")
      .map(word => ((line._1, regexForWord(word)), 1)))
      .reduceByKey(_ + _)
      .filter(x => keywords.contains(x._1._2))
      data.foreach(println)


    val totalTweets = dataWithOutHeader.count()


    val tweetsAtLeastOneInput = dataWithOutHeader
      .map(line => clearLine(line))
      .map(line => (line.split(",")(1).split(" ")(0), line.split(",")(2)))
      .flatMap(line => line._2.split(" ") )
      .filter(word => keywords.exists(keyword => regexForWord(word).contains(keyword)))
      .count()

    val percentageTweetsAtLeastOneInput = (tweetsAtLeastOneInput.toFloat / totalTweets) * 100
    println(s"Percentage of tweets have at least one input keywords: $percentageTweetsAtLeastOneInput%")


    val tweetsExactlyTwoInput = dataWithOutHeader
      .map(line => clearLine(line))
      .map(line => (line.split(",")(1).split(" ")(0), line.split(",")(2)))
      .filter(line => keywords.exists(keyword => line._2.split(" ")
      .count(word => regexForWord(word).contains(keyword)) == 2))
      .count()
    val percentageTweetsExactlyTwoInput = (tweetsExactlyTwoInput.toFloat / totalTweets) * 100
    println(s"Percentage of tweets with exactly two input keywords: $percentageTweetsExactlyTwoInput%")


    val tweetLengths = dataWithOutHeader
    .map(line => countWords(clearLine(line).split(",")(2)))
    val totalLength = tweetLengths.sum()
    val averageLength = totalLength.toFloat / totalTweets
    val variance = math.sqrt(tweetLengths.map(length => math.pow(length - averageLength, 2)).sum() / totalTweets)
    println(s"Standard Deviation: $variance")


  }
  private def regexForWord(str: String): String = {
    val string = str.replaceAll("[^a-zA-Z0-9]+", "").trim()
    string}

  private def clearLine(str: String): String = {
    val parts = str.split(",").map(_.trim)
    val string = parts.take(2).mkString(", ") + (if (parts.length > 2) ", " + parts.drop(2).mkString(" ") else "")
    string}
  private def countWords(line: String): Int = line.split("\\s+").length
}
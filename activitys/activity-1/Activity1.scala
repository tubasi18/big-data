package activitys

import org.apache.log4j.BasicConfigurator
import org.apache.log4j.varia.NullAppender
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SparkSession, functions}

import java.time.{LocalDate, Period}

object Activity1 {
  def main(args: Array[String]): Unit = {
    BasicConfigurator.configure(new NullAppender)

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("SparkDfDemo")
      .getOrCreate()

    import spark.implicits._

    val studentsDF = spark.read.option("header", "true").csv("stu/students.csv")

    //Task 1: split students into classes by studying status and count the number of students in each class
    val studentsClasses = studentsDF.groupBy("studying_status").agg(count("*").alias("student_count"))

    val studentsClasses1 = studentsDF.groupBy("studying_status").count()
    studentsClasses.show(10)

    //Task 2: view student first name, last name, and GPA for students who have graduated
    val student = studentsDF.filter(col("studying_status") === "graduated")
      .select("first_name", "last_name", "GPA")
    student.show(10)

    //Task 3: Calculate the mean of GPAs for students who are studying
    val GPAstudents = studentsDF.filter(col("studying_status") === "studying")
      .agg(mean("GPA").alias("mean_GPA_studying"))

    GPAstudents.show(10)
    //Task 4: Sort students by GPA in descending order
    val sortedByGPA = studentsDF.sort(col("GPA").desc)
    val desc = studentsDF.orderBy(functions.desc("GPA"))

    sortedByGPA.show(10)
    //Task 5: Add a new column 'age' calculated from birth date

    val studentsWithAgeDF = studentsDF.
      withColumn("age", datediff(current_date(), to_date(col("birth_date"), "yyyy-MM-dd")) / 365)


    val srt = (str: String) => {
      val date = LocalDate.parse(str)
      val dateNow = LocalDate.now()
      Period.between(date, dateNow).getYears
    }
    val dateUDF = udf(srt)
    val studentsWithAgeDF1 = studentsDF.
      withColumn("age", dateUDF($"birth_date"))

    studentsWithAgeDF1.show(10)

    //Task 6: show only the students who have a GPA greater than 3.7
    val gpas = studentsDF.filter(col("GPA") > "3.7")

    gpas.show();
  }
}

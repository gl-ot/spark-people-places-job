package site.trycatchers

import java.sql.Date
import java.text.SimpleDateFormat

import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.functions._

object PlaceSuggestionApp {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example") // todo rename
      .config("spark.master", "local")
      .getOrCreate()

    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._

    val peopleDF = spark.sparkContext.textFile("./people.txt")
      .map(_.split(","))
      .map(a =>
        Person(
          a(0).toLong,
          createDate(a(1), "yyyyMMdd_HH"),
          a(2).toDouble,
          a(3).toDouble,
          a(4).toInt,
          createDate(a(5), "yyyyMMdd")))
      .toDF()

    val placesDF = spark.sparkContext.textFile("./places.txt")
        .map(_.split(","))
        .map(a =>
          Place(a(0).toLong, a(1), a(2), a(3),
            a(4).toDouble,
            a(5).toDouble,
            a(6).toInt,
            createDate(a(7), "yyyyMMdd")))
        .toDF()
        .as("places")


    val uniquePlacesDF = placesDF
      .dropDuplicates("latitude", "longitude", "placeFirstDate")

    val placesPeople = peopleDF
      .join(broadcast(uniquePlacesDF), Seq("latitude", "longitude", "regionId"))

    val placePopularity = placesPeople
      .groupBy("placeId")
      .agg(count("*").as("popularity"))
      .as("placePopularity")

    val result = placesPeople
      .dropDuplicates("placeId", "personId")
      .join(broadcast(placePopularity), "placeId")
      .select("personId", "placeId", "popularity", "name",
        "description", "latitude", "longitude", "regionId", "placeFirstDate")

    result.rdd.map(_.toString()).saveAsTextFile("./suggestions")

    spark.stop()
  }

  private def createDate(date: String, format: String): Date = {
    new Date(new SimpleDateFormat(format).parse(date).getTime)
  }
}

case class Person(personId: Long,
                  stayTime: Date,
                  latitude: Double,
                  longitude: Double,
                  regionId: Long,
                  personFirstDate: Date)

case class Place(placeId: Long,
                 name: String,
                 category: String,
                 description: String,
                 latitude: Double,
                 longitude: Double,
                 regionId: Int,
                 placeFirstDate: Date)

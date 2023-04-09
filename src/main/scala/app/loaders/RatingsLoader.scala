package app.loaders

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * Helper class for loading the input
 *
 * @param sc The Spark context for the given application
 * @param path The path for the input file
 */
class RatingsLoader(sc : SparkContext, path : String) extends Serializable {

  /**
   * Read the rating file in the given path and convert it into an RDD
   *
   * @return The RDD for the given ratings
   */
  def load() : RDD[(Int, Int, Option[Double], Double, Int)] = {

    // Finding the path to retrieve data
    val resource = getClass.getResource(path).toString
    // Importing the data
    val rating_rdd = sc.textFile(resource)
    // Giving to each rate the structure described in the task (splitting and introducing optional old rate)
    val result = rating_rdd.map(x => {
      val fields = x.split('|')
      val id_client = fields(0).toInt
      val id_movie = fields(1).toInt
      val old_rating = if (fields.length == 5) Some(fields(2).toDouble) else None
      val new_rating = if (fields.length == 5) fields(3) else fields(2)
      val timestamp = if (fields.length == 5) fields(4) else fields(3)
      (id_client, id_movie, old_rating, new_rating.toDouble, timestamp.toInt)
    })
    // Caching the data to reduce overload
    result.cache
  }
}
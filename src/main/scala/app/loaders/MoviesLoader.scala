package app.loaders

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * Helper class for loading the input
 *
 * @param sc   The Spark context for the given application
 * @param path The path for the input file
 */
class MoviesLoader(sc: SparkContext, path: String) extends Serializable {

  /**
   * Read the title file in the given path and convert it into an RDD
   *
   * @return The RDD for the given titles
   */
  def load(): RDD[(Int, String, List[String])]= {

    val resource= getClass.getResource(path).toString
    val movie_rdd = sc.textFile(resource)
    val result = movie_rdd.map(x => {
      val fields = x.split('|')
      val id_client = fields(0).toInt
      val movie_name = fields(1).replaceAll("^\"|\"$","")
      val list = fields.toList.drop(2).map(x=> x.replaceAll("^\"|\"$",""))
      (id_client, movie_name,list)
    })
    result.cache()
  }
}


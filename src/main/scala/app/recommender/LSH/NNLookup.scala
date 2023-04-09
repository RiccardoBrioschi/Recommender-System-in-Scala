package app.recommender.LSH

import org.apache.spark.rdd.RDD

/**
 * Class for performing LSH lookups
 *
 * @param lshIndex A constructed LSH index
 */
class NNLookup(lshIndex: LSHIndex) extends Serializable {

  /**
   * Lookup operation for queries
   *
   * @param input The RDD of keyword lists
   * @return The RDD of (keyword list, result) pairs
   */
  def lookup(queries: RDD[List[String]])
  : RDD[(List[String], List[(Int, String, List[String])])] = {

    // For every list of string (genre) we compute the hashing
    val processed_queries = lshIndex.hash(queries)

    // Extracting similar movies depending on the hashing value
    val result = lshIndex.lookup(processed_queries).map(term => (term._2, term._3))
    result
  }
}

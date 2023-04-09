package app.recommender.LSH


import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag

/**
 * Class for indexing the data for LSH
 *
 * @param data The title data to index
 * @param seed The seed to use for hashing keyword lists
 */
class LSHIndex(data: RDD[(Int, String, List[String])], seed : IndexedSeq[Int]) extends Serializable {

  private val minhash = new MinHash(seed)

  /**
   * Hash function for an RDD of queries.
   *
   * @param input The RDD of keyword lists
   * @return The RDD of (signature, keyword list) pairs
   */
  def hash(input: RDD[List[String]]) : RDD[(IndexedSeq[Int], List[String])] = {
    input.map(x => (minhash.hash(x), x))
  }

  /**
   * Return data structure of LSH index for testing
   *
   * @return Data structure of LSH index
   */
  def getBuckets(): RDD[(IndexedSeq[Int], List[(Int, String, List[String])])] = {

    // We cluster movies sharing the same signature. More precisely, after computing the signature of each movie
    // based on their genres, we perform a groupBy on the signature
    val result = data.map(term => (minhash.hash(term._3),(term._1, term._2, term._3))).
      groupByKey().mapValues(elem => elem.toList)

    result
  }

  /**
   * Lookup operation on the LSH index
   *
   * @param queries The RDD of queries. Each query contains the pre-computed signature
   *                and a payload
   * @return The RDD of (signature, payload, result) triplets after performing the lookup.
   *         If no match exists in the LSH index, return an empty result list.
   */
  def lookup[T: ClassTag](queries: RDD[(IndexedSeq[Int], T)])
  : RDD[(IndexedSeq[Int], T, List[(Int, String, List[String])])] = {

    // We cluster the movies depending on their signature
    val processed_data = this.getBuckets()

    // Given the queries RDD (containing each list of genres and the corresponding signature), we assign every cluster
    // of movies to every query (in case no movies are assigned to a query, we assign it an empty list by default)
    val result = queries.leftOuterJoin(processed_data).
      mapValues { case (attributes, Some(movies)) => (attributes, movies)
    case (attributes, None) => (attributes, List())
    }

    // Returning the result in the correct format
    result.map(term => (term._1, term._2._1, term._2._2))
  }
}

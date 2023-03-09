package app.analytics

import org.apache.spark.HashPartitioner
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK
import org.joda.time.DateTime


class SimpleAnalytics() extends Serializable {

  private var ratingsPartitioner: HashPartitioner = null
  private var moviesPartitioner: HashPartitioner = null
  private var titlesGroupedById: RDD[(Int, Iterable[(Int,String,List[String])])] = null
  private var ratingsGroupedByYearByTitle:RDD[(Int, Map[Int,Iterable[(Int, Int, Option[Double], Double, Int)]])]   = null

  def init(
            ratings: RDD[(Int, Int, Option[Double], Double, Int)],
            movie: RDD[(Int, String, List[String])]
          ): Unit = {
    // We group movies by movie ID
    titlesGroupedById = movie.groupBy(x => x._1)
    // We group ratings based on year and ID, using dateTime to identify the year in which the rate was posted
    ratingsGroupedByYearByTitle = ratings.groupBy(rating => {
      val temp_year = new DateTime((rating._5.toLong) * 1000)
      val year = temp_year.year().get()
      year
    }).map(term=> (term._1, term._2.groupBy(elem => elem._2)))
    // I have to apply the partitioning
  }

  def getNumberOfMoviesRatedEachYear: RDD[(Int, Int)] = {

    // The number of movies corresponds to the length of the iterable (here size)
    val result = ratingsGroupedByYearByTitle.map(term => (term._1, term._2.size))
    result
  }

  def getMostRatedMovieEachYear: RDD[(Int, String)] = {

    // We create a first new RDD containing (movie_name,movie_ID) for each movie
    val first_rdd = titlesGroupedById.map(movie => (movie._1,movie._2.toList.head._2))
    // We create a second new RDD containing, for each year, the ID of the movie which received the largest number of ratings
    val second_rdd = ratingsGroupedByYearByTitle.map(term=> (term._2.map(elem=> (elem._1,elem._2.size)).maxBy(_._2), term._1)).map(movie=>
      (movie._1._1,movie._2))
    // We merge and return the result
    val joined_rdd = first_rdd.join(second_rdd).map(term=> (term._2._2, term._2._1))
    joined_rdd
  }

  def getMostRatedGenreEachYear: RDD[(Int, List[String])] = {
    // We create a first new RDD containing (movie_name,movie_ID) for each movie
    val first_rdd = titlesGroupedById.map(movie => (movie._1, movie._2.toList.head._3))
    // We create a second new RDD containing, for each year, the ID of the movie which received the largest number of ratings
    val second_rdd = ratingsGroupedByYearByTitle.map(term => (term._2.map(elem => (elem._1, elem._2.size)).maxBy(_._2), term._1)).map(movie =>
      (movie._1._1, movie._2))
    // We merge and return the result
    val joined_rdd = first_rdd.join(second_rdd).map(term => (term._2._2, term._2._1))
    joined_rdd
  }

  // Note: if two genre has the same number of rating, return the first one based on lexicographical sorting on genre.
  def getMostAndLeastRatedGenreAllTime: ((String, Int), (String, Int)) = ???

  /**
   * Filter the movies RDD having the required genres
   *
   * @param movies         RDD of movies dataset
   * @param requiredGenres RDD of genres to filter movies
   * @return The RDD for the movies which are in the supplied genres
   */
  def getAllMoviesByGenre(movies: RDD[(Int, String, List[String])],
                          requiredGenres: RDD[String]): RDD[String] = ???

  /**
   * Filter the movies RDD having the required genres
   * HINT: use the broadcast callback to broadcast requiresGenres to all Spark executors
   *
   * @param movies            RDD of movies dataset
   * @param requiredGenres    List of genres to filter movies
   * @param broadcastCallback Callback function to broadcast variables to all Spark executors
   *                          (https://spark.apache.org/docs/2.4.8/rdd-programming-guide.html#broadcast-variables)
   * @return The RDD for the movies which are in the supplied genres
   */
  def getAllMoviesByGenre_usingBroadcast(movies: RDD[(Int, String, List[String])],
                                         requiredGenres: List[String],
                                         broadcastCallback: List[String] => Broadcast[List[String]]): RDD[String] = ???

}


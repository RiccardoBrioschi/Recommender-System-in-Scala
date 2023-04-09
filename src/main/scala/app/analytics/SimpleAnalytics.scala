package app.analytics

import org.apache.spark.HashPartitioner
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK
import org.joda.time.DateTime


class SimpleAnalytics() extends Serializable {

  // Initializing helpful variables to avoid recomputation
  private var ratingsPartitioner: HashPartitioner = null
  private var moviesPartitioner: HashPartitioner = null
  private var titlesGroupedById: RDD[(Int, Iterable[(Int,String,List[String])])] = null
  private var ratingsGroupedByYearByTitle:RDD[(Int, Map[Int,Iterable[(Int, Int, Option[Double], Double, Int)]])]   = null

  def init(
            ratings: RDD[(Int, Int, Option[Double], Double, Int)],
            movie: RDD[(Int, String, List[String])]
          ): Unit = {

    // We group movies by movie ID (movieId is the first element in each tuple)
    titlesGroupedById = movie.groupBy(x => x._1).persist()
    // We group ratings based on year and ID, using dateTime to identify the year in which the rate has been posted
    // In order to retrieve the year we use a standard Java package
    ratingsGroupedByYearByTitle = ratings.groupBy(rating => {
      val temp_year = new DateTime((rating._5.toLong) * 1000)
      val year = temp_year.year().get()
      year
    }).map(term=> (term._1, term._2.groupBy(elem => elem._2))).persist()
  }

  def getNumberOfMoviesRatedEachYear: RDD[(Int, Int)] = {

    // We compute the number of movies rated each year.
    // The number of movies corresponds to the length of the iterable (here size)
    val result = ratingsGroupedByYearByTitle.map(term => (term._1, term._2.size))
    result
  }

  def getMostRatedMovieEachYear: RDD[(Int, String)] = {

    // We return the movies which received the largest number of ratings each year

    // We create a first new RDD containing (movie_name,movie_ID) for each movie. This is useful to perform
    // a future join on movieId
    val first_rdd = titlesGroupedById.map(movie => (movie._1,movie._2.toList.head._2))
    // We create a second new RDD containing, for each year, the ID of the movie which received the largest number of ratings
    val second_rdd = ratingsGroupedByYearByTitle.map(term=> (term._2.map(elem=> (elem._1,elem._2.size)).reduce((a,b) =>{
      if (a._2 > b._2) a
      else if (b._2 > a._2) b
      else if (a._1 > b._1) a
      else b
    }) , term._1)).map(movie=> (movie._1._1,movie._2))
    // We join and return the result
    val joined_rdd = first_rdd.join(second_rdd).map(term=> (term._2._2,term._2._1))
    joined_rdd
  }

  def getMostRatedGenreEachYear: RDD[(Int, List[String])] = {

    // In order to retrieve the most rated genre, we need to consider the movies that received
    // the largest number of ratings each year

    // We create a first new RDD containing (movie_name,movie_ID) for each movie
    val first_rdd = titlesGroupedById.map(movie => (movie._1, movie._2.toList.head._3))
    // We create a second new RDD containing, for each year, the ID of the movie which received the largest number of ratings
    // Observe: the function defined in the reduce operation is necessary to solve ties
    val second_rdd = ratingsGroupedByYearByTitle.map(term => (term._2.map(elem => (elem._1, elem._2.size)).reduce((a, b) => {
      if (a._2 > b._2) a
      else if (b._2 > a._2) b
      else if (a._1 > b._1) a
      else b
    }), term._1)).map(movie => (movie._1._1, movie._2))
    // Given the result, we consider the list containing the genres
    val joined_rdd = first_rdd.join(second_rdd).map(term => (term._2._2, term._2._1))
    joined_rdd
  }

  // Note: if two genre has the same number of rating, return the first one based on lexicographical sorting on genre.
  def getMostAndLeastRatedGenreAllTime: ((String, Int), (String, Int)) = {
    // We retrieve the movies which received the largest number of ratings each year
    val result = this.getMostRatedGenreEachYear
    // The computation of the most and least rated genre is done based on the results we have just retrieved
    // For every year, we compute the number of ratings every genre received (each genre is identified by its name)
    val intermediate = result.flatMap(year => year._2.map(genre => (genre,1))).reduceByKey(_ + _)
    // Finding the genre which received the largest number of ratings
    val most = intermediate.reduce((x,y) => {
      if (x._2 > y._2) x
      else if (y._2 > x._2) y
      else if (x._1 < y._1) x
      else y
    })
    // Finding the genre which received the smallest number of ratings
    val least = intermediate.reduce((x,y) => {
      if (x._2 > y._2) y
      else if (y._2 > x._2) x
      else if (x._1 < y._1) x
      else y
    })
    (least,most)
  }

  /**
   * Filter the movies RDD having the required genres
   *
   * @param movies         RDD of movies dataset
   * @param requiredGenres RDD of genres to filter movies
   * @return The RDD for the movies which are in the supplied genres
   */
  def getAllMoviesByGenre(movies: RDD[(Int, String, List[String])],
                          requiredGenres: RDD[String]): RDD[String] = {

    // in order to deal with the requiredGenres, we collect them to a list
    val requirements = requiredGenres.collect.toList
    // Given the movies, we filter them in order to keep only the titles whose list of genres contains
    // the required genres
    val result = movies.filter(movie => requirements.forall(x => movie._3.contains(x))).map(_._2)
    result
  }

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
                                         broadcastCallback: List[String] => Broadcast[List[String]]): RDD[String] = {
    // In order to avoid latency and useless additional latency, we broadcast the list of requirements.
    // This is beneficial to avoid useless replication across worker nodes
    val requirements = broadcastCallback(requiredGenres)
    // Given the movies, we filter them in order to keep only the titles whose list of genres contains
    // the required genres
    val result = movies.filter(movie => requirements.value.forall(x => movie._3.contains(x))).map(_._2)
    result
  }

}


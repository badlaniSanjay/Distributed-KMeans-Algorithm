
import org.apache.spark.sql.SparkSession
import breeze.linalg.{DenseVector, Vector, squaredDistance}
import org.apache.spark.rdd.RDD


/**
 * A K Means clustering algorithm that can be distributed across machines. The number of iterations can be decided to
 * get different K values and SSE (Sum of squared Errors) on convergence. The SSE it written out to a new file, which
 * can be plotted on a graph to extract the ideal K.
 */
object KMeans {
  def parseVector(line: String): Vector[Double] = {
    System.out.println("The Line Received " + line)
    DenseVector(line.split(',').map(_.toDouble))
  }

  /**
   * This method calculates the closest distance between the point and all the centroids and returns closest centroid
   * to this point
   *
   * @param p       it is the point whose closest neighbor is found
   * @param centers all all the centroids
   * @return index of the closest centroid
   */
  def closestPoint(p: Vector[Double], centers: Array[Vector[Double]]): Int = {
    var bestIndex = 0
    var closest = Double.PositiveInfinity

    for (i <- 0 until centers.length) {
      val tempDist = squaredDistance(p, centers(i))
      if (tempDist < closest) {
        closest = tempDist
        bestIndex = i
      }
    }

    bestIndex
  }


  /**
   * This is the main driver program
   *
   * @param args are command line arguments, that take in
   */
  def main(args: Array[String]) {

    if (args.length < 3) {
      System.err.println("Usage: SparkKMeans <file> <k> <convergeDist>")
      System.exit(1)
    }

    val spark = SparkSession
      .builder
      .appName("KMeans")
      .master("local[4]")
      .getOrCreate()

    val lines = spark.read.textFile(args(0)).rdd.filter(data => data.split(",").length == 2)
    val data = lines.map(parseVector _).cache()
    var K = 1
    val convergeDist = args(2).toDouble
    val sseValues = new Array[Double](11)

    while (K <= 10) {
      val kPoints = data.takeSample(withReplacement = false, K, 42)
      val tempDist = 1.0

      val value = getSSEValues(tempDist, convergeDist, K, data, kPoints)
      sseValues(K) = value

      K = K + 1
    }


    println("______SEE Values_______")
    for (i <- 1 until 10) {
      println("K:" + i + " SSE: " + sseValues(i))

    }
    spark.stop()
  }


  /**
   * This method is used to calculate the SSE values associated with a given K. The algorithm runs till it converges
   *
   * @param tempDist     It is the squared distance value between the New points and old points
   * @param convergeDist The threshold for convergence
   * @param K            The number of K the SSE values are being calculated for
   * @param data         is a Dense vector that is cached
   * @param kPoints      are the centroids of cluster
   */
  def getSSEValues(tempDist: Double, convergeDist: Double, K: Int, data: RDD[Vector[Double]], kPoints:
  Array[Vector[Double]]): Double = {
    var tempDistMutable = tempDist
    var SSE = 0.0

    var closest = data.map(p => (closestPoint(p, kPoints), (p, 1)))
    while (tempDistMutable > convergeDist) {
      //Compute the closest center to each point
      closest = data.map(p => (closestPoint(p, kPoints), (p, 1)))
      val pointStats = closest.reduceByKey { case ((p1, c1), (p2, c2)) => (p1 + p2, c1 + c2) }
      val pointsInACluster = closest.groupByKey().mapValues(_.map(_._1))
      val newPoints = pointStats.map { pair =>
        (pair._1, pair._2._1 * (1.0 / pair._2._2))
      }.collectAsMap()


      tempDistMutable = 0.0
      for (i <- 0 until K) {
        tempDistMutable += squaredDistance(kPoints(i), newPoints(i))
      }

      for (newP <- newPoints) {
        kPoints(newP._1) = newP._2
      }

      SSE = 0.0
      for (i <- 0 until K) {
        val list = pointsInACluster.lookup(i)
        for (l <- list) {
          for (litem <- l) {
            SSE += squaredDistance(litem, newPoints(i))
          }
        }
      }
    }

    SSE
  }
}


import org.apache.spark.sql.SparkSession
import breeze.linalg.{squaredDistance, DenseVector, Vector}


object KMeans {
  def parseVector(line: String): Vector[Double] = {
    System.out.println("The Line Received " + line)
    DenseVector(line.split(',')

      //        .map(x => x.length)
      //        .flatMap(x => x)
      .filter(data => data.toString.matches("[-+]?([0-9]*\\.[0-9]+|[0-9]+)"))
      .map(data => {
        if (data.length() == 0 || data.toString.equals("")) {
          "0.0"
        } else data
      })
      .map(_.toDouble))
    //    line.split(',').filter(data => !data.toString.equals(""))
    //      .filter(x => numericReg.pattern.matcher(x).matches).map(_.toDouble)
  }


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

    while (K <= 10) {
      val kPoints = data.takeSample(withReplacement = false, K, 42)
      var tempDist = 1.0
      var closest = data.map(p => (closestPoint(p, kPoints), (p, 1)))

      while (tempDist > convergeDist) {
        //Compute the closest center to each point
        closest = data.map(p => (closestPoint(p, kPoints), (p, 1)))

        val pointStats = closest.reduceByKey { case ((p1, c1), (p2, c2)) => (p1 + p2, c1 + c2) }

        val newPoints = pointStats.map { pair =>
          (pair._1, pair._2._1 * (1.0 / pair._2._2))
        }.collectAsMap()


        tempDist = 0.0
        for (i <- 0 until K) {
          tempDist += squaredDistance(kPoints(i), newPoints(i))
        }

        for (newP <- newPoints) {
          kPoints(newP._1) = newP._2
        }

        println(s"Finished iteration " + K + " (delta = $tempDist)")
      }

      println("**************************Iteration " + K + " **********************")
      println("Final centers:")
      kPoints.foreach(println)
      println("Points and centers")
      closest.foreach(println)
      K = K + 1
    }

    spark.stop()
  }
}

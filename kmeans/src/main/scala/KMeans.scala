import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import breeze.linalg.{squaredDistance, DenseVector, Vector}

object KMeans {
  def parseVector(line: String): Vector[Double] = {
    System.out.println("The Line Received "+line)
    DenseVector(line.split(',')

//        .map(x => x.length)
//        .flatMap(x => x)
      .filter(data => data.toString.matches("[-+]?([0-9]*\\.[0-9]+|[0-9]+)"))
      .map(data => {if(data.length() == 0 || data.toString.equals("")){
        "0.0"
      }else data })
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
    val lines = spark.read.textFile(args(0)).rdd.filter(data => data.split(",").length == 10)
    val data = lines.map(parseVector _).cache()


//    val K = args(1).toInt
//    val convergeDist = args(2).toDouble
//
//    val kPoints = data.takeSample(withReplacement = false, K, 42)
//    var tempDist = 1.0
//
//    while (tempDist > convergeDist) {
//      val closest = data.map(p => (closestPoint(p, kPoints), (p, 1)))
//
//      val pointStats = closest.reduceByKey { case ((p1, c1), (p2, c2)) => (p1 + p2, c1 + c2) }
//
//      val newPoints = pointStats.map { pair =>
//        (pair._1, pair._2._1 * (1.0 / pair._2._2))
//      }.collectAsMap()
//
//      tempDist = 0.0
//      for (i <- 0 until K) {
//        tempDist += squaredDistance(kPoints(i), newPoints(i))
//      }
//
//      for (newP <- newPoints) {
//        kPoints(newP._1) = newP._2
//      }
//      println(s"Finished iteration (delta = $tempDist)")
//    }
//
//    println("Final centers:")
//    kPoints.foreach(println)
//    data.saveAsTextFile("output")
    val size = data.map(x => x.length)
    size.saveAsTextFile("output")
    spark.stop()
  }
}

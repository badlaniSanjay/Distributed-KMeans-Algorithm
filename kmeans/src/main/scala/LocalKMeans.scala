import KMeans.parseVector
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object LocalKMeans {

  def nline(n: Int, path: String) = {
    val sc = SparkContext.getOrCreate
    val conf = new Configuration(sc.hadoopConfiguration)
    conf.setInt("mapreduce.input.lineinputformat.linespermap", n);

    sc.newAPIHadoopFile(path,
      classOf[NLineInputFormat], classOf[LongWritable], classOf[Text], conf
    )
  }


  def main(args: Array[String]) {

    if (args.length < 3) {
      System.err.println("Usage: SparkKMeans <file> <k> <convergeDist>")
      System.exit(1)
    }

//    System.out.println("--------------------"+nline(1, "local_input/k_values").glom.map(_.size).first)
//    System.out.println("--------------------"+nline(2, "local_input/k_values").glom.map(_.size).first)
//    System.out.println("--------------------"+nline(3, "local_input/k_values").glom.map(_.size).first)

    val sc = SparkContext.getOrCreate
    val conf = new Configuration(sc.hadoopConfiguration)
    conf.setInt("mapreduce.input.lineinputformat.linespermap", 2);

//    val spark = SparkSession
//      .builder
//      .appName("KMeans")
//      .master("local[4]")
//      .getOrCreate()
//    val textFile = spark.read.textFile(args(0)).rdd.filter(data => data.split(",").length == 2)


    val lines = sc.newAPIHadoopFile("local_input/k_values",
      classOf[NLineInputFormat], classOf[LongWritable], classOf[Text], conf
    )
    var newLines = lines.map{case (t1: LongWritable, t2: Text) => ""+t2}

    val textFile = sc.textFile("input")
    var crimeLocation = textFile.flatMap(line => line.split(" ")).filter(data => data.split(",").length == 2)
          .map(word =>{ (word.split(",")(0), word.split(",")(1))})


    var newCrimeLocation = crimeLocation.collectAsMap()

    newLines.sparkContext.broadcast(newCrimeLocation)

    var updated = newLines.mapPartitions{case (a)=>{
      val myList = a.toList

      myList.map(x => {
       newCrimeLocation.map{case(k:String, v: String)=>{
          k+"->"+v+"->"+x
        }}
      }).iterator
    }
    }

    var newUpdated = updated.flatMap( x => x)

//    newLines.foreach(doSomething())

    newUpdated.saveAsTextFile("output")
//
//
//    lines.flatMap().ma



  }
}

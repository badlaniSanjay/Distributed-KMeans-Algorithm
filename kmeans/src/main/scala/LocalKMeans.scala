import KMeans.parseVector
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat
import org.apache.spark.SparkContext
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

    System.out.println("--------------------"+nline(1, "local_input/k_values").glom.map(_.size).first)
    System.out.println("--------------------"+nline(2, "local_input/k_values").glom.map(_.size).first)
    System.out.println("--------------------"+nline(3, "local_input/k_values").glom.map(_.size).first)


  }
}

package Join

import java.io.BufferedReader
import java.io.BufferedWriter
import java.io.FileReader
import java.io.FileWriter
import org.apache.spark.rdd.RDD
import java.io.File

object FileManager {
  def mergeFiles(inputDir: String, outputPath: String, numPartitions: Int) {
    val bw = new BufferedWriter(new FileWriter(outputPath))
    try {
      for (i <- 0 to numPartitions - 1) {
        val br = new BufferedReader(new FileReader(inputDir + "/part-0000" + Integer.toString(i)))
        var line: String = null
        while ({ line = br.readLine; line != null }) {
          bw.write(line + "\n");
        }
        br.close()
      }
    } finally {
      bw.close()
    }
  }

  def writeRDDToFile(rdd: RDD[Array[String]], intermediateDir: String, outputDir: String, outputPath: String) = {
    rdd.map(record => record.mkString(",")).saveAsTextFile(intermediateDir)
    mergeFiles(intermediateDir, outputPath, rdd.partitions.size)
  }

  def delete(file: String) {
    val f = new File(file);
    if (f.isDirectory()) {
      for (c <- f.listFiles()) {
        delete(c.getPath());
      }
    }
    f.delete();
  }
}
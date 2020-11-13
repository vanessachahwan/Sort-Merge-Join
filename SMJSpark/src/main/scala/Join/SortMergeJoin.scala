package Join

import org.apache.spark.SparkContext._
import org.apache.spark._
import org.apache.spark.RangePartitioner
import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ArrayBuffer
import scala.Array


class SortMergeJoin(rdd1: RDD[(Int, Array[String])], rdd2: RDD[(Int, Array[String])]) {

  var rddL: RDD[(Int, Array[String])] = rdd1
  var rddR: RDD[(Int, Array[String])] = rdd2

  private def rangePartitionAndSort(numPartitions: Int) = {
    val partitioner = new RangePartitioner(numPartitions, rdd1)
    this.rddL = rddL.repartitionAndSortWithinPartitions(partitioner).persist()
    this.rddR = rddR.repartitionAndSortWithinPartitions(partitioner).persist()
  }

  private def hashPartitionAndSort(numPartitions: Int) = {
    val partitioner = new HashPartitioner(numPartitions)
    this.rddL = rdd1.repartitionAndSortWithinPartitions(partitioner).persist()
    this.rddR = rdd2.repartitionAndSortWithinPartitions(partitioner).persist()
  }

  def merge(): RDD[Array[String]] = {
    val result = rddL.zipPartitions(rddR)(
      (iterL, iterR) => {
        val indexKeyR = iterR.zipWithIndex.map { case (v, k) => (k, v) }.toMap
        val sizeR = indexKeyR.size
        var joined: ArrayBuffer[Array[String]] = ArrayBuffer()
        var mark, rightIndex = 0
        var endM, endR = (mark >= sizeR)
        for ((k, v) <- iterL) {
          while (!endM && k > indexKeyR.apply(mark)._1) {
            mark += 1
            endM = (mark >= sizeR)
          }
          rightIndex = mark
          if (!endM && k == indexKeyR.apply(mark)._1) {
            endR = endM
            while (!endR && k == indexKeyR.apply(rightIndex)._1) {
              joined += (v ++ indexKeyR.apply(rightIndex)._2)
              rightIndex += 1
              endR = (rightIndex >= sizeR)
            }
          }
        }
        joined.toIterator
      })
    return result
  }

  def join(partitionStrategy: String, numPartitions: Int): RDD[Array[String]] = {
    if (partitionStrategy == "Hash") hashPartitionAndSort(numPartitions) else rangePartitionAndSort(numPartitions)
    val joined = merge()
    return joined
  }

}
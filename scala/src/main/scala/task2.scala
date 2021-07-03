import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import java.io.PrintWriter
import java.io.File
import org.apache.spark.rdd.RDD

import scala.math.Ordering.Implicits._

object task2 {
    def getBidAggrByUid(inputDataRdd: RDD[(String, Long)], filterThreshold: Int) = {
        inputDataRdd.map((record) => {
            val line = record._1
            val filteredData = line.split(",")
            (filteredData(0), filteredData(1))
        }).groupByKey()
            .map((aggrTuple) => {
                (aggrTuple._1, aggrTuple._2.toSet.toList.sorted)
            })
            .filter(item_users => item_users._2.size > filterThreshold)
            .map((item_users) => item_users._2)
    }
    def getUidAggrByBid(inputDataRdd: RDD[(String, Long)]) = {
        inputDataRdd.map((record) => {
            val line = record._1
            val filteredData = line.split(",")
            (filteredData(1), filteredData(0))
        }).groupByKey()
            .map((aggrTuple) => {
                (aggrTuple._1, aggrTuple._2.toSet.toList.sorted)
            })
            .map((item_users) => item_users._2)
    }

    def selectValidCandidates(candidatesDict: scala.collection.mutable.Map[List[String], Int], supportThreshold: Float) = {
        val validCandidates = candidatesDict.filter((item) => {
            item._2 >= supportThreshold
        }).map((item) => item._1)
        validCandidates.toList.sorted
    }

    def generateNextIteration(records: Seq[List[String]], validCandidates: Set[String], size: Int, candidatesDict: scala.collection.mutable.Map[List[String], Int], support: Float) = {
        var res = scala.collection.mutable.Map[List[String], Int]()
        records.foreach((record) => {
            val frequentGranularRecords = record.toSet.intersect(validCandidates)
            val allCombinations = frequentGranularRecords.subsets(size).map(_.toList).toList
            for (combination <- allCombinations) {
                val sortedCombo = combination.sorted
                val allSmallerCombinations = sortedCombo.toSet.subsets(size - 1).map(_.toList).toList
                val allImmediateSubsetsValid = allSmallerCombinations.forall((elem) => {
                    val sortedElem = elem.sorted
                    candidatesDict.getOrElse(sortedElem, 0) >= support
                })
                if (allImmediateSubsetsValid) {
                    if (res.contains(sortedCombo)) {
                        res(sortedCombo) += 1
                    }
                    else {
                        res.put(sortedCombo, 1)
                    }
                }
            }
        })
        res
    }

    def pass2Son(partition: Iterator[List[String]], candidates: Array[List[String]]) = {
        val records = partition.toList
        var res = scala.collection.mutable.Map[List[String], Int]()
        records.map((record) => {
            candidates.map((candidate) => {
                if (candidate.toSet.subsetOf(record.toSet)) {
                    if (res.contains(candidate)) {
                        res(candidate) += 1
                    }
                    else {
                        res.put(candidate, 1)
                    }
                }
            })
        })
        res.toList
    }
    def generateTuplesOfSizeNFromN_1(validCandidates: List[List[String]], size: Int) = {
        var nSizeTuples = scala.collection.mutable.ListBuffer[List[String]]()
        for (vc <- validCandidates) {
            for (vc2 <- validCandidates) {
                if (vc != vc2) {
                    val res = vc.toSet.union(vc2.toSet)
                    if (res.size == size) {
                        nSizeTuples.append(res.toList.sorted)
                    }
                }
            }
        }
        nSizeTuples.toList
    }

    def formatter(output_data: Array[List[String]]) = {
        val res = StringBuilder.newBuilder
        var prev = 1
        output_data.foreach((outputList) => {
            val outputLength = outputList.size
            if (outputLength != prev) {
                res.append("\n\n")
            }
            res.append("('")
            res.append(outputList.mkString("', '"))
            res.append("'),")
            prev = outputLength
        })
        val finalString:String = res.toString()
        finalString.replaceAll(",\n\n", "\n\n").dropRight(1)
    }

    def generateCandidates(partition: Iterator[List[String]], support: Int, totalRecords: Long) = {

        val records = partition.toList
        val numRecords = records.size / totalRecords.toFloat
        val supportThreshold = (numRecords * support).ceil
        val basketToId = partition.zipWithIndex.toList.map((elem) => {
            (elem._1(0) -> elem._2)
        })
        val basketGroupedToIds = basketToId.groupBy(_._1).mapValues(value => value.map(_._2))
        val elemRecordTuple = records.flatten.map((elem) => {(elem, 1)})
        var candidatesDict = collection.mutable.Map() ++ elemRecordTuple.groupBy(List() :+ _._1).mapValues(_.size)
        var validCandidates = selectValidCandidates(candidatesDict, supportThreshold)
        var size = 2
        var setOfCandidates = validCandidates.flatten.toList.toSet
        var res = scala.collection.mutable.ListBuffer.empty[List[List[String]]]
        res += validCandidates
        do {
            candidatesDict = generateNextIteration(records, setOfCandidates, size, candidatesDict, supportThreshold)
            validCandidates = selectValidCandidates(candidatesDict, supportThreshold)
            if (validCandidates.size > 0) {
                res += validCandidates
                setOfCandidates = validCandidates.flatten.toList.toSet
            }
            size += 1
            val nextSizeTuples = generateTuplesOfSizeNFromN_1(validCandidates, size)
            nextSizeTuples.map((elem) => {
                elem.foreach((small_elem) => {
                    if (basketGroupedToIds.getOrElse(small_elem, false) == true) {

                    }
                })
            })
        } while(validCandidates.size > 0)
        res.toList
    }


    def processInput(input_file_path: String, sc: SparkContext, support: Int, filterThreshold: Int, outputPath: String) = {
        val startTime = System.nanoTime()
        val input_rdd = sc.textFile(input_file_path)
        val extracted_data_rdd = input_rdd.zipWithIndex().filter((tupleData) => {
            tupleData._2 > 0
        })
        val allBaskets = getBidAggrByUid(extracted_data_rdd, filterThreshold)
        val totalRecords = allBaskets.count()
        val candidates = allBaskets
            .mapPartitions((iterator) => {
                generateCandidates(iterator, support, totalRecords).toIterator
            })
            .flatMap((elem) => elem)
            .distinct()
            .sortBy((elem) => (elem.size, elem))
            .collect()
//
        val freqItems = allBaskets
            .mapPartitions((iterator) => {
                pass2Son(iterator, candidates)
            }.toIterator)
            .reduceByKey((val1, val2) => val1 + val2)
            .filter((tup) => tup._2 >= support)
            .map(tup => tup._1)
            .sortBy((elem) => (elem.size, elem))
            .collect()

        val writer = new PrintWriter(new File(outputPath))
        writer.write("Candidates:\n" + formatter(candidates) + "\n\n" + "Frequent Itemsets:\n" + formatter(freqItems))
        writer.close()
        val endTime = System.nanoTime()
        print("Duration: " + (endTime - startTime) / 1e9d)

    }
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("Task1").setMaster("local[*]")
        val sc: SparkContext = new SparkContext(conf)
        sc.setLogLevel("ERROR")
        val filterThreshold: Int = args(0).toInt
        val support: Int = args(1).toInt
        val inputPath: String = args(2)
        val outputPath: String = args(3)
        processInput(inputPath, sc, support, filterThreshold, outputPath)
    }
}


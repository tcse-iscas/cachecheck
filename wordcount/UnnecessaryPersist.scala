// scalastyle:off println
package org.apache.spark.examples.wordcount

import org.apache.spark.{SparkConf, SparkContext}

object UnnecessaryPersist{
    def main(args: Array[String]) {
        val sparkConf = new SparkConf().setAppName("Unnecessary Persist")
        val sc = new SparkContext(sparkConf)
        val data = sc.textFile("article.txt")
        val words = data.flatMap(x=>x.split(" "))
        words.persist()
        words.count()
        val pairs = words.map((_,1))
        pairs.persist()
        val result = pairs.reduceByKey(_+_)
        result.persist()
        result.count()
        words.unpersist()
        result.take(10)
        pairs.unpersist()
        result.unpersist()
        sc.stop()
    }
}
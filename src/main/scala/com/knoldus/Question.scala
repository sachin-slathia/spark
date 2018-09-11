package com.knoldus

import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}

object Question extends App {
  lazy val logger = Logger.getLogger(getClass.getName)
  val name = "Question"
  val conf = new SparkConf().setAppName(name).
                  setMaster("local[2]").set("spark.executor.memory", "1g")
  val sc = SparkContext.getOrCreate(conf)

  //question1
  val rddDataSetOne = sc.parallelize(Seq((1, 1.6), (1, 2.3), (2, 1.5), (2, .5)))
  val resultOne = rddDataSetOne.reduceByKey((value1, value2) =>
                                value1.doubleValue - value2.doubleValue)
  resultOne.collect.foreach(output => logger.info(output))

  //question 2
  val rddDataSetTwo = sc.parallelize(Array((1,Array((3,4), (4,5))), (2,Array((4,2), (4,4), (3,9)))))

  val resultTwo = rddDataSetTwo.flatMapValues(result => result)
  resultTwo.collect.foreach(output => logger.info(output))

  //question3
  val rddDataSetThree = sc.parallelize(Array((1, List(1, 2, 3, 4)), (2, List(1, 2, 3, 4)), (3, List(1, 2, 3, 4)),
                                       (4, List(1, 2, 3, 4))))

  val resultThree = rddDataSetThree.flatMapValues(value => value).filter(compareValue =>
                                    compareValue._1 == compareValue._2)

  resultThree.collect.foreach(output => logger.info(output._1 + "--" + output._2))


}

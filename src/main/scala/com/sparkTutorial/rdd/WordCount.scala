package com.sparkTutorial.rdd

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark._

import java.util.Timer
import org.apache.spark.util
import java.lang.System;


object WordCount {

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("wordCounts").setMaster("local[3]")
    val sc = new SparkContext(conf)
    val timeout = args(0).toInt // mania

    // mania
    val t = new java.util.Timer()
    val task = new java.util.TimerTask {
        def run() {
            println("YYYOOOOOOOOOOOOOO")
            //sc.session.stop();
             this.cancel()
            sc.cancelAllJobs()
            sc.stop()
            System.exit(0)
        }
    } 
    t.schedule(task, timeout*1000, timeout*1000)

    while(true) {
      val lines = sc.textFile("in/word_count.text")
      val words = lines.flatMap(line => line.split(" "))

      val wordCounts = words.countByValue()
      for ((word, count) <- wordCounts) println(word + " : " + count)
    }
  }
}

package com.blazedb.spark

import com.blazedb.spark.StreamWordCount.StringSource
import org.apache.spark._
import org.apache.spark.storage._
import org.apache.spark.streaming._

import scala.util.Random
import org.apache.spark.streaming.receiver._

object StreamWordCount {
  
  class StringSource(perSec: Int) extends Receiver[String](StorageLevel.MEMORY_AND_DISK) {


    override def onStart() {
      new Thread("StringSource") {
        override def run() {
          while (!isStopped) {
            store(s"StringSource: ${Random.nextInt(10)}")
            Thread.sleep((1000.0/perSec).toInt)
          }
        }
      }

    }
    override def onStop(): Unit = {}

  }
  
  def main(args: Array[String]) = {
    val sconf = new SparkConf().setMaster("local[2]").setAppName("StreamWC")
    val sc = new SparkContext(sconf)
    val swc = new StreamWordCount(sc, 3, 10)
    swc.run
    Thread.currentThread.join
  }
}

class StreamWordCount(@transient sc: SparkContext, batchIntervalSecs: Int, perSec: Int) extends java.io.Serializable {

  @transient val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  import sqlContext.implicits._


    def updateFn(ts: Time, key: String, vval:Option[Int], state: State[Long]): Option[(String,Long)] =  {
      val sum = vval.getOrElse(0L).asInstanceOf[Long] + state.getOption.getOrElse(0L)
      state.update(sum)
      Some((key, sum))
    }
    val initRdd = sc.parallelize((1 to 100).map{ n => (s"Key$n",n.toLong) })

    val stateSpec = StateSpec.function(updateFn _)
              .initialState(initRdd)
//              .numPartitions(Runtime.getRuntime.availableProcessors)
              .numPartitions(2)
              .timeout(Seconds(300))

  def createStream(/*sc: SparkContext, batchIntervalSecs: Int,perSec:Int */): StreamingContext = {

    val ssc = new StreamingContext(sc, Seconds(batchIntervalSecs))

    val stream = ssc.receiverStream(new StringSource(perSec)) 

    val wordStream = stream.flatMap { _.split(" ")}.map(w => (w,1))

    val wcStateStream = wordStream.mapWithState(stateSpec)
    wcStateStream.print

    val stateSnapStream = wcStateStream.stateSnapshots
    stateSnapStream.foreachRDD { rdd =>
      rdd.toDF("word","count").registerTempTable("word_count")
    }
    ssc.remember(Minutes(5))
    ssc.checkpoint("file:///tmp/wcStreams")
    println("completed creating ssc")
    ssc
  }


  def run() = {
    StreamingContext.getActive.foreach{ _.stop(stopSparkContext =false)}

//    val createStreamInst = createStream(sc, batchIntervalSecs, perSec) _
    val ssc = StreamingContext.getActiveOrCreate(createStream)
    ssc.awaitTerminationOrTimeout(batchIntervalSecs  * 1000)
    val df = sqlContext.sql("select * from word_count")
    println(s"count of df = ${df.count}")

  }

}


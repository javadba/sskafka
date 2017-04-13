package com.blazedb.spark

import org.apache.spark._
import org.apache.spark.storage._
import org.apache.spark.streaming._

import scala.util.Random
import org.apache.spark.streaming.receiver._


case class Evt(id: Long, v: Int, startEndOrReconciled: String, e: Option[Exception])

//object GaussSource {
//  val evtCounter = new java.util.concurrent.AtomicLong
//  def nextId() = evtCounter.incrementAndGet
//}
//
//class GaussSource(perSec: Int, vRange: (Int, Int), eProbability: Float) extends Receiver[Evt](StorageLevel.MEMORY_AND_DISK) {
//  import GaussSource._
//	def onStart() = {
//		new Thread("GaussianSource") {
//      val rand = new java.util.Random
//      val erolldice = rand.next
//      val e = if (erolldice > 1 - eProbability) Some(new RuntimeException(s"Randomly generated value $e compels us to throw up our arms")) else None
//      override def run() {
//        while (!isStopped) {
//          store(Evt(nextId, vRange._1 + (vRange._2-vRange._1)* rand.nextGaussian,e))
//        }
//        Thread.sleep((1000.0/perSec).toInt)
//      }
//    }.start
//  }
//
//  def onStop()=  {}
//}
//
//class SparkCep {
//
//  val intervalSecs = 1
//  val evtsPerSec = 1000
//
//  @inline def e(msg: String) =
//  def updateStateFn(ts: Time, key: Long, vvalo: Option[Evt], stateo: State[Evt]): Option[Long, Evt]) = {
//      if (stateo.isEmpty) {
//        vval
//      } else if (vvalo.isEmpty) {
//        p(s"WARN: vval is empty for state=$state")
//        state
//      } else {
//          val vval = vvalo.get
//          if (stateo.nonEmpty) {
//            val state  = stateo.get
//            val (newDisp,e)  = vval.startEndOrReconciled match {
//              case "start" =>
//                state.startEndOrReconciled match {
//                  case "start" => ("start", e("Duplicate start event"))
//                  case "end" => ("reconciled","
//                  case "reconciled" => ("reconciled", "Start after reconciled")
//                }
//              case "end" =>
//                vval match {
//                  case "end" => ("reconciled",None)
//                  case "start" => ("start", "Duplicate start event")
//                  case "reconciled" => ("reconciled", "Start after reconciled")
//                }
//    }
//
//}
//
//

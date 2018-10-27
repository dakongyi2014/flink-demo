/*
 * Copyright 2015 data Artisans GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cn.lk.opensource.flink.base

import cn.lk.opensource.flink.sources.TaxiRideSource
import cn.lk.opensource.flink.utils.ExerciseBase._
import cn.lk.opensource.flink.utils.{ExerciseBase, GeoUtils}
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.common.typeutils.base.ShortValueSerializer
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.Trigger.TriggerContext
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.types.ShortValue
import org.apache.flink.util.Collector


/**
 * Apache Flink DataStream API demo application.
 *
 * The program processes a stream of taxi ride events from the New York City Taxi and Limousine
 * Commission (TLC).
 * It computes every five minutes for each location the total number of persons that arrived
 * within the last 15 minutes by taxi. The program emits early partial count results whenever more
 * than 50 persons (or a multitude of 50 persons) arrive at a location within 15 minutes.
 *
 * update by lk20043635@hotmail.com
 *
 */
object EarlyArrivalCount extends ExerciseBase {

  def main(args: Array[String]) {

    // parse parameters
    val params = ParameterTool.fromArgs(args)
    val input = params.get("input", ExerciseBase.pathToRideData)

    val maxDelay = 60
    val speed = 600

    // window parameters
    val countWindowLength = 20 // window size in min
    val countWindowFrequency = 2 // window trigger interval in min
    val earlyCountThreshold = 20

    // Elasticsearch parameters
    val writeToElasticsearch = false // set to true to write results to Elasticsearch
    val elasticsearchHost = "" // look-up hostname in Elasticsearch log output
    val elasticsearchPort = 9300


    // set up the execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(parallelism)

    // get the taxi ride data stream
    val rides = env.addSource(rideSourceOrTest(new TaxiRideSource(input, maxDelay, speed)))
    // Define the data source
    val cleansedRides = rides.filter(!_.isStart).filter(r => GeoUtils.isInNYC(r.endLon, r.endLat))

    val cellIds: DataStream[(Int, Short)] = cleansedRides
      .map(r => (GeoUtils.mapToGridCell(r.endLon, r.endLat), r.passengerCnt))

    val trigger = new EarlyCountTrigger(earlyCountThreshold)


    val passengerCnts: DataStream[(Int, Long, Int)] = cellIds
      // key stream by cell Id
      .keyBy(_._1)
      // define sliding window on keyed streams
      .window(SlidingEventTimeWindows.of(Time.seconds(countWindowLength), Time.seconds(countWindowFrequency)))
      .trigger(trigger)
      // count events in window
      .apply { (
                 cell: Int,
                 window: TimeWindow,
                 events: Iterable[(Int, Short)],
                 out: Collector[(Int, Long, Int)]) =>
        out.collect( ( cell, window.getEnd, events.map( _._2 ).sum ) )
      }

    val cntByLocation: DataStream[(Int, Long, (Float, Float), Int)] = passengerCnts
      // map cell Id back to GeoPoint
      .map( r => ( r._1, r._2,(GeoUtils.getGridCellCenterLon(r._1),GeoUtils.getGridCellCenterLat(r._1)), r._3 ) )

    cntByLocation
      .print()

    env.execute("Early arrival counts per location")

  }


  /**
   * ValueState OperatorState类处理
   * @param triggerCnt
   */
  class EarlyCountTrigger(triggerCnt: Int) extends Trigger[(Int, Short), TimeWindow] {

    val stateDesc = new ValueStateDescriptor("personCnt", ShortValueSerializer.INSTANCE)

    override def onElement(event: (Int, Short), timestamp: Long, window: TimeWindow, ctx: TriggerContext): TriggerResult = {
      val personCnt = ctx.getPartitionedState(stateDesc);
      // check if count is high enough for early notification
      if (personCnt.value() == null) {
        // trigger count is reached
        personCnt.update(new ShortValue(0))
        TriggerResult.CONTINUE
      } else {
        if (personCnt.value().toString.toInt < triggerCnt) {
          // update count by passenger cnt of new event
          val sum_person: Short = (personCnt.value().getValue.toInt + event._2.toInt).toShort;
          val personCntShort = new ShortValue(sum_person)
          personCnt.update(personCntShort)
          // not yet
          TriggerResult.CONTINUE
        }
        else {
          // trigger count is reached
          personCnt.update(new ShortValue(0))
          TriggerResult.FIRE
        }
      }
    }

    override def clear(window: TimeWindow, ctx: TriggerContext): Unit = {
      ctx.getPartitionedState(stateDesc).clear
    }

    override def onProcessingTime(time: Long, window: TimeWindow, ctx: TriggerContext): TriggerResult = {
      throw new UnsupportedOperationException("I am not a processing time trigger")
    }

    override def onEventTime(time: Long, window: TimeWindow, ctx: TriggerContext): TriggerResult = {
      TriggerResult.FIRE_AND_PURGE
    }
  }

}


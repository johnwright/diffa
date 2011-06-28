/**
 * Copyright (C) 2010-2011 LShift Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.lshift.diffa.kernel.scheduler

import net.lshift.diffa.kernel.config.{ConfigStore, Pair}
import java.io.Closeable
import org.quartz.impl.DirectSchedulerFactory
import org.quartz.jobs.NoOpJob
import org.quartz.listeners.TriggerListenerSupport
import java.lang.IllegalStateException
import net.lshift.diffa.kernel.util.AlertCodes
import org.slf4j.{LoggerFactory, Logger}
import net.lshift.diffa.kernel.differencing.SessionManager
import org.quartz.simpl.{RAMJobStore, SimpleThreadPool}
import org.quartz._

/**
 * Quartz backed implementation of the ScanScheduler.
 */
class QuartzScanScheduler(config:ConfigStore, sessions:SessionManager, name:String)
    extends ScanScheduler
    with Closeable {

  private val log:Logger = LoggerFactory.getLogger(getClass)

  // Create a QuartzScheduler, and attach a global listener to it to see when events are triggered. This is the easiest
  // way to get stateful invocations whereby we can call other bean components.
  private val scheduler = createScheduler()
  scheduler.start()
  scheduler.addGlobalTriggerListener(new TriggerListenerSupport {
    override def triggerFired(trigger: Trigger, context: JobExecutionContext) {
      val pairKey = trigger.getName

      log.info("%s: Starting scheduled scan for pair %s".format(AlertCodes.SCHEDULED_SCAN_STARTING, pairKey))
      try {
        sessions.runScanForPair(pairKey)
      } catch {
          // Catch, log, and drop exceptions to prevent the scheduler trying to do any misfire handling
        case ex =>
          log.error("%s: Failed to start scheduled scan for %s".format(AlertCodes.SCHEDULED_SCAN_FAILURE, pairKey), ex)
      }
    }
    def getName = "GlobalScanTriggerListener"
  })

  // Ensure that a trigger is registered for each pair on startup
  config.listGroups.foreach(_.pairs.foreach(onUpdatePair(_)))

  def onUpdatePair(pairKey: String) {
    onUpdatePair(config.getPair(pairKey))
  }

  def onUpdatePair(pair:Pair) {
    val existingJob = jobForPair(pair.key)

    def unschedulePair() {
      log.debug("Removing schedule for pair %s".format(pair.key))
      scheduler.deleteJob(pair.key, Scheduler.DEFAULT_GROUP)
    }

    def schedulePair() {
      log.debug("Applying schedule '%s' for pair %s".format(pair.scanCronSpec, pair.key))
      val trigger = new CronTrigger(pair.key, Scheduler.DEFAULT_GROUP, pair.scanCronSpec)
      val job = new JobDetail(pair.key, classOf[NoOpJob])
      scheduler.scheduleJob(job, trigger)
    }

    if (existingJob != null && pair.scanCronSpec == null) {
      // No scan spec present, so just delete the schedule
      unschedulePair()
    } else if (existingJob != null) {
      val triggers = scheduler.getTriggersOfJob(pair.key, Scheduler.DEFAULT_GROUP)
      if (triggers.length != 1)
        throw new IllegalStateException("Expected exactly 1 trigger for " + pair.key + ", but instead found " + triggers)

      val currentExpr = triggers.head.asInstanceOf[CronTrigger].getCronExpression
      if (pair.scanCronSpec != currentExpr) {   // Only re-schedule if the cronspec has changed
        unschedulePair()
        schedulePair()
      }
    } else if (pair.scanCronSpec != null) {
      schedulePair();
    }
  }

  def onDeletePair(pairKey: String) {
    val existingJob = jobForPair(pairKey)
    if (existingJob != null) {
      scheduler.deleteJob(pairKey, Scheduler.DEFAULT_GROUP)
    }
  }

  def close() {
    scheduler.shutdown()
  }

  private def jobForPair(key:String) = scheduler.getJobDetail(key, Scheduler.DEFAULT_GROUP)
  private def createScheduler() = {
    val threadPool = new SimpleThreadPool(1, Thread.NORM_PRIORITY)
    threadPool.initialize()

    // Allocate the scheduler
    DirectSchedulerFactory.getInstance().createScheduler(name, name, threadPool, new RAMJobStore())

    // Retrieve it
    DirectSchedulerFactory.getInstance().getScheduler(name)
  }
}
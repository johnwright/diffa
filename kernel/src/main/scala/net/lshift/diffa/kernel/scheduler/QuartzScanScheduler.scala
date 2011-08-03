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
import org.quartz.JobKey.jobKey
import org.quartz.TriggerBuilder.newTrigger
import org.quartz.TriggerKey.triggerKey
import org.quartz.CronScheduleBuilder.cronSchedule
import org.quartz.JobBuilder.newJob
import net.lshift.diffa.kernel.config.system.SystemConfigStore
import net.lshift.diffa.kernel.config.{DomainConfigStore, Pair => DiffaPair}

/**
 * Quartz backed implementation of the ScanScheduler.
 */
class QuartzScanScheduler(//config:DomainConfigStore,
                          systemConfig:SystemConfigStore,
                          sessions:SessionManager,
                          name:String)
    extends ScanScheduler
    with Closeable {

  private val log:Logger = LoggerFactory.getLogger(getClass)

  // Create a QuartzScheduler, and attach a global listener to it to see when events are triggered. This is the easiest
  // way to get stateful invocations whereby we can call other bean components.
  private val scheduler = createScheduler()
  scheduler.start()
  scheduler.getListenerManager.addTriggerListener(new TriggerListenerSupport {
    override def triggerFired(trigger: Trigger, context: JobExecutionContext) {
      val pairId = trigger.getJobKey.getName
      var (domain,pairKey) = DiffaPair.fromIdentifier(pairId)
      val pair = systemConfig.getPair(domain,pairKey)

      log.info("%s: Starting scheduled scan for pair %s".format(AlertCodes.SCHEDULED_SCAN_STARTING, pairKey))
      try {
        sessions.runScanForPair(pair)
      } catch {
          // Catch, log, and drop exceptions to prevent the scheduler trying to do any misfire handling
        case ex =>
          log.error("%s: Failed to start scheduled scan for %s".format(AlertCodes.SCHEDULED_SCAN_FAILURE, pairKey), ex)
      }
    }
    def getName = "GlobalScanTriggerListener"
  })

  // Ensure that a trigger is registered for each pair on startup
  systemConfig.listPairs.foreach(onUpdatePair(_))

  def onUpdatePair(pair:DiffaPair) {
    val existingJob = jobForPair(pair)
    val jobId = jobIdentifier(pair)
    val jobName = jobId.toString

    def unschedulePair() {
      log.debug("Removing schedule for pair %s".format(jobName))
      scheduler.deleteJob(jobId)
    }

    def schedulePair() {
      log.debug("Applying schedule '%s' for pair %s".format(pair.scanCronSpec, jobName))
      val trigger = newTrigger()
        .withIdentity(triggerKey(jobName))
        .withSchedule(cronSchedule(pair.scanCronSpec))
        .build()
      val job = newJob(classOf[NoOpJob])
        .withIdentity(jobId)
        .build()
      scheduler.scheduleJob(job, trigger)
    }

    if (existingJob != null && pair.scanCronSpec == null) {
      // No scan spec present, so just delete the schedule
      unschedulePair()
    } else if (existingJob != null) {
      val triggers = scheduler.getTriggersOfJob(jobId)
      if (triggers.size != 1)
        throw new IllegalStateException("Expected exactly 1 trigger for " + pair.identifier + ", but instead found " + triggers)

      val currentExpr = triggers.get(0).asInstanceOf[CronTrigger].getCronExpression
      if (pair.scanCronSpec != currentExpr) {   // Only re-schedule if the cronspec has changed
        unschedulePair()
        schedulePair()
      }
    } else if (pair.scanCronSpec != null) {
      schedulePair();
    }
  }

  def onDeletePair(pair:DiffaPair) {
    val existingJob = jobForPair(pair)
    if (existingJob != null) {
      scheduler.deleteJob(jobIdentifier(pair))
    }
    else {
      log.warn("Received pair delete event (%s) for non-existent job".format(pair))
    }
  }

  def close() {
    scheduler.shutdown()
  }

  private def jobForPair(pair:DiffaPair) = scheduler.getJobDetail(jobIdentifier(pair))
  private def jobIdentifier(pair:DiffaPair) = new JobKey(pair.identifier)
  private def createScheduler() = {
    val threadPool = new SimpleThreadPool(1, Thread.NORM_PRIORITY)
    threadPool.initialize()

    // Allocate the scheduler
    DirectSchedulerFactory.getInstance().createScheduler(name, name, threadPool, new RAMJobStore())

    // Retrieve it
    DirectSchedulerFactory.getInstance().getScheduler(name)
  }

}
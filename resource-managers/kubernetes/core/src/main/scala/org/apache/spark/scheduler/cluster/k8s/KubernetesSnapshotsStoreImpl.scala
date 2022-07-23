/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.scheduler.cluster.k8s

import java.util
import java.util.concurrent.{
  CopyOnWriteArrayList,
  Future,
  LinkedBlockingQueue,
  ScheduledExecutorService,
  TimeUnit
}
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantLock
import javax.annotation.concurrent.GuardedBy

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import org.apache.spark.internal.Logging
import org.apache.spark.util.{Clock, SystemClock, ThreadUtils}


class KubernetesSnapshotsStoreImpl(
                                    subscribersExecutor: ScheduledExecutorService,
                                    clock: Clock = new SystemClock
                                  ) extends KubernetesSnapshotsStore with Logging {

  private val SNAPSHOT_LOCK = new Object()

  private val subscribers = new CopyOnWriteArrayList[SnapshotsSubscriber]()
  private val pollingTasks = new CopyOnWriteArrayList[Future[_]]

  @GuardedBy("SNAPSHOT_LOCK")
  private var currentSnapshot = KubernetesSnapshot()

  override def addSubscriber(
                              processBatchIntervalMillis: Long)
                            (onNewSnapshots: Seq[KubernetesSnapshot] => Unit): Unit = {
    val newSubscriber = new SnapshotsSubscriber(onNewSnapshots)
    SNAPSHOT_LOCK.synchronized {
      newSubscriber.addCurrentSnapshot()
    }
    subscribers.add(newSubscriber)
    pollingTasks.add(subscribersExecutor.scheduleWithFixedDelay(
      () => newSubscriber.processSnapshots(),
      0L,
      processBatchIntervalMillis,
      TimeUnit.MILLISECONDS))
  }

  override def notifySubscribers(): Unit = SNAPSHOT_LOCK.synchronized {
    subscribers.asScala.foreach { s =>
      subscribersExecutor.submit(new Runnable() {
        override def run(): Unit = s.processSnapshots()
      })
    }
  }

  override def stop(): Unit = {
    pollingTasks.asScala.foreach(_.cancel(false))
    ThreadUtils.shutdown(subscribersExecutor)
  }

  override def updateResource(
                               updatedResource: KubernetesResource
                             ): Unit = SNAPSHOT_LOCK.synchronized {
    updateResources(Set(updatedResource))
  }

  override def updateResources(
                                newSnapshot: Set[KubernetesResource]
                              ): Unit = SNAPSHOT_LOCK.synchronized {
    currentSnapshot = currentSnapshot.withUpdate(newSnapshot)
    addCurrentSnapshotToSubscribers()
  }

  private def addCurrentSnapshotToSubscribers(): Unit = {
    subscribers.asScala.foreach(_.addCurrentSnapshot())
  }

  private class SnapshotsSubscriber(onNewSnapshots: Seq[KubernetesSnapshot] => Unit) {

    private val snapshotsBuffer = new LinkedBlockingQueue[KubernetesSnapshot]()
    private val lock = new ReentrantLock()
    private val notificationCount = new AtomicInteger()

    def addCurrentSnapshot(): Unit = {
      snapshotsBuffer.add(currentSnapshot)
    }

    def processSnapshots(): Unit = {
      notificationCount.incrementAndGet()
      processSnapshotsInternal()
    }

    private def processSnapshotsInternal(): Unit = {
      if (lock.tryLock()) {
        // Check whether there are pending notifications before calling the subscriber. This
        // is needed to avoid calling the subscriber spuriously when the race described in the
        // comment below happens.
        if (notificationCount.get() > 0) {
          try {
            val snapshots = new util.ArrayList[KubernetesSnapshot]()
            snapshotsBuffer.drainTo(snapshots)
            onNewSnapshots(snapshots.asScala.toSeq)
          } catch {
            case e: IllegalArgumentException =>
              logError("Going to stop due to IllegalArgumentException", e)
              System.exit(1)
            case NonFatal(e) => logWarning("Exception when notifying snapshot subscriber.", e)
          } finally {
            lock.unlock()
          }

          if (notificationCount.decrementAndGet() > 0) {
            // There was another concurrent request for this subscriber. Schedule a task to
            // immediately process snapshots again, so that the subscriber can pick up any
            // changes that may have happened between the time it started looking at snapshots
            // above, and the time the concurrent request arrived.
            //
            // This has to be done outside of the lock, otherwise we might miss a notification
            // arriving after the above check, but before we've released the lock. Flip side is
            // that we may schedule a useless task that will just fail to grab the lock.
            subscribersExecutor.submit(new Runnable() {
              override def run(): Unit = processSnapshotsInternal()
            })
          }
        } else {
          lock.unlock()
        }
      }
    }
  }
}

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

import java.util.concurrent.{Future, ScheduledExecutorService, TimeUnit}

import org.apache.spark.SparkConf
import org.apache.spark.deploy.k8s.Config.KUBERNETES_EXECUTOR_API_POLLING_INTERVAL
import org.apache.spark.internal.Logging
import org.apache.spark.util.{ThreadUtils, Utils}



class KubernetesPollingSnapshotSource(
                                         conf: SparkConf,
                                         pollingExecutor: ScheduledExecutorService,
                                         poller: String => PollerRunnable
                                     ) extends Logging {

  private val pollingInterval = conf.get(KUBERNETES_EXECUTOR_API_POLLING_INTERVAL)

  private var pollingFuture: Future[_] = _

  def start(applicationId: String): Unit = {
    require(pollingFuture == null, "Cannot start polling more than once.")
    logDebug(s"Starting to check for executor pod state every $pollingInterval ms.")
    pollingFuture = pollingExecutor.scheduleWithFixedDelay(
      poller(applicationId), pollingInterval, pollingInterval, TimeUnit.MILLISECONDS)
  }

  def stop(): Unit = {
    if (pollingFuture != null) {
      pollingFuture.cancel(true)
      pollingFuture = null
    }
    ThreadUtils.shutdown(pollingExecutor)
  }

}

class PollerRunnable(snapshotsStore: KubernetesSnapshotsStore) extends Runnable with Logging {
  def poll(): Set[KubernetesResource] = Set.empty

  override def run(): Unit = Utils.tryLogNonFatalError {
    logDebug(s"Resynchronizing full resource state from Kubernetes.")
    snapshotsStore.updateResources(poll())
  }
}


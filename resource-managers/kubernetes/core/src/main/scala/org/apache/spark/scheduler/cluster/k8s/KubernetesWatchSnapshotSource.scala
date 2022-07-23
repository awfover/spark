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

import io.fabric8.kubernetes.client.{KubernetesClient, Watcher, WatcherException}
import io.fabric8.kubernetes.client.Watcher.Action
import java.io.Closeable

import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils


abstract class KubernetesWatchSnapshotSource[T](
                                     snapshotsStore: KubernetesSnapshotsStore,
                                     kubernetesClient: KubernetesClient) extends Logging {

  private var watchConnection: Closeable = _

  def start(applicationId: String): Unit = {
    require(watchConnection == null, "Cannot start the watcher twice.")
    watchConnection = watch(applicationId)
  }

  def watch(applicationId: String): Closeable

  def stop(): Unit = {
    if (watchConnection != null) {
      Utils.tryLogNonFatalError {
        watchConnection.close()
      }
      watchConnection = null
    }
  }

  protected abstract class KubernetesWatcher() extends Watcher[T] with Logging {
    def constructKubernetesResource(resource: T): KubernetesResource

    override def eventReceived(action: Action, resource: T): Unit = {
      val kubernetesResource = constructKubernetesResource(resource)
      logDebug(
        s"Received k8s resource update for resource named ${kubernetesResource.name}, " +
          s"action $action"
      )
      snapshotsStore.updateResource(kubernetesResource)
    }

    override def onClose(e: WatcherException): Unit = {
      logWarning("Kubernetes client has been closed (this is expected if the application is" +
        " shutting down.)", e)
    }

    override def onClose(): Unit = {
      logWarning("Kubernetes client has been closed.")
    }
  }

}




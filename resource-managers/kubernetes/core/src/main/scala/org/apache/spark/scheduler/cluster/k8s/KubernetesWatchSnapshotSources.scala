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

import io.fabric8.kubernetes.api.model.{Node, Pod}
import io.fabric8.kubernetes.client.KubernetesClient
import java.io.Closeable

import org.apache.spark.deploy.k8s.Constants.{SPARK_APP_ID_LABEL, SPARK_POD_DATASOURCE_ROLE, SPARK_POD_EXECUTOR_ROLE, SPARK_ROLE_LABEL}

class KubernetesWatchExecutorPodSnapshotSource(
                                        snapshotsStore: KubernetesSnapshotsStore,
                                        kubernetesClient: KubernetesClient,
                                        kubernetesResolver: KubernetesResourceResolver
                                      ) extends KubernetesWatchSnapshotSource[Pod](
  snapshotsStore, kubernetesClient
) {
  override def watch(applicationId: String): Closeable = {
    kubernetesClient.pods()
      .withLabel(SPARK_APP_ID_LABEL, applicationId)
      .withLabel(SPARK_ROLE_LABEL, SPARK_POD_EXECUTOR_ROLE)
      .watch(new PodWatcher())
  }

  private class PodWatcher extends KubernetesWatcher {
    override def constructKubernetesResource(pod: Pod): KubernetesResource = {
      KubernetesPod(pod, kubernetesResolver)
    }
  }
}

class KubernetesWatchDataSourcePodSnapshotSource(
                                                snapshotsStore: KubernetesSnapshotsStore,
                                                kubernetesClient: KubernetesClient,
                                                kubernetesResolver: KubernetesResourceResolver
                                              ) extends KubernetesWatchSnapshotSource[Pod](
  snapshotsStore, kubernetesClient
) {
  override def watch(applicationId: String): Closeable = {
    kubernetesClient.pods()
      .inAnyNamespace()
      .withLabel(SPARK_ROLE_LABEL, SPARK_POD_DATASOURCE_ROLE)
      .watch(new PodWatcher())
  }

  private class PodWatcher extends KubernetesWatcher {
    override def constructKubernetesResource(pod: Pod): KubernetesResource = {
      KubernetesPod(pod, kubernetesResolver)
    }
  }
}

class KubernetesWatchNodeSnapshotSource(
                                                snapshotsStore: KubernetesSnapshotsStore,
                                                kubernetesClient: KubernetesClient
                                              ) extends KubernetesWatchSnapshotSource[Node](
  snapshotsStore, kubernetesClient
) {
  override def watch(applicationId: String): Closeable = {
    kubernetesClient.nodes()
      .watch(new NodeWatcher())
  }

  private class NodeWatcher extends KubernetesWatcher {
    override def constructKubernetesResource(node: Node): KubernetesResource = {
      KubernetesNode(node)
    }
  }
}

object KubernetesWatchSnapshotSources {
  def apply(
             snapshotsStore: KubernetesSnapshotsStore,
             kubernetesClient: KubernetesClient,
             kubernetesResolver: KubernetesResourceResolver
           ): Seq[KubernetesWatchSnapshotSource[_]] = {
    new KubernetesWatchExecutorPodSnapshotSource(
      snapshotsStore, kubernetesClient, kubernetesResolver) ::
      new KubernetesWatchDataSourcePodSnapshotSource(
        snapshotsStore, kubernetesClient, kubernetesResolver) ::
      new KubernetesWatchNodeSnapshotSource(snapshotsStore, kubernetesClient) ::
      Nil
  }
}

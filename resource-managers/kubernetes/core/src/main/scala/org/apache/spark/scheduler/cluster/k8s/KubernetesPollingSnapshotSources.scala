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

import io.fabric8.kubernetes.client.KubernetesClient
import java.util.concurrent.ScheduledExecutorService
import scala.collection.JavaConverters._

import org.apache.spark.SparkConf
import org.apache.spark.deploy.k8s.Constants.{SPARK_APP_ID_LABEL, SPARK_EXECUTOR_INACTIVE_LABEL, SPARK_POD_DATASOURCE_ROLE, SPARK_POD_EXECUTOR_ROLE, SPARK_ROLE_LABEL}



class KubernetesExecutorPodPoller(
                                 kubernetesClient: KubernetesClient,
                                 kubernetesResolver: KubernetesResourceResolver,
                                 snapshotsStore: KubernetesSnapshotsStore,
                                 applicationId: String
                                 ) extends PollerRunnable(snapshotsStore) {
  override def poll(): Set[KubernetesResource] = {
    kubernetesClient
      .pods()
      .withLabel(SPARK_APP_ID_LABEL, applicationId)
      .withLabel(SPARK_ROLE_LABEL, SPARK_POD_EXECUTOR_ROLE)
      .withoutLabel(SPARK_EXECUTOR_INACTIVE_LABEL, "true")
      .list()
      .getItems
      .asScala
      .map(pod => KubernetesPod(pod, kubernetesResolver))
      .toSet
  }
}

class KubernetesDatasourcePodPoller(
                                     kubernetesClient: KubernetesClient,
                                     kubernetesResolver: KubernetesResourceResolver,
                                     snapshotsStore: KubernetesSnapshotsStore
                                   ) extends PollerRunnable(snapshotsStore) {
  override def poll(): Set[KubernetesResource] = {
    kubernetesClient
      .pods()
      .inAnyNamespace()
      .withLabel(SPARK_ROLE_LABEL, SPARK_POD_DATASOURCE_ROLE)
      .list()
      .getItems
      .asScala
      .map(pod => KubernetesPod(pod, kubernetesResolver))
      .toSet
  }
}

class KubernetesNodePoller(
                          kubernetesClient: KubernetesClient,
                          snapshotsStore: KubernetesSnapshotsStore
                          ) extends PollerRunnable(snapshotsStore) {
  override def poll(): Set[KubernetesResource] = {
    kubernetesClient
      .nodes()
      .list()
      .getItems
      .asScala
      .toSet
      .map(KubernetesNode)
  }
}

object KubernetesPollingSnapshotSources {
  def apply(conf: SparkConf,
            pollingExecutor: ScheduledExecutorService,
            kubernetesClient: KubernetesClient,
            kubernetesResolver: KubernetesResourceResolver,
            snapshotsStore: KubernetesSnapshotsStore): Seq[KubernetesPollingSnapshotSource] = {
    new KubernetesPollingSnapshotSource(
      conf,
      pollingExecutor,
      applicationId => new KubernetesExecutorPodPoller(
        kubernetesClient, kubernetesResolver, snapshotsStore, applicationId
      )
    ) ::
      new KubernetesPollingSnapshotSource(
        conf,
        pollingExecutor,
        _ => new KubernetesDatasourcePodPoller(
          kubernetesClient, kubernetesResolver, snapshotsStore
        )
      ) ::
      new KubernetesPollingSnapshotSource(
        conf,
        pollingExecutor,
        _ => new KubernetesNodePoller(
          kubernetesClient, snapshotsStore
        )
      ) ::
      Nil
  }
}

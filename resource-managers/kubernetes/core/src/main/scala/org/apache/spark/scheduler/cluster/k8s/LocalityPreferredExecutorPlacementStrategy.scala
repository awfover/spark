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

/**
 * This strategy is calculating the optimal locality preferences of YARN containers by considering
 * the node ratio of pending tasks, number of required cores/containers and locality of current
 * existing and pending allocated containers. The target of this algorithm is to maximize the number
 * of tasks that would run locally.
 *
 * Consider a situation in which we have 20 tasks that require (host1, host2, host3)
 * and 10 tasks that require (host1, host2, host4), besides each container has 2 cores
 * and cpus per task is 1, so the required container number is 15,
 * and host ratio is (host1: 30, host2: 30, host3: 20, host4: 10).
 *
 * 1. If the requested container number (18) is more than the required container number (15):
 *
 * requests for 5 containers with nodes: (host1, host2, host3, host4)
 * requests for 5 containers with nodes: (host1, host2, host3)
 * requests for 5 containers with nodes: (host1, host2)
 * requests for 3 containers with no locality preferences.
 *
 * The placement ratio is 3 : 3 : 2 : 1, and set the additional containers with no locality
 * preferences.
 *
 * 2. If requested container number (10) is less than or equal to the required container number
 * (15):
 *
 * requests for 4 containers with nodes: (host1, host2, host3, host4)
 * requests for 3 containers with nodes: (host1, host2, host3)
 * requests for 3 containers with nodes: (host1, host2)
 *
 * The placement ratio is 10 : 10 : 7 : 4, close to expected ratio (3 : 3 : 2 : 1)
 *
 * 3. If containers exist but none of them can match the requested localities,
 * follow the method of 1 and 2.
 *
 * 4. If containers exist and some of them can match the requested localities.
 * For example if we have 1 container on each node (host1: 1, host2: 1: host3: 1, host4: 1),
 * and the expected containers on each node would be (host1: 5, host2: 5, host3: 4, host4: 2),
 * so the newly requested containers on each node would be updated to (host1: 4, host2: 4,
 * host3: 3, host4: 1), 12 containers by total.
 *
 *   4.1 If requested container number (18) is more than newly required containers (12). Follow
 *   method 1 with an updated ratio 4 : 4 : 3 : 1.
 *
 *   4.2 If request container number (10) is less than newly required containers (12). Follow
 *   method 2 with an updated ratio 4 : 4 : 3 : 1.
 *
 * 5. If containers exist and existing localities can fully cover the requested localities.
 * For example if we have 5 containers on each node (host1: 5, host2: 5, host3: 5, host4: 5),
 * which could cover the current requested localities. This algorithm will allocate all the
 * requested containers with no localities.
 */
private[k8s] object LocalityPreferredExecutorPlacementStrategy {

  val LOCALITY_AWARE_PREFERENCE_WEIGHT = 100
  val LOCALITY_FREE_PREFERENCE_WEIGHT = 90

  /**
   * Calculate each container's node locality and rack locality
   * @param numContainer number of containers to calculate
   * @param numLocalityAwareTasks number of locality required tasks
   * @param hostToLocalTaskCount a map to store the preferred hostname and possible task
   *                             numbers running on it, used as hints for container allocation
   * @param allocatedHostToContainersMap host to allocated containers map, used to calculate the
   *                                     expected locality preference by considering the existing
   *                                     containers
   * @param localityMatchedPendingAllocations A sequence of pending container request which
   *                                          matches the localities of current required tasks.
   * @param rp The ResourceProfile associated with this container.
   * @return node localities and rack localities, each locality is an array of string,
   *         the length of localities is the same as number of containers
   */
  def localityOfRequestedExecutors(
                                     numExecutor: Int,
                                     hostToLocalTaskCount: Map[KubernetesNode, Int],
                                     allocatedHostToExecutorCount: Map[KubernetesNode, Int],
                                     unallocatedHostExecutorPreferences:
                                      Seq[Seq[(KubernetesNode, Int)]]
                                   ): Seq[Seq[(KubernetesNode, Int)]] = {
    val updatedHostToExecutorCount = expectedHostToExecutorCount(
      numExecutor,
      hostToLocalTaskCount,
      allocatedHostToExecutorCount,
      unallocatedHostExecutorPreferences
    )
    val updatedLocalityAwareExecutorNum = updatedHostToExecutorCount.values.sum

    val totalTasks = hostToLocalTaskCount.values.sum
    val hostWeights = hostToLocalTaskCount.map { case (host, num) =>
      host -> num * LOCALITY_FREE_PREFERENCE_WEIGHT / totalTasks
    }

    var executorLocalityPreferences = Seq.empty[Seq[(KubernetesNode, Int)]]
    updatedHostToExecutorCount.foreach { case (host, num) =>
      val preferences = (host -> LOCALITY_AWARE_PREFERENCE_WEIGHT :: Nil) ++
        hostWeights.filter(_._1 != host)
      executorLocalityPreferences ++= Seq.fill(num)(
        preferences
      )
    }

    val requiredLocalityFreeContainerNum = numExecutor - updatedLocalityAwareExecutorNum
    executorLocalityPreferences ++= Seq.fill(requiredLocalityFreeContainerNum)(
      hostWeights.toSeq
    )

    executorLocalityPreferences
  }

  /**
   * Calculate the expected host to number of containers by considering with allocated containers.
   * @param localityAwareTasks number of locality aware tasks
   * @param hostToLocalTaskCount a map to store the preferred hostname and possible task
   *                             numbers running on it, used as hints for container allocation
   * @param allocatedHostToContainersMap host to allocated containers map, used to calculate the
   *                                     expected locality preference by considering the existing
   *                                     containers
   * @param localityMatchedPendingAllocations A sequence of pending container request which
   *                                          matches the localities of current required tasks.
   * @return a map with hostname as key and required number of containers on this host as value
   */
  private def expectedHostToExecutorCount(
                                            numExecutor: Int,
                                            hostToLocalTaskCount: Map[KubernetesNode, Int],
                                            allocatedHostToExecutorCount: Map[KubernetesNode, Int],
                                            unallocatedHostExecutorPreferences:
                                              Seq[Seq[(KubernetesNode, Int)]]
                                          ): Map[KubernetesNode, Int] = {
    val unallocatedHostToExecutorCount = estimateUnallocatedHostToExecutorCount(
      unallocatedHostExecutorPreferences
    )

    val totalLocalTaskNum = hostToLocalTaskCount.values.sum

    hostToLocalTaskCount.map { case (host, count) =>
      val expectedCount =
        count.toDouble * numExecutor / totalLocalTaskNum
      // Take the locality of pending containers into consideration
      val existedCount = allocatedHostToExecutorCount
          .getOrElse(host, 0).toDouble +
        unallocatedHostToExecutorCount.getOrElse(host, 0.0)

      // If existing container can not fully satisfy the expected number of container,
      // the required container number is expected count minus existed count. Otherwise the
      // required container number is 0.
      (host, math.max(0, (expectedCount - existedCount).ceil.toInt))
    }
  }

  private def estimateUnallocatedHostToExecutorCount(
    unallocatedHostExecutorPreferences: Seq[Seq[(KubernetesNode, Int)]]
  ): Map[KubernetesNode, Double] = {
    unallocatedHostExecutorPreferences
      .flatMap { preferences =>
        val totalWeights = preferences.map { case (_, p) => p }.sum
        preferences.map { case (node, p) =>
          node -> p.toDouble / totalWeights
        }
      }
      .groupBy(_._1)
      .map { case (node, weights) => node -> weights.map(_._2).sum }
  }
}

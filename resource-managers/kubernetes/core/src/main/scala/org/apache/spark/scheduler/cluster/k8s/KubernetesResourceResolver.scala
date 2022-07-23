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
import org.apache.commons.validator.routines.InetAddressValidator
import scala.collection.mutable

import org.apache.spark.SparkConf
import org.apache.spark.deploy.k8s.Config.{KUBERNETES_ALLOCATION_BATCH_DELAY, KUBERNETES_RESOURCE_RESOLVER_MODE}
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{LiveListenerBus, SparkListenerKubernetesResourceResolverMeasurement}
import org.apache.spark.scheduler.cluster.k8s.KubernetesKinds.{KUBERNETES_NODE, KUBERNETES_POD}
import org.apache.spark.scheduler.cluster.k8s.KubernetesLabels.KUBERNETES_HOSTNAME
import org.apache.spark.scheduler.cluster.k8s.KubernetesResourceResolver.isValidIp
import org.apache.spark.util.Utils.tryLogNonFatalError


private[spark] class KubernetesResourceResolver(
                                                 conf: SparkConf,
                                                 listenerBus: LiveListenerBus,
                                                 kubernetesClient: KubernetesClient,
                                                 snapshotsStore: KubernetesSnapshotsStore
                                               ) extends Logging {
  private val mode = ResolverMode.withName(conf.get(KUBERNETES_RESOURCE_RESOLVER_MODE))
  private val podAllocationDelay = conf.get(KUBERNETES_ALLOCATION_BATCH_DELAY)

  private val defaultKinds = Seq(KUBERNETES_POD, KUBERNETES_NODE)

  private val nameMap = new mutable.HashMap[String, KubernetesResource]()
  private val hostnameMap = new mutable.HashMap[String, KubernetesResource]()
  private val ipMap = new mutable.HashMap[String, KubernetesResource]()

  private val caches = Map(
    "name" -> nameMap,
    "hostname" -> hostnameMap,
    "ip" -> ipMap
  )

  private val indexes = mode match {
    case ResolverMode.Off =>
      Map.empty[String, Seq[(String, String, Seq[String]) => Option[KubernetesResource]]]

    case ResolverMode.Direct =>
      Map[String, Seq[(String, String, Seq[String]) => Option[KubernetesResource]]](
        "name" -> Seq(search(searchByName)),
        "hostname" -> Seq(search(checkHostname(searchByHostname))),
        "ip" -> Seq(search(checkIp(searchByIp)))
      )

    case ResolverMode.Cached =>
      Map[String, Seq[(String, String, Seq[String]) => Option[KubernetesResource]]](
        "name" -> Seq(get, search(searchByName)),
        "hostname" -> Seq(get, search(checkHostname(searchByHostname))),
        "ip" -> Seq(get, search(checkIp(searchByIp)))
      )
  }

  private def measure[T](
                          phase: String,
                          key: String,
                          value: String,
                          kinds: Seq[String]
                        )(
                          f: => Option[T]
                        ): Option[T] = {
    val startTime = System.currentTimeMillis()
    val ret = f
    val endTime = System.currentTimeMillis

    listenerBus.post(
      SparkListenerKubernetesResourceResolverMeasurement(
        phase,
        key,
        value,
        kinds,
        ret.isDefined,
        endTime - startTime
      )
    )

    ret
  }

  def start(applicationId: String): Unit = {
    snapshotsStore.addSubscriber(podAllocationDelay) {
      onNewSnapshots
    }
  }

  private def onNewSnapshots(snapshots: Seq[KubernetesSnapshot]): Unit = {
    if (snapshots.nonEmpty) {
      val resources = snapshots.last.resources
      nameMap ++= resources.map(r => r.name -> r).toMap
      hostnameMap ++= resources.filter(_.hostname.isDefined).map(r => r.hostname.get -> r).toMap
      ipMap ++= resources.flatMap(r => r.ips.map(ip => ip -> r)).toMap
    }
  }

  def get(key: String, value: String, kinds: Seq[String]): Option[KubernetesResource] =
    measure("cache", key, value, kinds) {
      caches.get(key) match {
        case Some(map) =>
          map.get(value) match {
            case Some(rc) if kinds.contains(rc.kind) => Some(rc)
            case _ => None
          }

        case _ => None
      }
    }

  def search(func: (String, String, String) => Option[KubernetesResource])
            (key: String, value: String, kinds: Seq[String]): Option[KubernetesResource] =
    kinds
      .view
      .map(func(key, value, _))
      .collectFirst { case Some(rc) => rc }

  def getOrSearch(
                   keys: Seq[String],
                   value: String,
                   kinds: Seq[String]
                 ): Option[KubernetesResource] = {
    val transformedKeys = if (keys.contains("ip")) {
      if (isValidIp(value)) {
        if (keys.head.equals("ip")) {
          keys
        } else {
          Seq("ip") ++ keys.filterNot(_.equals("ip"))
        }
      } else {
        keys.filterNot(_.equals("ip"))
      }
    } else {
      keys
    }

    transformedKeys
      .view
      .map(getOrSearch(_, value, kinds))
      .collectFirst { case Some(rc) => rc }
  }

  def getOrSearch(
                   key: String,
                   value: String,
                   kinds: Seq[String] = defaultKinds
                 ): Option[KubernetesResource] = indexes.get(key) match {
    case Some(get) =>
      get.view.map(_ (key, value, kinds)).collectFirst { case Some(rc) => rc }

    case _ =>
      logWarning(s"Kubernetes resource key $key is not supported")
      None
  }

  def checkHostname(func: (String, String, String) => Option[KubernetesResource])
                   (key: String, hostname: String, kind: String): Option[KubernetesResource] =
    if (hostname.length <= 63) {
      func(key, hostname, kind)
    } else {
      None
    }

  def checkIp(func: (String, String, String) => Option[KubernetesResource])
             (key: String, ip: String, kind: String): Option[KubernetesResource] =
    if (isValidIp(ip)) {
      func(key, ip, kind)
    } else {
      None
    }

  def searchByName(key: String, name: String, kind: String): Option[KubernetesResource] = {
    tryLogNonFatalError {
      return measure("search", key, name, Seq(kind)) {
        kind match {
          case KUBERNETES_NODE =>
            val node = KubernetesNode(
              kubernetesClient
                .nodes()
                .withName(name)
                .get()
            )
            nameMap += (name -> node)
            Some(node)

          case _ => None
        }
      }
    }
    None
  }

  def searchByHostname(key: String, hostname: String, kind: String): Option[KubernetesResource] = {
    tryLogNonFatalError {
      return measure("search", key, hostname, Seq(kind)) {
        kind match {
          case KUBERNETES_NODE =>
            val nodes = kubernetesClient
              .nodes
              .withLabel(KUBERNETES_HOSTNAME, hostname)
              .list
              .getItems
            if (nodes.isEmpty) {
              None
            } else {
              val node = KubernetesNode(nodes.get(0))
              hostnameMap += (hostname -> node)
              Some(node)
            }

          case KUBERNETES_POD =>
            val pods = kubernetesClient
              .pods
              .inAnyNamespace
              .withLabel(KUBERNETES_HOSTNAME, hostname)
              .list
              .getItems
            if (pods.isEmpty) {
              None
            } else {
              val pod = KubernetesPod(pods.get(0), this)
              hostnameMap += (hostname -> pod)
              Some(pod)
            }

          case _ => None
        }
      }

    }
    None
  }

  def searchByIp(key: String, ip: String, kind: String): Option[KubernetesResource] = {
    tryLogNonFatalError {
      return measure("search", key, ip, Seq(kind)) {
        kind match {
          case KUBERNETES_POD =>
            val pods = kubernetesClient
              .pods
              .inAnyNamespace
              .withField("status.podIP", ip)
              .list
              .getItems
            if (pods.isEmpty) {
              None
            } else {
              val pod = KubernetesPod(pods.get(0), this)
              ipMap += (ip -> pod)
              Some(pod)
            }

          case _ => None
        }
      }
    }

    None
  }
}

object ResolverMode extends Enumeration {
  type Mode = Value
  val Off, Direct, Cached = Value
}

object KubernetesResourceResolver {
  def isValidIp(ip: String): Boolean = {
    InetAddressValidator.getInstance().isValid(ip)
  }
}

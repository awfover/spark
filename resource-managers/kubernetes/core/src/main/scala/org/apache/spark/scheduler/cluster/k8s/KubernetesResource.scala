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

import com.google.common.base.Objects
import io.fabric8.kubernetes.api.model.{Node, Pod}
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.cluster.k8s.KubernetesKinds.{KUBERNETES_NODE, KUBERNETES_POD}

sealed trait KubernetesResourceKey
case class KubernetesResourceName(key: String) extends KubernetesResourceKey
case class KubernetesResourceHostname(key: String) extends KubernetesResourceKey
case class KubernetesResourceIp(key: String) extends KubernetesResourceKey

sealed trait KubernetesResource {
  def uid: String
  def name: String
  def kind: String
  def hostname: Option[String]
  def ips: Set[String]

  override def equals(obj: Any): Boolean = obj match {
    case other: KubernetesResource =>
      uid == other.uid
    case _ => false
  }

  override def hashCode(): Int = Objects.hashCode(uid)
}

case class KubernetesPod(
                          pod: Pod,
                          resolver: KubernetesResourceResolver
                        ) extends KubernetesResource {
  import KubernetesResource._

  override def uid: String = safeGet(
    { pod.getMetadata.getUid },
    "Undefined"
  )

  override def name: String = safeGet(
    { pod.getMetadata.getName },
    uid
  )

  override def kind: String = KUBERNETES_POD

  override def hostname: Option[String] = safeGet(
    { Option(pod.getSpec.getHostname) },
    None
  )

  override def ips: Set[String] = safeGet(
    { pod.getStatus.getPodIPs.asScala.map(_.getIp).toSet },
    Set.empty
  )

  val nodeName: Option[String] = safeGet(
    { Option(pod.getSpec.getNodeName) },
    None
  )

  lazy val node: Option[KubernetesNode] = nodeName
    .flatMap(resolver.getOrSearch("name", _, KUBERNETES_NODE :: Nil))
    .flatMap {
      case node: KubernetesNode => Some(node)
      case _ => None
    }
}

private[spark] case class KubernetesNode(node: Node) extends KubernetesResource {
  import KubernetesLabels._
  import KubernetesResource._

  val DEFAULT_REGION = "region"
  val DEFAULT_ZONE = "zone"

  val region: Option[String] =
    safeGet(
      { Option(node.getMetadata.getLabels.get(KUBERNETES_TOPOLOGY_REGION)) },
      None
    )

  val zone: Option[String] =
    safeGet(
      { Option(node.getMetadata.getLabels.get(KUBERNETES_TOPOLOGY_ZONE)) },
      None
    )

  val zoneLabel: String = s"${region.getOrElse(DEFAULT_REGION)}/${zone.getOrElse(DEFAULT_ZONE)}"

  override def equals(obj: Any): Boolean = obj match {
    case other: KubernetesNode =>
      uid == other.uid

    case _ => false
  }

  override def hashCode(): Int = Objects.hashCode(uid)

  override def uid: String =
    safeGet(
      { node.getMetadata.getUid },
      "Undefined"
    )

  override def name: String =
    safeGet(
      { node.getMetadata.getName },
      uid
    )

  override def kind: String = KUBERNETES_NODE

  override def hostname: Option[String] = safeGet(
    { Option(node.getMetadata.getLabels.get(KUBERNETES_HOSTNAME)) },
    None
  )

  override def ips: Set[String] = safeGet(
    {
      node
        .getStatus
        .getAddresses
        .asScala
        .filter(addr => "InternalIP".equals(addr.getType))
        .map(_.getAddress)
        .toSet
    },
    Set.empty
  )
}

object KubernetesResource extends Logging {
  def safeGet[T](supplier: => T, defaultValue: T): T = {
    try {
      supplier
    } catch {
      case NonFatal(t) =>
        logError(s"Get information from k8s resource error", t)
        defaultValue
    }
  }

  def getNode(resource: KubernetesResource): Option[KubernetesNode] = resource match {
    case pod: KubernetesPod =>
      pod.node

    case node: KubernetesNode =>
      Some(node)

    case _ => None
  }
}

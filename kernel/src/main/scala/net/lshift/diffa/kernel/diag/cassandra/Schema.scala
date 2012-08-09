/**
 * Copyright (C) 2010-2012 LShift Ltd.
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

package net.lshift.diffa.kernel.diag.cassandra

import me.prettyprint.cassandra.model.ConfigurableConsistencyLevel
import me.prettyprint.cassandra.serializers._
import me.prettyprint.hector.api.{Serializer, Cluster}
import me.prettyprint.hector.api.HConsistencyLevel.{ANY, ONE}
import me.prettyprint.hector.api.ddl.{ColumnFamilyDefinition, KeyspaceDefinition, ComparatorType}
import me.prettyprint.hector.api.ddl.ComparatorType._
import me.prettyprint.hector.api.factory.HFactory
import scala.collection.JavaConversions._

/**
 * Manages the Cassandra schema for explain logs.
 */
class Schema(cluster: Cluster,
             keyspaceName: String = "diffa",
             replicaPlacementStrategy: String = "org.apache.cassandra.locator.SimpleStrategy",
             replicationFactor: Int = 1) {

  lazy val keyspace =
    HFactory.createKeyspace(keyspaceName, cluster, consistencyLevel)

  lazy val consistencyLevel = {
    val cl = new ConfigurableConsistencyLevel()
    cl.setDefaultReadConsistencyLevel(ONE)
    cl.setDefaultWriteConsistencyLevel(ANY)
    cl
  }


  lazy val keyspaceDef = HFactory.createKeyspaceDefinition(keyspaceName,
                                                           replicaPlacementStrategy,
                                                           replicationFactor,
                                                           List(explainLogEntries, explainLogAttachments))

  case class Serializers[K, N, V](rowKey: Serializer[K], columnKey: Serializer[N], value: Serializer[V])

  lazy val explainLogEntries = columnFamily("ExplainLogEntries", TIMEUUIDTYPE, Some(UTF8TYPE))

  val explainLogEntriesSerializers =
    Serializers(rowKey = StringSerializer.get, columnKey = UUIDSerializer.get, value = StringSerializer.get)

  lazy val explainLogAttachments = columnFamily("ExplainLogAttachments", UTF8TYPE, Some(UTF8TYPE))

  val explainLogAttachmentsSerializers =
    Serializers(rowKey = StringSerializer.get, columnKey = StringSerializer.get, value = StringSerializer.get)

  def init() {
    val existingKeyspace = cluster.describeKeyspace(keyspaceName)
    if (existingKeyspace == null) {
      // create keyspace
      cluster.addKeyspace(keyspaceDef, true)
    } else {
      // conservative schema migration - only add new column families
      val existingCfNames = getColumnFamilyNames(existingKeyspace)
      val proposedCfNames = getColumnFamilyNames(keyspaceDef)
      val newCfNames = proposedCfNames -- existingCfNames
      for {
        cfName <- newCfNames
        cf <- getColumnFamilyByName(cfName, keyspaceDef)
      } cluster.addColumnFamily(cf, true)
    }
  }

  private def getColumnFamilyNames(keyspaceDef: KeyspaceDefinition) =
    Set.empty ++ keyspaceDef.getCfDefs.map(_.getName)

  private def getColumnFamilyByName(name: String, keyspaceDef: KeyspaceDefinition) =
    keyspaceDef.getCfDefs.find(_.getName == name)

  private def columnFamily(name: String,
                           columnKeyComparator: ComparatorType,
                           rowKeyValidationClass: Option[ComparatorType] = None) = {
    val cfDef = HFactory.createColumnFamilyDefinition(keyspaceName, name, columnKeyComparator)

    for (c <- rowKeyValidationClass) {
      cfDef.setKeyValidationClass(c.getClassName)
    }

    cfDef
  }


}
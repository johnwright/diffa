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
package net.lshift.diffa.agent.rest

import java.lang.{String, Class}
import javax.ws.rs.core.{MultivaluedMap, MediaType}
import java.lang.annotation.Annotation
import java.lang.reflect.Type
import javax.ws.rs.ext.{MessageBodyWriter, MessageBodyReader, Provider}
import javax.ws.rs.{Consumes, Produces}
import org.springframework.core.io.ClassPathResource
import org.springframework.oxm.castor.CastorMarshaller
import javax.xml.transform.Result
import java.io.{StringWriter, OutputStream, InputStream}
import javax.xml.transform.stream.{StreamSource, StreamResult}
import scala.collection.JavaConversions._
import reflect.BeanProperty
import net.lshift.diffa.kernel.config._
import net.lshift.diffa.kernel.differencing.PairScanState
import net.lshift.diffa.kernel.frontend.DiffaConfig

/**
 * Provider for encoding and decoding diffa configuration blocks.
 */
@Provider
@Produces(Array("application/xml"))
@Consumes(Array("application/xml"))
class DiffaConfigReaderWriter
    extends MessageBodyReader[DiffaConfig]
    with MessageBodyWriter[DiffaConfig] {

  val marshaller = new CastorMarshaller();
  marshaller.setMappingLocation(new ClassPathResource("/net/lshift/diffa/agent/rest/castorMapping.xml"));
  marshaller.afterPropertiesSet();

  def isReadable(propType : Class[_], genericType: Type, annotations: Array[Annotation], mediaType: MediaType) =
    classOf[DiffaConfig].isAssignableFrom(propType)
  def isWriteable(propType : Class[_], genericType: Type, annotations: Array[Annotation], mediaType: MediaType) =
    classOf[DiffaConfig].isAssignableFrom(propType)

  def readFrom(propType: Class[DiffaConfig], genericType: Type, annotations: Array[Annotation], mediaType: MediaType, httpHeaders: MultivaluedMap[String, String], entityStream: InputStream) =
    marshaller.unmarshal(new StreamSource(entityStream)).asInstanceOf[DiffaCastorSerializableConfig].toDiffaConfig

  def writeTo(t: DiffaConfig, `type` : Class[_], genericType: Type, annotations: Array[Annotation], mediaType: MediaType, httpHeaders: MultivaluedMap[String, AnyRef], entityStream: OutputStream) = {
    var r = new StreamResult(entityStream)
    marshaller.marshal((new DiffaCastorSerializableConfig).fromDiffaConfig(t), r)
  }

  def getSize(t: DiffaConfig, `type` : Class[_], genericType: Type, annotations: Array[Annotation], mediaType: MediaType) = {
    var sw = new StringWriter
    var r = new StreamResult(sw)
    marshaller.marshal((new DiffaCastorSerializableConfig).fromDiffaConfig(t), r)

    sw.toString.length
  }
}

//
// The below types are essential private the DiffaConfigReaderWriter, and are used simply to allow for clean xml
// to be mapped to/from by Castor. Our internal types simply don't serialize to XML cleanly, and we don't want to live
// with the mess they'd be in if we adjusted them to be so.
//


/**
 * Describes a complete diffa configuration.
 */
class DiffaCastorSerializableConfig {
  @BeanProperty var users:java.util.List[User] = new java.util.ArrayList[User]
  @BeanProperty var properties:java.util.List[DiffaProperty] = new java.util.ArrayList[DiffaProperty]
  @BeanProperty var endpoints:java.util.List[CastorSerializableEndpoint] = new java.util.ArrayList[CastorSerializableEndpoint]
  @BeanProperty var groups:java.util.List[CastorSerializableGroup] = new java.util.ArrayList[CastorSerializableGroup]

  def fromDiffaConfig(c:DiffaConfig) = {
    this.users = c.users.toList
    this.properties = c.properties.map { case (k, v) => new DiffaProperty(k, v) }.toList
    this.endpoints = c.endpoints.map { e => (new CastorSerializableEndpoint).fromDiffaEndpoint(e) }.toList
    this.groups = c.groups.map(g => {
      def repairActionsForPair(pairKey: String) = c.repairActions.filter(_.pairKey == pairKey).toList
      val pairs = c.pairs.filter(_.groupKey == g.key).map(p => CastorSerializablePair.fromPairDef(p, repairActionsForPair(p.pairKey))).toList
      new CastorSerializableGroup(g.key, pairs)
    }).toList

    this
  }

  def toDiffaConfig:DiffaConfig =
    DiffaConfig(
      users = users.toSet,
      properties = properties.map(p => p.key -> p.value).toMap,
      endpoints = endpoints.map(_.toDiffaEndpoint).toSet,
      groups = groups.map(g => PairGroup(g.name)).toSet,
      pairs = (for (g <- groups; p <- g.pairs) yield p.toPairDef(g.name)).toSet,
      repairActions = (for (g <- groups; p <- g.pairs; a <- p.repairActions) yield { a.pairKey = p.key ; a }).toSet
    )

}

class DiffaProperty(@BeanProperty var key:String, @BeanProperty var value:String) {
  def this() = this(null, null)
}

class CastorSerializableEndpoint {
  @BeanProperty var name: String = null
  @BeanProperty var scanUrl: String = null
  @BeanProperty var contentRetrievalUrl: String = null
  @BeanProperty var versionGenerationUrl: String = null
  @BeanProperty var contentType: String = null
  @BeanProperty var inboundUrl: String = null
  @BeanProperty var inboundContentType: String = null
  @BeanProperty var rangeCategories: java.util.List[CastorSerializableRangeCategoryDescriptor] = new java.util.ArrayList[CastorSerializableRangeCategoryDescriptor]
  @BeanProperty var prefixCategories: java.util.List[CastorSerializablePrefixCategoryDescriptor] = new java.util.ArrayList[CastorSerializablePrefixCategoryDescriptor]
  @BeanProperty var setCategories: java.util.List[CastorSerializableSetCategoryDescriptor] = new java.util.ArrayList[CastorSerializableSetCategoryDescriptor]

  def fromDiffaEndpoint(e:Endpoint) = {
    this.name = e.name
    this.scanUrl = e.scanUrl
    this.contentRetrievalUrl = e.contentRetrievalUrl
    this.versionGenerationUrl = e.versionGenerationUrl
    this.contentType = e.contentType
    this.inboundUrl = e.inboundUrl
    this.inboundContentType = e.inboundContentType

    this.rangeCategories = e.categories.filter { case (key, cat) => cat.isInstanceOf[RangeCategoryDescriptor] }.
      map { case (key, cat) => new CastorSerializableRangeCategoryDescriptor(key, cat.asInstanceOf[RangeCategoryDescriptor]) }.toList
    this.prefixCategories = e.categories.filter { case (key, cat) => cat.isInstanceOf[PrefixCategoryDescriptor] }.
      map { case (key, cat) => new CastorSerializablePrefixCategoryDescriptor(key, cat.asInstanceOf[PrefixCategoryDescriptor]) }.toList
    this.setCategories = e.categories.filter { case (key, cat) => cat.isInstanceOf[SetCategoryDescriptor] }.
      map { case (key, cat) => new CastorSerializableSetCategoryDescriptor(key, cat.asInstanceOf[SetCategoryDescriptor]) }.toList

    this
  }

  def toDiffaEndpoint =
    Endpoint(
      name = name, contentType = contentType, inboundUrl = inboundUrl, inboundContentType = inboundContentType,
      scanUrl = scanUrl, contentRetrievalUrl = contentRetrievalUrl, versionGenerationUrl = versionGenerationUrl,
      categories =
        rangeCategories.map(c => c.name -> c.toRangeCategoryDescriptor).toMap[String, CategoryDescriptor] ++
        prefixCategories.map(c => c.name -> c.toPrefixCategoryDescriptor).toMap[String, CategoryDescriptor] ++
        setCategories.map(c => c.name -> c.toSetCategoryDescriptor).toMap[String, CategoryDescriptor]
    )
}

class CastorSerializableRangeCategoryDescriptor(@BeanProperty var name:String, @BeanProperty var dataType:String,
                                          @BeanProperty var lower:String, @BeanProperty var upper:String) {

  def this() = this(null, null, null, null)
  def this(name:String, rcd:RangeCategoryDescriptor) = this(name, rcd.dataType, rcd.lower, rcd.upper)

  def toRangeCategoryDescriptor = new RangeCategoryDescriptor(dataType, lower, upper)
}
class CastorSerializablePrefixCategoryDescriptor(@BeanProperty var name:String, @BeanProperty var prefixLength:Int,
                                           @BeanProperty var maxLength:Int, @BeanProperty var step:Int) {

  def this() = this(null, 1, 1, 1)
  def this(name:String, pcd:PrefixCategoryDescriptor) = this(name, pcd.prefixLength, pcd.maxLength, pcd.step)

  def toPrefixCategoryDescriptor = new PrefixCategoryDescriptor(prefixLength, maxLength, step)
}
class CastorSerializableSetCategoryDescriptor(@BeanProperty var name:String, @BeanProperty var values:java.util.Set[SetValue]) {
  def this() = this(null, new java.util.HashSet[SetValue])
  def this(name:String, scd:SetCategoryDescriptor) = this(name, scd.values.map(v => new SetValue(v)).toSet)

  def toSetCategoryDescriptor = new SetCategoryDescriptor(new java.util.HashSet(values.map(v => v.value).toList))
}
class SetValue(@BeanProperty var value:String) {
  def this() = this(null)
}

class CastorSerializableGroup(
    @BeanProperty var name:String,
    @BeanProperty var pairs:java.util.List[CastorSerializablePair] = new java.util.ArrayList[CastorSerializablePair]
) {
  def this() = this(name = null)
}

class CastorSerializablePair(
  @BeanProperty var key: String = null,
  @BeanProperty var upstream: String = null,
  @BeanProperty var downstream: String = null,
  @BeanProperty var versionPolicy: String = null,
  @BeanProperty var matchingTimeout: Int = 0,
  @BeanProperty var repairActions: java.util.List[RepairAction] = new java.util.ArrayList[RepairAction],
  @BeanProperty var scanCronSpec: String = null
) {
  def this() = this(key = null)

  def toPairDef(groupKey: String): PairDef =
    new PairDef(key, versionPolicy, matchingTimeout, upstream, downstream, groupKey, scanCronSpec)
}

object CastorSerializablePair {
  def fromPairDef(p: PairDef, repairActions: java.util.List[RepairAction]): CastorSerializablePair =
    new CastorSerializablePair(p.pairKey, p.upstreamName, p.downstreamName, p.versionPolicyName, p.matchingTimeout,
                               repairActions, p.scanCronSpec)
}

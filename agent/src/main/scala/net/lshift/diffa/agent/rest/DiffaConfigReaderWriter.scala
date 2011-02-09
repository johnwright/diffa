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

import net.lshift.diffa.kernel.frontend.DiffaConfig
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
    marshaller.unmarshal(new StreamSource(entityStream)).asInstanceOf[DiffaSerialisableConfig].toDiffaConfig

  def writeTo(t: DiffaConfig, `type` : Class[_], genericType: Type, annotations: Array[Annotation], mediaType: MediaType, httpHeaders: MultivaluedMap[String, AnyRef], entityStream: OutputStream) = {
    var r = new StreamResult(entityStream)
    marshaller.marshal((new DiffaSerialisableConfig).fromDiffaConfig(t), r)
  }

  def getSize(t: DiffaConfig, `type` : Class[_], genericType: Type, annotations: Array[Annotation], mediaType: MediaType) = {
    var sw = new StringWriter
    var r = new StreamResult(sw)
    marshaller.marshal((new DiffaSerialisableConfig).fromDiffaConfig(t), r)

    sw.toString.length
  }
}

/**
 * Describes a complete diffa configuration.
 */
class DiffaSerialisableConfig {
  @BeanProperty var users:java.util.List[User] = new java.util.ArrayList[User]
  @BeanProperty var properties:java.util.Map[String, String] = new java.util.HashMap[String, String]
  @BeanProperty var endpoints:java.util.List[SerialisableEndpoint] = new java.util.ArrayList[SerialisableEndpoint]
  @BeanProperty var groups:java.util.List[SerialisableGroup] = new java.util.ArrayList[SerialisableGroup]

  def fromDiffaConfig(c:DiffaConfig) = {
    this.users = c.users
    this.properties = c.properties
    this.endpoints = c.endpoints.map { e => (new SerialisableEndpoint).fromDiffaEndpoint(e) }
    this.groups = c.groups.map(g => new SerialisableGroup(g.key, c.pairs.filter(p => p.groupKey == g.key)))

    this
  }

  def toDiffaConfig:DiffaConfig = {
    DiffaConfig(
      users = users.toList,
      properties = properties.toMap,
      endpoints = endpoints.map(_.toDiffaEndpoint).toList,
      groups = groups.map(g => PairGroup(g.name)).toList,
      pairs = groups.flatMap(g => g.pairs.map { p => { p.groupKey = g.name; p } }).toList)
  }
}

class SerialisableEndpoint {
  @BeanProperty var name: String = null
  @BeanProperty var url: String = null
  @BeanProperty var contentType: String = null
  @BeanProperty var inboundUrl: String = null
  @BeanProperty var inboundContentType: String = null
  @BeanProperty var rangeCategories: java.util.List[SerialisableRangeCategoryDescriptor] = new java.util.ArrayList[SerialisableRangeCategoryDescriptor]
  @BeanProperty var prefixCategories: java.util.List[SerialisablePrefixCategoryDescriptor] = new java.util.ArrayList[SerialisablePrefixCategoryDescriptor]
  @BeanProperty var setCategories: java.util.List[SerialisableSetCategoryDescriptor] = new java.util.ArrayList[SerialisableSetCategoryDescriptor]

  def fromDiffaEndpoint(e:Endpoint) = {
    this.name = e.name
    this.url = e.url
    this.contentType = e.contentType
    this.inboundUrl = e.inboundUrl
    this.inboundContentType = e.inboundContentType

    this.rangeCategories = e.categories.filter { case (key, cat) => cat.isInstanceOf[RangeCategoryDescriptor] }.
      map { case (key, cat) => new SerialisableRangeCategoryDescriptor(key, cat.asInstanceOf[RangeCategoryDescriptor]) }.toList
    this.prefixCategories = e.categories.filter { case (key, cat) => cat.isInstanceOf[PrefixCategoryDescriptor] }.
      map { case (key, cat) => new SerialisablePrefixCategoryDescriptor(key, cat.asInstanceOf[PrefixCategoryDescriptor]) }.toList
    this.setCategories = e.categories.filter { case (key, cat) => cat.isInstanceOf[SetCategoryDescriptor] }.
      map { case (key, cat) => new SerialisableSetCategoryDescriptor(key, cat.asInstanceOf[SetCategoryDescriptor]) }.toList

    this
  }

  def toDiffaEndpoint =
    Endpoint(
      name = name, url = url, contentType = contentType, inboundUrl = inboundUrl, inboundContentType = inboundContentType,
      categories =
        rangeCategories.map(c => c.name -> c.toRangeCategoryDescriptor).toMap[String, CategoryDescriptor] ++
        prefixCategories.map(c => c.name -> c.toPrefixCategoryDescriptor).toMap[String, CategoryDescriptor] ++
        setCategories.map(c => c.name -> c.toSetCategoryDescriptor).toMap[String, CategoryDescriptor]
    )
}

class SerialisableRangeCategoryDescriptor(@BeanProperty var name:String, dataType:String, lower:String, upper:String)
  extends RangeCategoryDescriptor(dataType, lower, upper) {

  def this() = this(null, null, null, null)
  def this(name:String, rcd:RangeCategoryDescriptor) = this(name, rcd.dataType, rcd.lower, rcd.upper)

  def toRangeCategoryDescriptor = new RangeCategoryDescriptor(dataType, lower, upper)
}
class SerialisablePrefixCategoryDescriptor(@BeanProperty var name:String, prefixLength:Int, maxLength:Int, step:Int)
  extends PrefixCategoryDescriptor(prefixLength, maxLength, step) {

  def this() = this(null, 1, 1, 1)
  def this(name:String, pcd:PrefixCategoryDescriptor) = this(name, pcd.prefixLength, pcd.maxLength, pcd.step)

  def toPrefixCategoryDescriptor = new PrefixCategoryDescriptor(prefixLength, maxLength, step)
}
class SerialisableSetCategoryDescriptor(@BeanProperty var name:String, @BeanProperty var values:java.util.Set[SetValue]) {
  def this() = this(null, new java.util.HashSet[SetValue])
  def this(name:String, scd:SetCategoryDescriptor) = this(name, scd.values.map(v => new SetValue(v)).toSet)

  def toSetCategoryDescriptor = new SetCategoryDescriptor(new java.util.HashSet(values.map(v => v.value).toList))
}
class SetValue(@BeanProperty var value:String) {
  def this() = this(null)
}

class SerialisableGroup(@BeanProperty var name:String, @BeanProperty var pairs:java.util.List[PairDef]) {
  def this() = this(null, null)
}
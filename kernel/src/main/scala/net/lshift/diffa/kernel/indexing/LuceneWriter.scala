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
package net.lshift.diffa.kernel.indexing

import org.apache.lucene.store.Directory
import org.slf4j.LoggerFactory
import net.lshift.diffa.kernel.events.VersionID
import org.apache.lucene.util.Version
import org.apache.lucene.analysis.standard.StandardAnalyzer
import net.lshift.diffa.kernel.differencing._
import org.joda.time.{LocalDate, DateTimeZone, DateTime}
import org.apache.lucene.document.{NumericField, Fieldable, Field, Document}
import collection.mutable.{HashSet, HashMap}
import scala.collection.JavaConversions._
import org.apache.lucene.index.{IndexReader, IndexWriter, IndexWriterConfig, Term}
import org.apache.commons.codec.binary.Hex
import java.nio.charset.Charset
import java.lang.StringBuffer

class LuceneWriter(index: Directory) extends ExtendedVersionCorrelationWriter {
  import LuceneVersionCorrelationHandler._

  private val log = LoggerFactory.getLogger(getClass)

  private val maxBufferSize = 10000

  private val updatedDocs = HashMap[VersionID, Document]()
  private val deletedDocs = HashSet[VersionID]()
  private var writer = createIndexWriter

  def getReader : IndexReader = IndexReader.open(writer, true)

  def storeUpstreamVersion(id:VersionID, attributes:scala.collection.immutable.Map[String,TypedAttribute], lastUpdated: DateTime, vsn: String) = {
    log.trace("Indexing upstream " + id + " with attributes: " + attributes + " lastupdated at " + lastUpdated + " with version " + vsn)

    val currentVersion = computeIndexEntryVersion(attributes, true, vsn)

    doDocUpdate(id, lastUpdated, currentVersion, "up.", doc => {
      // Update all of the upstream attributes
      applyAttributes(doc, "up.", attributes)
      updateField(doc, boolField(Upstream.presenceIndicator, true))
      updateField(doc, stringField("uvsn", vsn))
    })
  }

  private def computeIndexEntryVersion(attributes:scala.collection.immutable.Map[String,TypedAttribute], presence:Boolean, versions:String*) : String = {
    val buffer = new StringBuffer()

    attributes.toSeq.sortBy(_._1).foreach{ case (name,attribute) => {
      buffer.append(name)
      buffer.append(attribute.value)
    }}

    buffer.append(presence)
    versions.foreach(buffer.append(_))

    val digest = java.security.MessageDigest.getInstance("MD5")
    val bytes = buffer.toString().getBytes(Charset.forName("UTF-8"))
    digest.update(bytes, 0, bytes.length)
    new String(Hex.encodeHex(digest.digest()))
  }

  def storeDownstreamVersion(id: VersionID, attributes: scala.collection.immutable.Map[String, TypedAttribute], lastUpdated: DateTime, uvsn: String, dvsn: String) = {

    val currentVersion = computeIndexEntryVersion(attributes, true, uvsn, dvsn)

    log.trace("Indexing downstream " + id + " with attributes: " + attributes)
    doDocUpdate(id, lastUpdated, currentVersion, "down.", doc => {
      // Update all of the upstream attributes
      applyAttributes(doc, "down.", attributes)
      updateField(doc, boolField(Downstream.presenceIndicator, true))
      updateField(doc, stringField("duvsn", uvsn))
      updateField(doc, stringField("ddvsn", dvsn))
    })
  }

  def clearUpstreamVersion(id:VersionID) = {
    doClearAttributes(id, doc => {
      if (doc.get("duvsn") == null) {
        false // Remove the document
      } else {
        // Remove all the upstream attributes. Convert to list as middle-step to prevent ConcurrentModificationEx - see #177
        doc.getFields.toList.foreach(f => {
          if (f.name.startsWith("up.")) doc.removeField(f.name)
        })
        updateField(doc, boolField(Upstream.presenceIndicator, false))
        doc.removeField("uvsn")

        true  // Keep the document
      }
    })
  }

  def clearDownstreamVersion(id:VersionID) = {
    doClearAttributes(id, doc => {
      if (doc.get("uvsn") == null) {
        false // Remove the document
      } else {
        // Remove all the upstream attributes. Convert to list as middle-step to prevent ConcurrentModificationEx - see #177
        doc.getFields.toList.foreach(f => {
          if (f.name.startsWith("down.")) doc.removeField(f.name)
        })
        updateField(doc, boolField(Downstream.presenceIndicator, false))
        doc.removeField("duvsn")
        doc.removeField("ddvsn")

        true  // Keep the document
      }
    })
  }

  def isDirty = bufferSize > 0

  def rollback() = {
    writer.rollback()
    writer = createIndexWriter    // We need to create a new writer, since rollback will have closed the previous one
    log.info("Writer rolled back")
  }

  def flush() {
    if (isDirty) {
      updatedDocs.foreach { case (id, doc) =>
        writer.updateDocument(new Term("id", id.id), doc)
      }
      deletedDocs.foreach { id =>
        writer.deleteDocuments(queryForId(id))
      }
      writer.commit()
      updatedDocs.clear()
      deletedDocs.clear()
      log.trace("Writer flushed")
    }
  }

  def reset() {
    writer.deleteAll()
    writer.commit()
  }

  def close() {
    writer.close()
  }

  private def createIndexWriter() = {
    val version = Version.LUCENE_34
    val config = new IndexWriterConfig(version, new StandardAnalyzer(version))
    val writer = new IndexWriter(index, config)
    writer
  }

  private def bufferSize = updatedDocs.size + deletedDocs.size

  private def prepareUpdate(id: VersionID, doc: Document) = {
    if (deletedDocs.remove(id)) {
      log.warn("Detected update of a document that was deleted in the same writer: " + id)
    }
    updatedDocs.put(id, doc)
    if (bufferSize >= maxBufferSize) {
      flush()
    }
  }

  private def prepareDelete(id: VersionID) = {
    if (updatedDocs.remove(id).isDefined) {
      log.warn("Detected delete of a document that was updated in the same writer: " + id)
    }
    deletedDocs.add(id)
    if (bufferSize >= maxBufferSize) {
      flush()
    }
  }

  private def getCurrentOrNewDoc(id:VersionID) = {
    if (updatedDocs.contains(id)) {
      updatedDocs(id)
    } else {
      val doc =
        if (deletedDocs.contains(id))
          None
        else
          retrieveCurrentDoc(this, id)

      doc match {
        case None => {
          // Nothing in the index yet for this document, or it's pending deletion
          val doc = new Document
          doc.add(new Field("id", id.id, Field.Store.YES, Field.Index.NOT_ANALYZED_NO_NORMS, Field.TermVector.NO))
          doc.add(new Field(Upstream.presenceIndicator, "0", Field.Store.YES, Field.Index.NOT_ANALYZED_NO_NORMS, Field.TermVector.NO))
          doc.add(new Field(Downstream.presenceIndicator, "0", Field.Store.YES, Field.Index.NOT_ANALYZED_NO_NORMS, Field.TermVector.NO))
          doc
        }
        case Some(doc) => doc
      }
    }
  }

  private def doDocUpdate(id:VersionID, lastUpdatedIn:DateTime, currentDocumentVersion:String, prefix:String, f:Document => Unit) = {
    val doc = getCurrentOrNewDoc(id)

    val documentVersionLabel = prefix + "doc.version"
    val previousDocumentVersion = doc.get(documentVersionLabel)

    // If the incoming digest does actually differ from the previous update
    if (previousDocumentVersion == null || previousDocumentVersion != currentDocumentVersion) {

      f(doc)

      // If the participant does not supply a timestamp, then create one on the fly
      val lastUpdated = lastUpdatedIn match {
        case null => new DateTime
        case d    => d
      }

      updateField(doc, dateTimeField("lastUpdated", lastUpdated, indexed = false))
      updateField(doc, stringField(documentVersionLabel, currentDocumentVersion, indexed = false))
    }

    // Update the matched status
    val isMatched = doc.get("uvsn") == doc.get("duvsn")
    updateField(doc, boolField("isMatched", isMatched))

    // Update the timestamp
    updateField(doc, dateTimeField("timestamp", new DateTime().withZone(DateTimeZone.UTC), indexed = false))

    prepareUpdate(id, doc)

    docToCorrelation(doc, id)
  }

  private def doClearAttributes(id:VersionID, f:Document => Boolean) = {
    val currentDoc =
      if (updatedDocs.contains(id))
        Some(updatedDocs(id))
      else if (deletedDocs.contains(id))
        None
      else
        retrieveCurrentDoc(this, id)

    currentDoc match {
      case None => Correlation.asDeleted(id, new DateTime)
      case Some(doc) => {
        if (f(doc)) {
          // We want to keep the document. Update match status and write it out
          updateField(doc, boolField("isMatched", false))

          prepareUpdate(id, doc)

          docToCorrelation(doc, id)
        } else {
          // We'll just delete the doc if it doesn't have an upstream
          prepareDelete(id)

          Correlation.asDeleted(id, new DateTime)
        }
      }
    }
  }

  private def applyAttributes(doc:Document, prefix:String, attributes:Map[String, TypedAttribute]) = {
    attributes.foreach { case (k, v) => {
      val vF = v match {
        case StringAttribute(s)     => stringField(prefix + k, s)
        case DateAttribute(dt)      => dateField(prefix + k, dt)
        case DateTimeAttribute(dt)  => dateTimeField(prefix + k, dt)
        case IntegerAttribute(intV) => intField(prefix + k, intV)
      }
      updateField(doc, vF)
    } }
  }

  private def updateField(doc:Document, field:Fieldable) = {
    doc.removeField(field.name)
    doc.add(field)
  }

  private def stringField(name:String, value:String, indexed:Boolean = true) =
    new Field(name, value, Field.Store.YES, indexConfig(indexed), Field.TermVector.NO)
  private def dateTimeField(name:String, dt:DateTime, indexed:Boolean = true) =
    new Field(name, formatDateTime(dt), Field.Store.YES, indexConfig(indexed), Field.TermVector.NO)
  private def dateField(name:String, dt:LocalDate, indexed:Boolean = true) =
    new Field(name, formatDate(dt), Field.Store.YES, indexConfig(indexed), Field.TermVector.NO)
  private def intField(name:String, value:Int, indexed:Boolean = true) =
    (new NumericField(name, Field.Store.YES, indexed)).setIntValue(value)
  private def boolField(name:String, value:Boolean, indexed:Boolean = true) =
    new Field(name, if (value) "1" else "0", Field.Store.YES, indexConfig(indexed), Field.TermVector.NO)
  private def indexConfig(indexed:Boolean) = if (indexed) Field.Index.NOT_ANALYZED_NO_NORMS else Field.Index.NO

}
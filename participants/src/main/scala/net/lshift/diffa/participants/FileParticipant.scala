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

package net.lshift.diffa.participants

import org.joda.time.DateTime
import org.apache.commons.io.IOUtils
import org.apache.commons.codec.digest.DigestUtils
import net.lshift.diffa.messaging.json.ChangesRestClient
import java.io.{Closeable, FileInputStream, File}
import collection.mutable.{HashMap, ListBuffer}
import org.joda.time.format.ISODateTimeFormat
import net.lshift.diffa.kernel.participants._
import net.lshift.diffa.kernel.frontend.wire.InvocationResult
import net.lshift.diffa.kernel.differencing.{AttributesUtil}
import net.lshift.diffa.participant.scanning.{TimeRangeConstraint, ScanResultEntry, ScanAggregation, ScanConstraint}

/**
 * Basic functionality requried for a file-based participant.
 */
abstract class FileParticipant(val dir:String, val agentRoot:String) extends Closeable {

  private var isClosing = false
  val rootDir = new File(dir)
  val watcher = new DirWatcher(dir, onFileChange)
  val changesClient = new ChangesRestClient(agentRoot)

  val isoFormat = ISODateTimeFormat.dateTime()

  def scan(constraints: Seq[ScanConstraint], aggregations: Seq[CategoryFunction]):Seq[ScanResultEntry] =
      // TODO
    Seq[ScanResultEntry]()

  /*def queryEntityVersions(constraints:Seq[QueryConstraint]) : Seq[EntityVersion] = {
    // Validate the constraints
    assert(constraints.length == 1, "FileParticipant requires a single constraint")
    assert(constraints(0).category == "bizDate", "FileParticipant can only constrain on bizDate")

    val files = queryFiles(constraints(0))
    files.map(f => EntityVersion(idFor(f), AttributesUtil.toSeq(attributesFor(f)), dateFor(f), versionFor(f)))
  }

  def queryAggregateDigests(bucketing:Map[String, CategoryFunction], constraints:Seq[QueryConstraint]) : Seq[AggregateDigest] = {
    // Validate the constraints
    assert(constraints.length == 1, "FileParticipant requires a single constraint")
    assert(constraints(0).category == "bizDate", "FileParticipant can only constrain on bizDate")
    
    val files = queryFiles(constraints(0))
    val builder = new DigestBuilder(bucketing)
    files.sortBy(_.getAbsolutePath).foreach(f => {
      builder.add(idFor(f), attributesFor(f), dateFor(f), versionFor(f))
    })
    builder.digests
  }*/

  def queryFiles(constraint:ScanConstraint) = {
    val rangeConstraint = constraint.asInstanceOf[TimeRangeConstraint]
    val lower = rangeConstraint.getStart
    val upper = rangeConstraint.getEnd

    val files = new ListBuffer[File]

    DirWalker.walk(dir,
      f => {
        val lastModDate = new DateTime(f.lastModified)
        ((lower == null || lower.isBefore(lastModDate)) && (upper == null || upper.isAfter(lastModDate)))
      },
      f => {
        files += f
      })
      files
  }

  def retrieveContent(identifier: String) = {
    val path = new File(rootDir, identifier)
    IOUtils.toString(new FileInputStream(path))
  }

  def invoke(actionId:String, entityId:String) : InvocationResult = {
    actionId match {
      case "resend" => InvocationResult("error", "Unknown Entity:" + entityId)
      case _        => InvocationResult("error", "Unknown action:" + actionId)
    }
  }

  override def close() = {
    if (!isClosing) {
      isClosing = true
      changesClient.close
      watcher.close
    }    
  }

  protected def onFileChange(f:File)

  protected def idFor(f:File) = f.getAbsolutePath.substring(rootDir.getAbsolutePath.length)
  protected def attributesFor(f:File) = Map("bizDate" -> f.lastModified.toString())
  protected def dateFor(f:File) = new DateTime(f.lastModified)
  protected def versionFor(f:File) = DigestUtils.md5Hex(new FileInputStream(f))
}
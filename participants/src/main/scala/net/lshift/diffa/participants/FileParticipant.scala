/**
 * Copyright (C) 2010 LShift Ltd.
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
import net.lshift.diffa.kernel.differencing.DigestBuilder
import net.lshift.diffa.kernel.participants._

/**
 * Basic functionality requried for a file-based participant.
 */
abstract class FileParticipant(val dir:String, val agentRoot:String) extends Closeable {

  private var isClosing = false
  val rootDir = new File(dir)
  val watcher = new DirWatcher(dir, onFileChange)
  val changesClient = new ChangesRestClient(agentRoot)

  val isoFormat = ISODateTimeFormat.dateTime()

  def queryEntityVersions(constraints:Seq[QueryConstraint]) : Seq[EntityVersion] = {
    val files = queryFiles(constraints(0))
    files.map(f => EntityVersion(idFor(f), attributesFor(f), dateFor(f), versionFor(f)))
  }

  def queryAggregateDigests(constraints:Seq[QueryConstraint]) : Seq[AggregateDigest] = {
    val files = queryFiles(constraints(0))
    val builder = new DigestBuilder(constraints(0).function)
    files.sortBy(_.getAbsolutePath).foreach(f => {
      builder.add(idFor(f), attributesFor(f), dateFor(f), versionFor(f))
    })
    builder.digests
  }

  def queryFiles(constraint:QueryConstraint) = {
    val lower = isoFormat.parseDateTime(constraint.values(0))
    val upper = isoFormat.parseDateTime(constraint.values(1))    
    val dateConstraint = SimpleDateConstraint(lower,upper)

    val files = new ListBuffer[File]

    DirWalker.walk(dir,
      f => {
        val lastModDate = new DateTime(f.lastModified)
        dateConstraint.contains(lastModDate)
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

  def invoke(actionId:String, entityId:String) : ActionResult = {
    actionId match {
      case "resend" => ActionResult("error", "Unknown Entity:" + entityId)
      case _        => ActionResult("error", "Unknown action:" + actionId)
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
  protected def attributesFor(f:File) = Seq(f.lastModified.toString())
  protected def dateFor(f:File) = new DateTime(f.lastModified)
  protected def versionFor(f:File) = DigestUtils.md5Hex(new FileInputStream(f))
}
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

import collection.mutable.HashMap
import java.util.{TimerTask, Timer}
import java.io.{Closeable, FileFilter, File}

/**
 * Directory watcher for implementing a filesystem backed participant.
 */
class DirWatcher(val dir:String, val listener:(File) => Unit, val delay:Int = 100) extends Closeable {
  val seenFiles = new HashMap[String, Long]
  scan(true)

  private val scanThread = new Thread {
    var shouldRun = true
    override def run {
      while (shouldRun) {
        Thread.sleep(delay)
        scan(false)
      }
    }
  }
  scanThread.start

  override def close() = scanThread.shouldRun = false

  def scan(initial:Boolean) {
    DirWalker.walk(dir,
      f => {
        val absPath = f.getAbsolutePath
        if (seenFiles.contains(absPath)) {
          f.lastModified > seenFiles(absPath)
        } else {
          true
        }
      },
      f => {
        seenFiles(f.getAbsolutePath) = f.lastModified
        if (!initial) {
          listener(f)
        }
      })
  }
}
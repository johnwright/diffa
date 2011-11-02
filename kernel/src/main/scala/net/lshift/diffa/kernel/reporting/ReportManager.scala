/**
 * Copyright (C) 2011 LShift Ltd.
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
package net.lshift.diffa.kernel.reporting

import net.lshift.diffa.kernel.config.{PairReportType, DiffaPairRef, DomainConfigStore}

/**
 * Component responsible for executing reports defined on a pair.
 */
class ReportManager(configStore:DomainConfigStore) {
  import PairReportType._

  /**
   * Executes a report with the given name for the given pair.
   * @throws MissingObjectException if the requested report doesn't exist.
   */
  def executeReport(pair:DiffaPairRef, name:String) {
    val reportDef = configStore.getPairReportDef(pair.domain, name, pair.key)

    reportDef.reportType match {
      case DIFFERENCES => runDifferencesReport(pair, reportDef.target)
    }
  }

  private def runDifferencesReport(pair:DiffaPairRef, target:String) {

  }
}
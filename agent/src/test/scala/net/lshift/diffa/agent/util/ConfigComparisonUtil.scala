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
package net.lshift.diffa.agent.util

import org.custommonkey.xmlunit._
import org.custommonkey.xmlunit.XMLAssert.assertXMLEqual
import org.w3c.dom.Element

/**
 * Util for verifying that configuration matches expected XML.
 */
object ConfigComparisonUtil {
  def assertConfigMatches(expected:String, actual:String) = {
    XMLUnit.setNormalize(true)
    XMLUnit.setNormalizeWhitespace(true)
    XMLUnit.setIgnoreWhitespace(true)

    val diff = new DetailedDiff(new Diff(expected, actual))
    diff.overrideElementQualifier(new ElementQualifier() {
      val nameAndAttrQualifier = new ElementNameAndAttributeQualifier
      val nameAndTextQualifier = new ElementNameAndTextQualifier

      def qualifyForComparison(control: Element, test: Element) = {
        // Custom behaviour for value nodes, since they don't have alignment attributes
        if (test.getNodeName == "value") {
          nameAndTextQualifier.qualifyForComparison(control, test)
        } else {
          nameAndAttrQualifier.qualifyForComparison(control, test)
        }
      }
    })

    assertXMLEqual("Serialized xml " + actual + " does not match expected " + expected, diff, true)
  }
}
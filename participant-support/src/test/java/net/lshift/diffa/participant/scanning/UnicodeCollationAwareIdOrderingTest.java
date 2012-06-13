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
package net.lshift.diffa.participant.scanning;

import org.junit.Test;

public class UnicodeCollationAwareIdOrderingTest {
  private IdOrdering idOrdering = new UnicodeCollationAwareIdOrdering();
  @Test
  public void testLt() {
    assert(idOrdering.sortsBefore("a", "b"));
  }
  @Test public void testMixCaseLt() {
    assert(idOrdering.sortsBefore("a", "B"));
  }

  @Test public void testGt() {
    assert(!idOrdering.sortsBefore("c", "b"));
  }
  @Test public void testMixCaseGt() {
    assert(!idOrdering.sortsBefore("C", "b"));
  }

}

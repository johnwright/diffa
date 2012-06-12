package net.lshift.diffa.kernel.config

import org.junit.Test
import org.junit.runner.RunWith
import org.junit.experimental.theories.{DataPoint, Theories, Theory}
import org.hamcrest.Matchers._
import org.hamcrest.MatcherAssert.assertThat


// TODO: Remove these; as the functionality is tested in the Java superclass.
class UnicodeCollationOrderingTest {
  @Test def testLt() = assert(UnicodeCollationOrdering.sortsBefore("a", "b"))
  @Test def testMixCaseLt() = assert(UnicodeCollationOrdering.sortsBefore("a", "B"))

  @Test def testGt() = assert(!UnicodeCollationOrdering.sortsBefore("c", "b"))
  @Test def testMixCaseGt() = assert(!UnicodeCollationOrdering.sortsBefore("C", "b"))

}


class AsciiCollationOrderingTest {
  @Test def testLt() = assert(AsciiCollationOrdering.sortsBefore("a", "b"))
  @Test def testMixCaseLt() = assert(!AsciiCollationOrdering.sortsBefore("a", "B"))

  @Test def testGt() = assert(!AsciiCollationOrdering.sortsBefore("c", "b"))
  @Test def testMixCaseGt() = assert(AsciiCollationOrdering.sortsBefore("C", "b"))

}

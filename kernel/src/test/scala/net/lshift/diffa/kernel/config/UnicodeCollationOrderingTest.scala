package net.lshift.diffa.kernel.config

import org.junit.Test
import org.junit.runner.RunWith
import org.junit.experimental.theories.{DataPoint, Theories, Theory}
import org.hamcrest.Matchers._
import org.hamcrest.MatcherAssert.assertThat

class UnicodeCollationOrderingTest {
  @Test def testLt() = assert(UnicodeCollationOrdering.compare("a", "b") < 0)
  @Test def testMixCaseLt() = assert(UnicodeCollationOrdering.compare("a", "B") < 0)

  @Test def testGt() = assert(UnicodeCollationOrdering.compare("c", "b") > 0)
  @Test def testMixCaseGt() = assert(UnicodeCollationOrdering.compare("C", "b") > 0)

}


class AsciiCollationOrderingTest {
  @Test def testLt() = assert(AsciiCollationOrdering.compare("a", "b") < 0)
  @Test def testMixCaseLt() = assert(AsciiCollationOrdering.compare("a", "B") > 0)

  @Test def testGt() = assert(AsciiCollationOrdering.compare("c", "b") > 0)
  @Test def testMixCaseGt() = assert(AsciiCollationOrdering.compare("C", "b") < 0)

}

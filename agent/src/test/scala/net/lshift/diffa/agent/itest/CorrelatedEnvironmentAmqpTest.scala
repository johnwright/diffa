package net.lshift.diffa.agent.itest

import org.junit.Assume.assumeTrue
import support.{AmqpConnectionChecker, TestEnvironments}

/**
 * Test cases where various differences between a pair of participants are caused, and the agent is invoked
 * to detect and report on them. The participants in this test require correlation between their versioning schemes,
 * and will produce different versions for a given piece of content.
 */
class CorrelatedEnvironmentAmqpTest extends AbstractEnvironmentTest
  with CommonDifferenceTests
  with CommonActionTests {

  assumeTrue(AmqpConnectionChecker.isConnectionAvailable)

  val env = TestEnvironments.abCorrelatedAmqp
}
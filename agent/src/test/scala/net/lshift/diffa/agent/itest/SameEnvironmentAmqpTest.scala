package net.lshift.diffa.agent.itest

import org.junit.Assume.assumeTrue
import support.{AmqpConnectionChecker, TestEnvironments}

/**
 * Test cases where various differences between a pair of participants are caused, and the agent is invoked
 * to detect and report on them. The participants in this test use the same versioning scheme, and thus will produce
 * the same versions for a given content item.
 */
class SameEnvironmentAmqpTest extends AbstractEnvironmentTest
  with CommonDifferenceTests {

  assumeTrue(AmqpConnectionChecker.isConnectionAvailable)

  val env = TestEnvironments.abSameAmqp
}
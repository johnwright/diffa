package net.lshift.diffa.kernel.differencing

/**
 * Concrete implementation of the Data Driven Policy test for same versions.
 */
class SameVersionPolicyDataDrivenTest extends AbstractDataDrivenPolicyTest {
  val policy = new SameVersionPolicy(store, listener, configStore)
}
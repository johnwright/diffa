package net.lshift.diffa.agent.rest



import scala.util.matching.Regex
import scala.collection.JavaConversions._
import org.junit.experimental.theories.{Theories, DataPoint,Theory}
import org.junit.runner.RunWith
import org.junit.Assert._
import org.easymock.EasyMock._
import net.lshift.diffa.kernel.config.system.SystemConfigStore
import net.lshift.diffa.agent.rest.UserResourceScanTest.Scenario
import net.lshift.diffa.kernel.config.User
import net.lshift.diffa.participant.scanning.{ScanConstraint, ScanResultEntry}
import org.junit.{Before, Test}
import net.lshift.diffa.kernel.participants.{StringPrefixCategoryFunction, CategoryFunction}


@RunWith(classOf[Theories])
class UserResourceScanTest {
  val systemConfigStore = createStrictMock("systemConfigStore", classOf[SystemConfigStore])

  var userResourceScan = new UserResourceScan(systemConfigStore)


  def setup(scenario: Scenario) {
    expect(systemConfigStore.listUsers).andReturn(scenario.userList).once()
    replay(systemConfigStore)
  }

  @Theory
  def shouldFetchUsersListFromConfigStore(scenario:Scenario) {
    setup(scenario)

    userResourceScan.perform(scenario.constraints, scenario.aggregation)

    verify(systemConfigStore)
    //throw new RuntimeException("Kaboom!")
  }

  @Theory
  def shouldAggregateUsers(scenario:Scenario) {
    setup(scenario)
    assertEquals(scenario.results,
      userResourceScan.perform(scenario.constraints, scenario.aggregation))
  }

}

object UserResourceScanTest {
  case class Scenario(userList:Seq[User],
                      constraints:Seq[ScanConstraint],
                      aggregation:Seq[CategoryFunction],
                      results:Seq[ScanResultEntry])


  @DataPoint def nullExample = Scenario(List(), List(), List(), List())
  def a_user = User("abc")
  def b_user = User("bcd")

  @DataPoint def singleUserNoAggregation = Scenario(
    List(a_user),
    List(),// Constraints
    List(), // Aggregation.
    List(ScanResultEntry.forEntity(a_user.name, ScannableUtils.generateDigest(a_user.name, a_user.token), null, Map("name" -> "abc")))
  )

  @DataPoint def singleUserWithPrefixAggregation = {
    var agg = ScanResultEntry.forAggregate("ec0405c5aef93e771cd80e0db180b88b", Map("name" -> "a"))
    Scenario(
      List(a_user),
      List(),// Constraints
      List(StringPrefixCategoryFunction("name", 1, 3, 1)),
      List(agg))
  }
}

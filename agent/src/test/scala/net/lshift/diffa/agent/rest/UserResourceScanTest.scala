package net.lshift.diffa.agent.rest



import scala.util.matching.Regex
import scala.collection.JavaConversions._
import org.junit.experimental.theories.{Theories, DataPoint,Theory}
import org.junit.runner.RunWith
import org.junit.Assert._
import org.easymock.EasyMock.{expect}
import org.easymock.classextension.EasyMock.{replay,createStrictMock,verify}
import net.lshift.diffa.kernel.config.system.SystemConfigStore
import net.lshift.diffa.agent.rest.UserResourceScanTest.Scenario
import net.lshift.diffa.kernel.config.User
import org.junit.{Before, Test}
import net.lshift.diffa.kernel.participants.{StringPrefixCategoryFunction, CategoryFunction}
import net.lshift.diffa.participant.scanning.{StringPrefixConstraint, ScanConstraint, ScanResultEntry}
import net.lshift.diffa.kernel.frontend.{UserDef, SystemConfiguration}
import java.security.MessageDigest
import net.lshift.diffa.agent.rest.ScannableUtils
import scala.Predef._
import org.apache.commons.codec.binary.Hex
import java.nio.charset.Charset


@RunWith(classOf[Theories])
class UserResourceScanTest {
  val systemConfig = createStrictMock("systemConfiguration", classOf[SystemConfiguration])

  var userResourceScan = new UserResourceScan(systemConfig)


  def setup(scenario: Scenario) {
    expect(systemConfig.listFullUsers).andReturn(scenario.userList).once()
    replay(systemConfig)
  }

  @Theory
  def shouldFetchUsersListFromConfigStore(scenario:Scenario) {
    setup(scenario)

    userResourceScan.perform(scenario.constraints, scenario.aggregation)

    verify(systemConfig)
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
  val user_abc = User("abc")
  val user_abd = User("abd")
  val user_bcd = User("bcd")

  @DataPoint def singleUserNoAggregation = Scenario(
    List(user_abc),
    List(),// Constraints
    List(), // Aggregation.
    List(ScanResultEntry.forEntity(user_abc.name,
      ScannableUtils.generateDigest(user_abc.name, user_abc.token),
      null, Map("name" -> user_abc.name)))
  )

  @DataPoint def twoUsersWithPrefixFilter = Scenario(
    List(user_abc, user_bcd),
    List(new StringPrefixConstraint("name", "b")),
    List(), // Aggregation.
    List(ScanResultEntry.forEntity(user_bcd.name,
      ScannableUtils.generateDigest(user_bcd.name, user_bcd.token),
      null, Map("name" -> user_bcd.name)))
  )

  def aggregated_versions(vsns: Seq[String]) = {

    val md = MessageDigest.getInstance("MD5")
    vsns.foreach { s => md.update(s.getBytes(Charset.forName("UTF-8"))) }
    new String (Hex.encodeHex(md.digest()))
  }

  @DataPoint def twoUsersWithPrefixFilterAndAggregation = {
    var matching_users = List(user_abc, user_abd)
    val versions: List[String] = matching_users.map { user =>
      ScannableUtils.generateDigest(user.name, user.token)
    }
    var agg_version = aggregated_versions(versions)
    Scenario(
      List(user_abc, user_abd, user_bcd),
      List(new StringPrefixConstraint("name", "a")),
      List(StringPrefixCategoryFunction("name", 1, 3, 1)),
      List(ScanResultEntry.forAggregate(
      agg_version,
      Map("name" -> "a")))
  )
  }


  @DataPoint def singleUserWithPrefixAggregation = {
    val agg = ScanResultEntry.forAggregate("ec0405c5aef93e771cd80e0db180b88b", Map("name" -> "a"))
    Scenario(
      List(user_abc),
      List(),// Constraints
      List(StringPrefixCategoryFunction("name", 1, 3, 1)),
      List(agg))
  }

   // Result ordering?

}

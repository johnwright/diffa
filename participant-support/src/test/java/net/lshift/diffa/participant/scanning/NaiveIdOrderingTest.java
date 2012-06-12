package net.lshift.diffa.participant.scanning;

import org.junit.Test;

/**
 * Created with IntelliJ IDEA.
 * User: ceri
 * Date: 12/06/12
 * Time: 19:38
 * To change this template use File | Settings | File Templates.
 */
public class NaiveIdOrderingTest {
  
  private IdOrdering idOrdering = new NaiveIdOrdering();
  @Test 
  public void testLt() {
    assert(idOrdering.sortsBefore("a", "b"));
  }
  @Test public void testMixCaseLt() {
    assert(!idOrdering.sortsBefore("a", "B"));
  }

  @Test public void testGt() {
    assert(!idOrdering.sortsBefore("c", "b"));
  }
  @Test public void testMixCaseGt() {
    assert(idOrdering.sortsBefore("C", "b"));
  }

}

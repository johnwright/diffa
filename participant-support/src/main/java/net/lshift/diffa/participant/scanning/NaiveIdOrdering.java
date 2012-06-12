package net.lshift.diffa.participant.scanning;

/**
 * Created with IntelliJ IDEA.
 * User: ceri
 * Date: 12/06/12
 * Time: 19:09
 * To change this template use File | Settings | File Templates.
 */
public class NaiveIdOrdering implements IdOrdering {
  @Override
  public boolean sortsBefore(String left, String right) {
    return left.compareTo(right) < 0;
  }
}

package net.lshift.diffa.participant.scanning;

/**
 * Created with IntelliJ IDEA.
 * User: ceri
 * Date: 12/06/12
 * Time: 19:02
 * To change this template use File | Settings | File Templates.
 */
public interface IdOrdering {
  public boolean sortsBefore(String left, String right);
}

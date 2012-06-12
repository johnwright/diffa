package net.lshift.diffa.participant.scanning;

import com.ibm.icu.text.Collator;
import java.util.Locale;

/**
 * Created with IntelliJ IDEA.
 * User: ceri
 * Date: 12/06/12
 * Time: 19:07
 * To change this template use File | Settings | File Templates.
 */
public class UnicodeCollationAwareIdOrdering implements IdOrdering {
  private Collator coll = Collator.getInstance(Locale.ROOT);

  @Override
  public boolean sortsBefore(String left, String right) {
    return coll.compare(left, right) < 0;
  }
}

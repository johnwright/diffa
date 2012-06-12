package net.lshift.diffa.kernel.config

import net.lshift.diffa.participant.scanning.{NaiveIdOrdering, UnicodeCollationAwareIdOrdering, IdOrdering}


/**
 * Created with IntelliJ IDEA.
 * User: ceri
 * Date: 12/06/11
 * Time: 14:53
 * To change this template use File | Settings | File Templates.
 */

object UnicodeCollationOrdering extends UnicodeCollationAwareIdOrdering {
}


object AsciiCollationOrdering extends NaiveIdOrdering {
}
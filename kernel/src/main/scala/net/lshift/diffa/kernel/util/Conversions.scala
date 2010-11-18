package net.lshift.diffa.kernel.util

import scala.collection.Map

object Conversions {

  implicit def toTreeMap[K,V](map:Map[K,V]) = {
    val treeMap = new java.util.TreeMap[K,V]
    map.foreach{case (k,v) => treeMap.put(k,v)}
    treeMap
  }
}
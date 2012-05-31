package net.lshift.diffa.kernel.util

import net.lshift.diffa.kernel.config.ServiceLimit
import net.lshift.diffa.kernel.limiting.Limiter

trait Registry[KeyType, ObjectType] {
  def register(key: KeyType, factory: () => ObjectType) {
    get(key, factory)
  }

  def get(key: KeyType, factory: () => ObjectType): ObjectType
}

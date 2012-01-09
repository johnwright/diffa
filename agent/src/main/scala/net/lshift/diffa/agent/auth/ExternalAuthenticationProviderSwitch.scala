/**
 * Copyright (C) 2012 LShift Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.lshift.diffa.agent.auth

import org.springframework.security.core.Authentication
import org.springframework.security.authentication.{AuthenticationProvider}
import java.lang.Class
import net.lshift.diffa.kernel.config.system.SystemConfigStore
import org.springframework.security.ldap.authentication.ad.ActiveDirectoryLdapAuthenticationProvider
import org.slf4j.{LoggerFactory, Logger}

/**
 * Runtime controllable authentication provider implementation, that will proxy to an internally configured LDAP
 * implementation if configuration is available.
 */
class ExternalAuthenticationProviderSwitch(val configStore:SystemConfigStore) extends AuthenticationProvider {

  val log:Logger = LoggerFactory.getLogger(getClass)

  // Look for configuration for various authentication mechanisms
  val activeDirectoryDomain = configStore.maybeSystemConfigOption("activedirectory.domain")

  // Generate a backing provider based upon the settings that are available
  val backingProvider:Option[AuthenticationProvider] = if (activeDirectoryDomain.isDefined) {
    val domain = activeDirectoryDomain.get
    val activeDirectoryServer = configStore.systemConfigOptionOrDefault("activedirectory.server", "ldap://" + domain + "/")

    log.info("Using ActiveDirectory authentication for domain %s with server %s".format(domain, activeDirectoryServer))

    Some(new ActiveDirectoryLdapAuthenticationProvider(domain, activeDirectoryServer))
  } else {
    log.info("No external authentication providers configured")

    None
  }

  def authenticate(authentication: Authentication) = backingProvider match {
    case Some(ap) => ap.authenticate(authentication)
    case None     => null
  }

  def supports(authentication: Class[_]):Boolean = backingProvider match {
    case Some(ap) => ap.supports(authentication)
    case None     => false
  }
}
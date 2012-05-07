package net.lshift.diffa.kernel.frontend

import reflect.BeanProperty
import net.lshift.diffa.kernel.config.ValidationUtil

/**
 * Serializable representation of http credentials within the context of a domain, without any sensitive information.
 * This is used when listing credentials back to clients, so that the underlying secret credential is not leaked.
 * Generally used for outbound communication.
 */
case class OutboundExternalHttpCredentialsDef(@BeanProperty var url: String = null,
                                              @BeanProperty var key: String = null,
                                              @BeanProperty var `type`: String = null) {
  def this() = this(url = null)
}

/**
 * Serializable representation of http credentials within the context of a domain.
 * Generally used for inbound communication.
 */
case class InboundExternalHttpCredentialsDef(@BeanProperty var url: String = null,
                                             @BeanProperty var key: String = null,
                                             @BeanProperty var value: String = null,
                                             @BeanProperty var `type`: String = null) {

  def this() = this(url = null)

  def validate(path:String = null) {

    val credsPath = ValidationUtil.buildPath(path, "external-http-credentials", Map("url" -> url))

    ValidationUtil.ensureLengthLimit(credsPath, "url", url, DefaultLimits.URL_LENGTH_LIMIT)
  }
}

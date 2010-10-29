package net.lshift.diffa.kernel.lifecycle

/**
 * Trait implemented by any component that wishes to be informed about Agent-specific lifecycle events.
 */
trait AgentLifecycleAware {
  /**
   * Indicates that assembly of the agent has completed. This means that:
   * <ul>
   *   <li>All components have been instantiated and their dependencies wired</li>
   *   <li>Startup methods (as wired in Spring) have been called on all components</li>
   * </ul>
   *
   * This lifecycle event should be hooked when components need to activate some form of persistent configuration
   * within the agent where a plugin needs to have been booted for it to succeed.
   */
  def onAgentAssemblyCompleted {}
}
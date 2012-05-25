package net.lshift.diffa.schema;

import net.lshift.diffa.kernel.config.HibernateConfigStorePreparationStep;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;

/**
 * Temporary helper class for proof-of-concept JOOQ integration.
 *
 * Delete this class and move all schema-generation code out to a separate module.
 */
public class Migrate {

  public static void main(String[] args) {
    Configuration config = new Configuration()
      // TODO add hbm.xml resources?
      .setProperty("hibernate.dialect", System.getProperty("diffa.hibernate.dialect"))
      .setProperty("hibernate.connection.url", System.getProperty("diffa.jdbc.url"))
      .setProperty("hibernate.connection.driver_class", System.getProperty("diffa.jdbc.driver"))
      .setProperty("hibernate.connection.username", System.getProperty("diffa.jdbc.username"))
      .setProperty("hibernate.connection.password", System.getProperty("diffa.jdbc.password"));
    SessionFactory sessionFactory = config.buildSessionFactory();
    try {
      (new HibernateConfigStorePreparationStep()).prepare(sessionFactory, config);
    } finally {
      sessionFactory.close();
    }
  }
}

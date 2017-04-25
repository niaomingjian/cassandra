package com.niaomingjian.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

public final class CassandraConnector {

  private static final Logger LOGGER = LoggerFactory.getLogger(CassandraConnector.class);

  /** cassandra_config.yaml */
  private static CassandraConfig config;

  /** cluster */
  private static Cluster cluster;

  /** session */
  private static Session session;

  private CassandraConnector() {

  }

  public static Cluster getCluster() {
    return cluster;
  }

  public static Session getSession() {
    return session;
  }

  public static void init() {
    Constructor constructor = new Constructor(CassandraConfig.class);
    Yaml yaml = new Yaml(constructor);
    config = yaml.loadAs(CassandraConnector.class.getResourceAsStream("/cassandra_config.yaml"), CassandraConfig.class);

    Cluster.Builder builder = Cluster.builder()
        .withQueryOptions(new QueryOptions().setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM))
        .addContactPoints(config.getContactPointsData()).withPort(config.getPortData());
    cluster = builder.build();
    session = cluster.connect(config.getKeyspaceNameData());
  }

  public static void close() {
    if (!session.isClosed()) {
      session.close();
    }
    if (!cluster.isClosed()) {
      cluster.close();
    }
  }

}

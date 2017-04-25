package com.niaomingjian.cassandra;

import lombok.Data;

@Data
public class CassandraConfig {
  private String keyspaceNameData;

  private String[] contactPointsData;

  private int portData;
}

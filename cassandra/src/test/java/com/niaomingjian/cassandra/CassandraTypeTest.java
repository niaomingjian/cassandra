package com.niaomingjian.cassandra;

import com.datastax.driver.core.LocalDate;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.time.temporal.ChronoUnit;
import java.util.Date;

/**
 * <pre>
 * </pre>
 */
public class CassandraTypeTest {

  @Before
  public void setUp() {
    CassandraConnector.init();
    SimpleStatement statement = new SimpleStatement("DROP TABLE IF EXISTS typetest");
    CassandraConnector.getSession().execute(statement);

    statement = new SimpleStatement("" 
                                  + "CREATE TABLE typetest (\n" 
                                  + "col1_ascii ascii,\n" 
                                  + "col2_bigint bigint,\n"
                                  + "col3_blob blob,\n"
                                  + "col4_boolean boolean,\n" 
                                  + "col6_date date,\n" 
                                  + "col7_decimal decimal,\n" 
                                  + "col8_double double,\n" 
                                  + "col9_float float,\n" 
                                  + "col10_inet inet,\n"
                                  + "col11_int int,\n" 
                                  + "col15_smallint smallint,\n" 
                                  + "col16_text text,\n" 
                                  + "col17_time time,\n" 
                                  + "col18_timestamp timestamp,\n"
                                  + "col20_tinyint tinyint,\n" 
                                  + "col24_varchar varchar,\n" 
                                  + "PRIMARY KEY (col24_varchar, col11_int)\n" + ")");
    CassandraConnector.getSession().execute(statement);

    insert("'aaa'", "'1000'", String.valueOf(1));

    CassandraConnector.close();
  }

  private void insert(String col1_ascii, String col24_varchar, String col11_int) {
    String insertBase = "insert into "
        + "typetest(col1_ascii,col2_bigint,col3_blob,col4_boolean,col6_date,col7_decimal,col8_double,col9_float,col10_inet,col15_smallint,col16_text,col17_time,col18_timestamp,col20_tinyint,col24_varchar,col11_int)"
        + "values(@col1_ascii, 10000, 0x414141, false,'2016-01-01', 20.1, 200.1, 2000.1, '10.0.0.1', 1, 'test_new','01:01:01.013013013','2016-01-01T01:01:01.013+0700', 1, @col24_varchar, @col11_int)";

    SimpleStatement statement = new SimpleStatement(
        insertBase.replaceFirst("@col1_ascii", col1_ascii).replaceFirst("@col24_varchar", col24_varchar).replaceFirst("@col11_int", col11_int));
    CassandraConnector.getSession().execute(statement);
  }

  @Test
  public void test() {

    CassandraConnector.init();

    Statement statement = new SimpleStatement("select * from typetest");
    Session session = CassandraConnector.getSession();
    ResultSet rs = session.execute(statement);
    Row row = rs.one();

    // Date DB=>com.datastax.driver.core.LocalDate
    LocalDate date = row.getDate("col6_date");
    System.out.println("The Returned Value:" + date.toString());

    System.out.println("The Returned Value:" + date.getDaysSinceEpoch());
    java.time.LocalDate localDate1 = java.time.LocalDate.parse("1970-01-01");
    java.time.LocalDate localDate2 = java.time.LocalDate.parse("2016-01-01");
    System.out.println("The Calculated Value:" + ChronoUnit.DAYS.between(localDate1, localDate2));

    System.out.println("The Returned Value:" + date.getMillisSinceEpoch());
    System.out.println("The Calculated Value:" + date.getDaysSinceEpoch() * 24 * 3600 * 1000L);

    // Time DB=>long
    long time = row.getTime("col17_time");
    System.out.println(time);

    // Timestamp DB=>java.util.Date
    Date timestamp = row.getTimestamp("col18_timestamp");
    System.out.println(timestamp.toString());

    // Blob DB=>ByteBuffer
    ByteBuffer blob = row.getBytes("col3_blob");
    System.out.println(new String(blob.array()));

    CassandraConnector.close();
  }
}

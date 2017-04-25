package com.niaomingjian.cassandra;

import com.datastax.driver.core.LocalDate;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import org.junit.Before;
import org.junit.Test;

import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.GregorianCalendar;

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
                                  + "col1 ascii,\n" 
                                  + "col2 bigint,\n"
                                  + "col4 boolean,\n" 
                                  + "col6 date,\n" 
                                  + "col7 decimal,\n" 
                                  + "col8 double,\n" 
                                  + "col9 float,\n" 
                                  + "col10 inet,\n"
                                  + "col11 int,\n" 
                                  + "col15 smallint,\n" 
                                  + "col16 text,\n" 
                                  + "col17 time,\n" 
                                  + "col18 timestamp,\n"
                                  + "col20 tinyint,\n" 
                                  + "col24 varchar,\n" 
                                  + "PRIMARY KEY (col24, col11)\n" + ")");
    CassandraConnector.getSession().execute(statement);

    insert("'aaa'", "'1000'", String.valueOf(1));

    CassandraConnector.close();
  }

  private void insert(String col1, String col24, String col11) {
    String insertBase = "insert into "
        + "typetest(col1,col2,col4,col6,col7,col8,col9,col10,col15,col16,col17,col18,col20,col24,col11)"
        + "values(@col1, 10000, false,'2016-01-01', 20.1, 200.1, 2000.1, '10.0.0.1', 1, 'test_new','01:01:01.013013013','2016-01-01T01:01:01.013+0700', 1, @col24, @col11)";

    SimpleStatement statement = new SimpleStatement(
        insertBase.replaceFirst("@col1", col1).replaceFirst("@col24", col24).replaceFirst("@col11", col11));
    CassandraConnector.getSession().execute(statement);
  }

  @Test
  public void test() {

    CassandraConnector.init();

    Statement statement = new SimpleStatement("select * from typetest");
    Session session = CassandraConnector.getSession();
    ResultSet rs = session.execute(statement);
    Row row = rs.one();

    // Date   DB=>com.datastax.driver.core.LocalDate
    LocalDate date = row.getDate("col6");
    System.out.println("The Returned Value:" + date.toString());

    System.out.println("The Returned Value:" + date.getDaysSinceEpoch());
    java.time.LocalDate localDate1 = java.time.LocalDate.parse("1970-01-01");
    java.time.LocalDate localDate2 = java.time.LocalDate.parse("2016-01-01");
    System.out.println("The Calculated Value:" + ChronoUnit.DAYS.between(localDate1, localDate2));

    System.out.println("The Returned Value:" + date.getMillisSinceEpoch());
    System.out.println("The Calculated Value:" + date.getDaysSinceEpoch() * 24 * 3600 * 1000L);

    // Time  DB=>long
    long time = row.getTime("col17");
    System.out.println(time);

    // Timestamp DB=>java.util.Date
    Date timestamp = row.getTimestamp("col18");
    System.out.println(timestamp.toString());

    CassandraConnector.close();
  }
}
